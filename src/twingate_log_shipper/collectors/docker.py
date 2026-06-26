"""Docker JSON log file collector for Twingate connector analytics.

Discovers *all* connector containers matching a name/image filter and tails each
one's Docker ``json-file`` log concurrently, fanning parsed analytics events into a
single internal queue. Containers that start or stop after launch are picked up and
released dynamically (e.g. a Fleet Commander connector fleet scaling up and down).

Two Docker-specific framing concerns are handled:

* **Line chunking.** Docker's ``json-file`` driver splits any single stdout line
  longer than ~16 KB into multiple log records; only the last record's ``log`` value
  ends with a newline. Fragments are reassembled per stream before parsing.
* **Stream separation.** ``ANALYTICS`` records are emitted on ``stdout``; resource
  metrics and diagnostics go to ``stderr``. Only the ``stdout`` stream is parsed.

Trust boundary: in host-level mode the shipper tails every container whose name or
image matches the filter, reading files under ``docker_log_path``. Container
directories are validated as 64-char hex IDs and symlinks are rejected; the log file
is opened with ``O_NOFOLLOW`` where supported. The containers directory is expected to
be a trusted, read-only mount.
"""

import asyncio
import contextlib
import json
import os
import re
import time
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from pathlib import Path
from typing import IO, Any

import structlog

from twingate_log_shipper.collectors.base import BaseCollector

log = structlog.get_logger(__name__)

ANALYTICS_PREFIX = "ANALYTICS "
POLL_INTERVAL = 0.5  # seconds between EOF polls
DEFAULT_DISCOVERY_INTERVAL = 5.0  # seconds between container rescans
DEFAULT_MAX_LINE_BYTES = 1_048_576  # reassembly buffer cap per container (runaway guard)

# Docker container IDs are 64-char lowercase hex; the log dir is named with the full ID.
_CONTAINER_ID_RE = re.compile(r"[0-9a-f]{64}\Z")

# Sentinel placed on the internal queue by close() to unblock a pending events() get.
_SENTINEL = object()


def _parse_created(value: Any) -> float | None:
    """Parse a Docker ``Created`` ISO-8601 timestamp into epoch seconds (UTC).

    Tolerates the trailing ``Z`` and Docker's nanosecond fractional seconds (which
    :func:`datetime.fromisoformat` cannot parse) by truncating to microseconds.
    Returns None if the value is missing or unparseable. Example input:
    ``"2026-06-26T17:29:00.123456789Z"``.
    """
    if not isinstance(value, str) or not value or len(value) > 64:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    if "." in text:
        head, _, rest = text.partition(".")
        frac, tz = rest, ""
        for sep in ("+", "-"):
            if sep in rest:
                idx = rest.index(sep)
                frac, tz = rest[:idx], rest[idx:]
                break
        text = f"{head}.{frac[:6]}{tz}"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.timestamp()


def _find_matching_containers(
    docker_log_path: str, name_filter: str
) -> list[tuple[str, Path, float | None]]:
    """Return ``(container_id, log_path, created_epoch)`` for every matching container.

    Walks ``docker_log_path`` and checks each container's ``config.v2.json``. A
    container matches if ``name_filter`` (case-insensitive) is a substring of either
    its ``Name`` or its image reference (``Config.Image``). Checking the image allows
    reliable matching regardless of what the user named the container.

    Entries are skipped unless the directory name is a valid 64-char hex container ID,
    and symlinked container dirs / config / log files are rejected so a crafted entry
    under the watched tree cannot redirect reads to an arbitrary host file.
    """
    base = Path(docker_log_path)
    results: list[tuple[str, Path, float | None]] = []
    if not base.is_dir():
        return results

    needle = name_filter.lower()
    for container_dir in base.iterdir():
        if container_dir.is_symlink() or not container_dir.is_dir():
            continue
        if not _CONTAINER_ID_RE.match(container_dir.name):
            continue

        config_file = container_dir / "config.v2.json"
        if config_file.is_symlink() or not config_file.exists():
            continue
        try:
            with config_file.open() as f:
                config = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue

        name: str = config.get("Name", "")
        image: str = config.get("Config", {}).get("Image", "")
        if needle not in name.lower() and needle not in image.lower():
            continue

        log_file = container_dir / f"{container_dir.name}-json.log"
        if log_file.is_symlink() or not log_file.exists():
            continue

        results.append((container_dir.name, log_file, _parse_created(config.get("Created"))))
    return results


def _open_log_file(log_path: Path) -> IO[str]:
    """Open a container log file read-only, refusing to follow a final symlink.

    Uses ``O_NOFOLLOW`` on platforms that support it (Linux/macOS in production) to
    close the check-then-open TOCTOU window; falls back to a plain open elsewhere
    (e.g. Windows during local development).
    """
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    if nofollow:
        fd = os.open(str(log_path), os.O_RDONLY | nofollow)
        return os.fdopen(fd, "r", encoding="utf-8", errors="replace")
    return log_path.open("r", encoding="utf-8", errors="replace")


class _ReassemblyBuffer:
    """Accumulates Docker log fragments for one stream with O(1) size tracking."""

    def __init__(self) -> None:
        self._parts: list[str] = []
        self.size = 0

    def add(self, fragment: str) -> None:
        """Append a fragment to the buffer."""
        self._parts.append(fragment)
        self.size += len(fragment)

    def take(self) -> str:
        """Return the joined buffer contents and reset."""
        joined = "".join(self._parts)
        self.clear()
        return joined

    def clear(self) -> None:
        """Discard buffered contents."""
        self._parts.clear()
        self.size = 0


class DockerCollector(BaseCollector):
    """Tails the Docker JSON logs of all matching connector containers.

    Spawns one tailer task per matching container, fanning events into a shared queue
    exposed via :meth:`events`. A discovery loop rescans periodically so connectors
    that appear or disappear after startup are handled without a restart.
    """

    def __init__(
        self,
        docker_log_path: str,
        container_name_filter: str,
        discovery_interval_seconds: float = DEFAULT_DISCOVERY_INTERVAL,
        max_line_bytes: int = DEFAULT_MAX_LINE_BYTES,
        include_stderr: bool = True,
    ) -> None:
        """Initialise the collector.

        Args:
            docker_log_path: Path to the Docker containers directory.
            container_name_filter: Substring matched against container names/images.
            discovery_interval_seconds: How often to rescan for new/removed containers.
            max_line_bytes: Per-container reassembly/read cap (runaway-line guard).
            include_stderr: Also ship stderr lines (tagged ``_record_type=stderr``).
        """
        self._docker_log_path = docker_log_path
        self._name_filter = container_name_filter
        self._discovery_interval = discovery_interval_seconds
        self._max_line_bytes = max_line_bytes
        self._include_stderr = include_stderr

        self._queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=10_000)
        self._tailers: dict[str, asyncio.Task[None]] = {}
        self._discovery_task: asyncio.Task[None] | None = None
        self._start_time: float | None = None
        self._closed = False
        self._dropped_events = 0

    @property
    def dropped_events(self) -> int:
        """Total analytics lines dropped (parse failures, runaway lines)."""
        return self._dropped_events

    async def events(self) -> AsyncGenerator[dict[str, Any], None]:
        """Yield parsed analytics events from all matching containers."""
        if self._start_time is None:
            self._start_time = time.time()
        if self._discovery_task is None:
            self._discovery_task = asyncio.create_task(
                self._discover_loop(), name="docker-discovery"
            )

        while not self._closed:
            item = await self._queue.get()
            if not isinstance(item, dict):
                # _SENTINEL from close() — stop iteration.
                return
            yield item

    async def _discover_loop(self) -> None:
        """Periodically scan for matching containers and manage per-container tailers."""
        first = True
        while not self._closed:
            try:
                self._reconcile_tailers(first)
            except Exception as exc:
                # The loop must self-heal, never die: a failure here must not silently
                # stop dynamic discovery of new/removed containers (R4/R9).
                log.warning("docker_discovery_error", error=str(exc), component="docker_collector")
            first = False
            await asyncio.sleep(self._discovery_interval)

    def _reconcile_tailers(self, first: bool) -> None:
        """Start tailers for new containers and reap finished ones (one scan)."""
        try:
            containers = _find_matching_containers(self._docker_log_path, self._name_filter)
        except OSError:
            containers = []

        if not containers and first:
            log.error(
                "docker_log_not_found",
                docker_log_path=self._docker_log_path,
                name_filter=self._name_filter,
            )

        for container_id, log_path, created in containers:
            if container_id in self._tailers:
                continue
            # Seek policy (no history replay, no loss of genuinely-new traffic):
            #   first scan (startup)        -> seek to end
            #   later, Created after start  -> seek to start (new connector, capture birth)
            #   later, Created before start -> seek to end (adopted, move forward)
            late_arrival = not first
            if late_arrival and created is None:
                # Unknown age: bias to no-replay, but make the choice visible (R2).
                log.warning(
                    "container_created_unknown",
                    container_id=container_id,
                    component="docker_collector",
                )
            seek_to_start = bool(
                late_arrival
                and created is not None
                and self._start_time is not None
                and created > self._start_time
            )
            self._tailers[container_id] = asyncio.create_task(
                self._tail_container(container_id, log_path, seek_to_start),
                name=f"tailer-{container_id[:12]}",
            )
            log.info(
                "container_tailer_started",
                container_id=container_id,
                log_path=str(log_path),
                seek_to_start=seek_to_start,
                active_tailers=len(self._tailers),
                component="docker_collector",
            )

        for cid in [c for c, t in self._tailers.items() if t.done()]:
            task = self._tailers.pop(cid)
            exc = None if task.cancelled() else task.exception()
            if exc is not None:
                log.warning(
                    "container_tailer_failed",
                    container_id=cid,
                    error=str(exc),
                    component="docker_collector",
                )
            log.info(
                "container_tailer_stopped",
                container_id=cid,
                active_tailers=len(self._tailers),
                component="docker_collector",
            )

    async def _tail_container(self, container_id: str, log_path: Path, seek_to_start: bool) -> None:
        """Tail one container's JSON log, reassembling chunked lines, until it vanishes."""
        f: IO[str] | None = None
        file_inode: int | None = None
        # One reassembly buffer per Docker stream (stdout/stderr are chunked separately).
        buffers: dict[str, _ReassemblyBuffer] = {
            "stdout": _ReassemblyBuffer(),
            "stderr": _ReassemblyBuffer(),
        }
        try:
            while not self._closed:
                if f is None:
                    try:
                        f = _open_log_file(log_path)
                    except OSError:
                        return  # container/log removed or symlink refused
                    if not seek_to_start:
                        f.seek(0, 2)  # seek to end
                    file_inode = os.fstat(f.fileno()).st_ino

                pos = f.tell()
                # Bounded read: never allocate more than the cap for a single physical
                # line. A real Docker record is <=~16 KB, so hitting the cap without a
                # newline means a runaway/foreign line — resync rather than buffer it.
                line = f.readline(self._max_line_bytes)

                if not line:
                    if self._handle_eof(f, log_path, file_inode, buffers, container_id):
                        f = None
                        seek_to_start = True  # read the fresh file from its start
                        continue
                    if f is None:  # removed
                        return
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                if not line.endswith("\n"):
                    if len(line) >= self._max_line_bytes:
                        # Runaway single line — count, discard to next newline, resync.
                        self._dropped_events += 1
                        log.warning(
                            "docker_line_too_long",
                            container_id=container_id,
                            bytes_read=len(line),
                            component="docker_collector",
                        )
                        for b in buffers.values():
                            b.clear()
                        self._discard_to_newline(f)
                        continue
                    # Partial wrapper record still being written — rewind and wait.
                    f.seek(pos)
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                event = self._process_wrapper(line.rstrip("\n"), container_id, buffers)
                if event is not None:
                    await self._queue.put(event)
        finally:
            if f is not None:
                with contextlib.suppress(OSError):
                    f.close()

    def _handle_eof(
        self,
        f: IO[str],
        log_path: Path,
        file_inode: int | None,
        buffers: dict[str, _ReassemblyBuffer],
        container_id: str,
    ) -> bool:
        """At EOF, detect rotation/removal. Returns True if the file should be reopened.

        Returns False to keep polling the same handle. Closes ``f`` and returns False
        when the container has been removed (the caller then exits). Counts any
        partially-reassembled line discarded due to rotation/removal (R2).
        """
        try:
            current_inode = log_path.stat().st_ino
        except OSError:
            current_inode = None  # removed

        if current_inode == file_inode:
            return False  # same file, just no new data yet

        if any(b.size for b in buffers.values()):  # a line was mid-reassembly at the boundary
            self._dropped_events += 1
            log.warning(
                "line_truncated_at_rotation",
                container_id=container_id,
                component="docker_collector",
            )
            for b in buffers.values():
                b.clear()

        with contextlib.suppress(OSError):
            f.close()

        if current_inode is None:
            return False  # removed → caller sees f closed and exits

        log.info("docker_log_rotated", container_id=container_id, path=str(log_path))
        return True

    def _discard_to_newline(self, f: IO[str]) -> None:
        """Read and discard up to the next newline (used to resync after a runaway)."""
        while True:
            block = f.readline(self._max_line_bytes)
            if not block or block.endswith("\n"):
                return

    def _process_wrapper(
        self, wrapper_line: str, container_id: str, buffers: dict[str, _ReassemblyBuffer]
    ) -> dict[str, Any] | None:
        """Parse one Docker json-file record, reassemble, and parse a complete line.

        ``buffers`` holds the per-stream reassembly buffers, mutated in place. Returns a
        shipped record (analytics from stdout, or stderr line when ``include_stderr``)
        when a complete logical line is assembled, otherwise None (incomplete line,
        filtered stream, non-analytics stdout, or parse failure).
        """
        try:
            wrapper = json.loads(wrapper_line)
        except json.JSONDecodeError:
            log.warning("docker_wrapper_parse_error", component="docker_collector")
            log.debug("docker_wrapper_parse_error_raw", raw=wrapper_line[:200])
            return None

        stream = wrapper.get("stream", "stdout")
        if stream == "stderr" and not self._include_stderr:
            return None
        if stream not in buffers:
            return None  # unknown stream — ignore

        buf = buffers[stream]
        frag: str = wrapper.get("log", "")
        buf.add(frag)

        if buf.size > self._max_line_bytes:
            self._dropped_events += 1
            log.warning(
                "line_too_large",
                container_id=container_id,
                stream=stream,
                buffered_bytes=buf.size,
                component="docker_collector",
            )
            buf.clear()
            return None

        if not frag.endswith("\n"):
            return None  # incomplete logical line — keep buffering

        logical = buf.take().rstrip("\n")

        if stream == "stderr":
            return self._build_stderr_record(logical)

        if not logical.startswith(ANALYTICS_PREFIX):
            return None  # non-ANALYTICS stdout (connector service logs) is ignored

        event = self._parse_analytics(logical[len(ANALYTICS_PREFIX) :])
        if event is not None:
            event["_record_type"] = "analytics"
        return event

    def _build_stderr_record(self, line: str) -> dict[str, Any] | None:
        """Build a shipped record from one stderr line, tagged ``_record_type=stderr``.

        Extracts an embedded JSON object when present (e.g. the ``[ts] [metrics] {…}``
        emitter) so metrics arrive structured; otherwise keeps the line verbatim under
        ``_raw``. Blank lines are skipped.
        """
        if not line.strip():
            return None

        brace = line.find("{")
        if brace != -1:
            with contextlib.suppress(json.JSONDecodeError):
                obj, _ = json.JSONDecoder().raw_decode(line[brace:])
                if isinstance(obj, dict):
                    obj["_record_type"] = "stderr"
                    return obj

        return {"_record_type": "stderr", "_raw": line}

    def _parse_analytics(self, json_str: str) -> dict[str, Any] | None:
        """Parse an ANALYTICS payload, with a defensive re-sync fallback.

        Older producer builds could fuse a second log line onto the end of an analytics
        record; ``raw_decode`` salvages the leading valid object in that case. A genuine
        mid-object corruption is unrecoverable: it is counted and dropped (never silent).
        The raw payload (which contains analytics PII) is logged only at DEBUG.
        """
        obj: Any = None
        try:
            obj = json.loads(json_str)
        except json.JSONDecodeError:
            with contextlib.suppress(json.JSONDecodeError):
                obj, _ = json.JSONDecoder().raw_decode(json_str)

        if isinstance(obj, dict):
            return obj

        self._dropped_events += 1
        log.warning("analytics_parse_error", component="docker_collector")
        log.debug("analytics_parse_error_raw", raw=json_str[:200])
        return None

    async def close(self) -> None:
        """Stop discovery, cancel all tailers, and release file handles."""
        self._closed = True

        if self._discovery_task is not None:
            self._discovery_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._discovery_task
            self._discovery_task = None

        for task in list(self._tailers.values()):
            task.cancel()
        for task in list(self._tailers.values()):
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tailers.clear()

        # Unblock a pending events() get so the generator can stop.
        with contextlib.suppress(asyncio.QueueFull):
            self._queue.put_nowait(_SENTINEL)
