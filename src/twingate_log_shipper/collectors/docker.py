"""Docker JSON log file collector for Twingate connector analytics."""

import asyncio
import contextlib
import json
import os
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import IO, Any

import structlog

from twingate_log_shipper.collectors.base import BaseCollector

log = structlog.get_logger(__name__)

ANALYTICS_PREFIX = "ANALYTICS "
POLL_INTERVAL = 0.5  # seconds between EOF polls


def _find_container_log(docker_log_path: str, name_filter: str) -> Path | None:
    """Return the path to the active JSON log file for the first matching container.

    Walks ``docker_log_path`` and checks each container's ``config.v2.json``.
    A container matches if ``name_filter`` (case-insensitive) is a substring of
    either the container's ``Name`` or its image reference (``Config.Image``).
    Checking the image allows reliable matching regardless of what the user
    has named the container.

    Returns None if no matching container is found.
    """
    base = Path(docker_log_path)
    if not base.is_dir():
        return None

    needle = name_filter.lower()
    for container_dir in base.iterdir():
        if not container_dir.is_dir():
            continue
        config_file = container_dir / "config.v2.json"
        if not config_file.exists():
            continue
        try:
            with config_file.open() as f:
                config = json.load(f)
            name: str = config.get("Name", "")
            image: str = config.get("Config", {}).get("Image", "")
            if needle in name.lower() or needle in image.lower():
                log_file = container_dir / f"{container_dir.name}-json.log"
                if log_file.exists():
                    return log_file
        except (OSError, json.JSONDecodeError):
            continue
    return None


class DockerCollector(BaseCollector):
    """Tails the Docker JSON log file for a Twingate connector container.

    Seeks to the end of the file on startup (does not replay historical logs).
    Handles log rotation by detecting inode changes at EOF.
    """

    def __init__(self, docker_log_path: str, container_name_filter: str) -> None:
        """Initialise the collector.

        Args:
            docker_log_path: Path to the Docker containers directory.
            container_name_filter: Substring matched against container names.
        """
        self._docker_log_path = docker_log_path
        self._name_filter = container_name_filter
        self._file: IO[str] | None = None

    async def events(self) -> AsyncGenerator[dict[str, Any], None]:
        """Yield parsed analytics events from the Docker JSON log file."""
        log_path: Path | None = None
        file_inode: int | None = None
        f = None

        while True:
            # ── Locate log file ───────────────────────────────────────────────
            if log_path is None:
                log_path = _find_container_log(self._docker_log_path, self._name_filter)
                if log_path is None:
                    log.error(
                        "docker_log_not_found",
                        docker_log_path=self._docker_log_path,
                        name_filter=self._name_filter,
                    )
                    await asyncio.sleep(5.0)
                    continue

            # ── Open file (seek to end on first open) ─────────────────────────
            if f is None:
                try:
                    f = log_path.open("r", encoding="utf-8", errors="replace")
                    f.seek(0, 2)  # seek to end
                    file_inode = os.fstat(f.fileno()).st_ino
                    self._file = f
                    log.info("docker_collector_started", log_path=str(log_path))
                except OSError as exc:
                    log.error("docker_log_open_failed", path=str(log_path), error=str(exc))
                    f = None
                    await asyncio.sleep(5.0)
                    continue

            # ── Read available lines ──────────────────────────────────────────
            line = f.readline()
            if not line:
                # EOF — check for rotation before sleeping
                try:
                    current_inode = log_path.stat().st_ino
                except FileNotFoundError:
                    current_inode = None

                if current_inode != file_inode:
                    log.info("docker_log_rotated", path=str(log_path))
                    f.close()
                    f = None
                    self._file = None
                    log_path = None
                    file_inode = None
                    continue

                await asyncio.sleep(POLL_INTERVAL)
                continue

            # ── Parse Docker JSON wrapper ─────────────────────────────────────
            line = line.rstrip("\n")
            try:
                wrapper = json.loads(line)
                raw_log_line: str = wrapper.get("log", "")
            except json.JSONDecodeError:
                log.warning("docker_wrapper_parse_error", raw=line[:200])
                continue

            raw_log_line = raw_log_line.rstrip("\n")

            # ── Filter and parse ANALYTICS lines ──────────────────────────────
            if not raw_log_line.startswith(ANALYTICS_PREFIX):
                continue

            json_str = raw_log_line[len(ANALYTICS_PREFIX) :]
            try:
                event = json.loads(json_str)
            except json.JSONDecodeError:
                log.warning(
                    "analytics_parse_error",
                    raw=json_str[:200],
                    component="docker_collector",
                )
                continue

            yield event

    async def close(self) -> None:
        """Close the open log file handle if any."""
        if self._file is not None:
            with contextlib.suppress(OSError):
                self._file.close()
            self._file = None
