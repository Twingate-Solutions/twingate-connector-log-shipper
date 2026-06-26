"""Unit tests for DockerCollector and JournaldCollector."""

import asyncio
import json
import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import (
    make_container,
    make_docker_chunked_lines,
    make_docker_log_line,
)
from twingate_log_shipper.collectors.base import BaseCollector
from twingate_log_shipper.collectors.docker import DockerCollector
from twingate_log_shipper.collectors.journald import JournaldCollector

# ── Helpers ──────────────────────────────────────────────────────────────────


async def _collect_n(
    collector: BaseCollector,
    n: int,
    timeout: float = 2.0,
) -> list[dict[str, Any]]:
    """Drive a collector's events() generator until n events are yielded."""
    events: list[dict[str, Any]] = []

    async def _run() -> None:
        async for event in collector.events():
            events.append(event)
            if len(events) >= n:
                break

    await asyncio.wait_for(_run(), timeout=timeout)
    return events


# ── DockerCollector ───────────────────────────────────────────────────────────


async def test_docker_collector_yields_analytics_event(
    docker_log_dir: Path,
    docker_log_file: Path,
    analytics_event: dict[str, Any],
) -> None:
    """ANALYTICS line is parsed and yielded with correct fields."""
    collector = DockerCollector(str(docker_log_dir), "twingate")
    line = make_docker_log_line(f"ANALYTICS {json.dumps(analytics_event)}")

    async def _write() -> None:
        await asyncio.sleep(0.05)
        docker_log_file.write_text(line)

    events, _ = await asyncio.gather(
        _collect_n(collector, 1),
        _write(),
    )
    await collector.close()

    assert len(events) == 1
    assert events[0]["event_type"] == "closed_connection"
    assert events[0]["user"]["email"] == "user@example.com"


async def test_docker_collector_skips_non_analytics_lines(
    docker_log_dir: Path,
    docker_log_file: Path,
    analytics_event: dict[str, Any],
) -> None:
    """Non-ANALYTICS lines (INFO, ERROR, etc.) are silently skipped."""
    collector = DockerCollector(str(docker_log_dir), "twingate")
    noise = make_docker_log_line("INFO Connected to relay")
    valid = make_docker_log_line(f"ANALYTICS {json.dumps(analytics_event)}")

    async def _write() -> None:
        await asyncio.sleep(0.05)
        docker_log_file.write_text(noise + valid)

    events, _ = await asyncio.gather(
        _collect_n(collector, 1),
        _write(),
    )
    await collector.close()

    assert len(events) == 1
    assert events[0]["event_type"] == "closed_connection"


async def test_docker_collector_skips_malformed_analytics_json(
    docker_log_dir: Path,
    docker_log_file: Path,
    analytics_event: dict[str, Any],
) -> None:
    """Malformed ANALYTICS JSON is skipped; subsequent valid events still yielded."""
    collector = DockerCollector(str(docker_log_dir), "twingate")
    bad = make_docker_log_line("ANALYTICS {not valid json!!}")
    good = make_docker_log_line(f"ANALYTICS {json.dumps(analytics_event)}")

    async def _write() -> None:
        await asyncio.sleep(0.05)
        docker_log_file.write_text(bad + good)

    events, _ = await asyncio.gather(
        _collect_n(collector, 1),
        _write(),
    )
    await collector.close()

    assert len(events) == 1
    assert events[0]["event_type"] == "closed_connection"


async def test_docker_collector_yields_multiple_events_in_order(
    docker_log_dir: Path,
    docker_log_file: Path,
    analytics_event: dict[str, Any],
) -> None:
    """Multiple ANALYTICS lines are yielded in file order."""
    collector = DockerCollector(str(docker_log_dir), "twingate")
    event2 = {**analytics_event, "event_type": "established_connection"}
    content = make_docker_log_line(
        f"ANALYTICS {json.dumps(analytics_event)}"
    ) + make_docker_log_line(f"ANALYTICS {json.dumps(event2)}")

    async def _write() -> None:
        await asyncio.sleep(0.05)
        docker_log_file.write_text(content)

    events, _ = await asyncio.gather(
        _collect_n(collector, 2),
        _write(),
    )
    await collector.close()

    assert len(events) == 2
    assert events[0]["event_type"] == "closed_connection"
    assert events[1]["event_type"] == "established_connection"


async def test_docker_collector_handles_missing_container(
    tmp_path: Path,
) -> None:
    """DockerCollector instantiates successfully even when no matching container exists."""
    collector = DockerCollector(str(tmp_path), "nonexistent")
    assert collector is not None
    await collector.close()


async def test_docker_collector_close_is_idempotent(
    docker_log_dir: Path,
) -> None:
    """Calling close() multiple times does not raise an exception."""
    collector = DockerCollector(str(docker_log_dir), "twingate")
    await collector.close()
    await collector.close()


# ── DockerCollector: line reassembly (>16 KB chunking) ────────────────────────


async def test_docker_collector_reassembles_chunked_analytics_line(
    docker_log_dir: Path,
    docker_log_file: Path,
    analytics_event: dict[str, Any],
) -> None:
    """A single ANALYTICS line split across multiple json-file records is reassembled.

    Docker splits any stdout line >16 KB into several log records, only the last
    ending in a newline. The collector must concatenate them into one logical line
    before parsing, yielding exactly one fully-parsed event.
    """
    collector = DockerCollector(str(docker_log_dir), "twingate")
    # Inflate the event well past the 16 KB chunk boundary so Docker would split it.
    big_event = {**analytics_event, "_pad": "x" * 20000}
    message = f"ANALYTICS {json.dumps(big_event)}"
    chunked = make_docker_chunked_lines(message, chunk_size=16384)
    # Sanity: the helper must actually produce more than one record.
    assert chunked.count('"stream"') >= 2

    async def _write() -> None:
        await asyncio.sleep(0.05)
        docker_log_file.write_text(chunked)

    events, _ = await asyncio.gather(_collect_n(collector, 1), _write())
    await collector.close()

    assert len(events) == 1
    assert events[0]["event_type"] == "closed_connection"
    assert len(events[0]["_pad"]) == 20000


async def test_docker_collector_ignores_stderr_when_disabled(
    docker_log_dir: Path,
    docker_log_file: Path,
    analytics_event: dict[str, Any],
) -> None:
    """With include_stderr=False, stderr records are ignored; stdout ANALYTICS is yielded."""
    collector = DockerCollector(str(docker_log_dir), "twingate", include_stderr=False)
    metrics = make_docker_log_line(
        '[2026-06-26 17:54:01] [metrics] {"event":"metrics","cpu_pct":0.33}',
        stream="stderr",
    )
    valid = make_docker_log_line(f"ANALYTICS {json.dumps(analytics_event)}")

    async def _write() -> None:
        await asyncio.sleep(0.05)
        docker_log_file.write_text(metrics + valid)

    events, _ = await asyncio.gather(_collect_n(collector, 1), _write())
    await collector.close()

    assert len(events) == 1
    assert events[0]["event_type"] == "closed_connection"


async def test_docker_collector_ships_stderr_metrics_parsed(
    docker_log_dir: Path,
    docker_log_file: Path,
) -> None:
    """A stderr [metrics] line is parsed into a structured record tagged _record_type=stderr."""
    collector = DockerCollector(str(docker_log_dir), "twingate")  # include_stderr defaults True
    metrics = make_docker_log_line(
        '[2026-06-26 17:54:01] [metrics] {"ts":"2026-06-26T17:54:01Z",'
        '"event":"metrics","cpu_pct":0.33,"mem_bytes":109379584}',
        stream="stderr",
    )

    async def _write() -> None:
        await asyncio.sleep(0.05)
        docker_log_file.write_text(metrics)

    events, _ = await asyncio.gather(_collect_n(collector, 1), _write())
    await collector.close()

    assert len(events) == 1
    assert events[0]["_record_type"] == "stderr"
    assert events[0]["event"] == "metrics"
    assert events[0]["cpu_pct"] == 0.33


async def test_docker_collector_ships_stderr_raw_when_not_json(
    docker_log_dir: Path,
    docker_log_file: Path,
) -> None:
    """A non-JSON stderr line is shipped as a raw record tagged _record_type=stderr."""
    collector = DockerCollector(str(docker_log_dir), "twingate")
    err = make_docker_log_line("ERROR failed to reach relay: timeout", stream="stderr")

    async def _write() -> None:
        await asyncio.sleep(0.05)
        docker_log_file.write_text(err)

    events, _ = await asyncio.gather(_collect_n(collector, 1), _write())
    await collector.close()

    assert len(events) == 1
    assert events[0]["_record_type"] == "stderr"
    assert events[0]["_raw"] == "ERROR failed to reach relay: timeout"


async def test_docker_collector_tags_analytics_records(
    docker_log_dir: Path,
    docker_log_file: Path,
    analytics_event: dict[str, Any],
) -> None:
    """Analytics events are tagged _record_type=analytics (additive, fields preserved)."""
    collector = DockerCollector(str(docker_log_dir), "twingate")
    valid = make_docker_log_line(f"ANALYTICS {json.dumps(analytics_event)}")

    async def _write() -> None:
        await asyncio.sleep(0.05)
        docker_log_file.write_text(valid)

    events, _ = await asyncio.gather(_collect_n(collector, 1), _write())
    await collector.close()

    assert len(events) == 1
    assert events[0]["_record_type"] == "analytics"
    assert events[0]["event_type"] == "closed_connection"


async def test_docker_collector_drops_and_counts_oversized_reassembly(
    docker_log_dir: Path,
    docker_log_file: Path,
    analytics_event: dict[str, Any],
) -> None:
    """Chunk fragments accumulating past the buffer cap are dropped and counted.

    Each record's ``log`` lacks a trailing newline (a chunk of a still-unterminated
    line), so they accumulate in the reassembly buffer. Once the total exceeds
    ``max_line_bytes`` the buffer is discarded, the drop counted, and the collector
    keeps running to parse the next valid event.
    """
    collector = DockerCollector(str(docker_log_dir), "twingate", max_line_bytes=1000)
    # Three 400-byte fragments (no trailing newline) → 1200 bytes buffered > 1000 cap.
    # Each wrapper record is itself well under the cap and ends with a newline.
    fragment = json.dumps({"log": "x" * 400, "stream": "stdout", "time": "t"}) + "\n"
    runaway = fragment * 3
    valid = make_docker_log_line(f"ANALYTICS {json.dumps(analytics_event)}")

    async def _write() -> None:
        await asyncio.sleep(0.05)
        docker_log_file.write_text(runaway + valid)

    events, _ = await asyncio.gather(_collect_n(collector, 1), _write())
    await collector.close()

    assert len(events) == 1  # collector survived and yielded the valid event
    assert collector.dropped_events >= 1


async def test_docker_collector_resyncs_on_trailing_garbage(
    docker_log_dir: Path,
    docker_log_file: Path,
    analytics_event: dict[str, Any],
) -> None:
    """A valid ANALYTICS object with trailing garbage appended is still recovered."""
    collector = DockerCollector(str(docker_log_dir), "twingate")
    fused = f"ANALYTICS {json.dumps(analytics_event)} [metrics] trailing junk"
    line = make_docker_log_line(fused)

    async def _write() -> None:
        await asyncio.sleep(0.05)
        docker_log_file.write_text(line)

    events, _ = await asyncio.gather(_collect_n(collector, 1), _write())
    await collector.close()

    assert len(events) == 1
    assert events[0]["event_type"] == "closed_connection"


# ── DockerCollector: multi-container + dynamic discovery ───────────────────────


async def test_docker_collector_captures_all_matching_containers(
    tmp_path: Path,
    analytics_event: dict[str, Any],
) -> None:
    """Events from every matching container are captured, not just the first."""
    log_a = make_container(tmp_path, "a" * 64, "/twingate-connector-1")
    log_b = make_container(tmp_path, "b" * 64, "/twingate-connector-2")
    event_a = {**analytics_event, "connector": {"name": "conn-a", "id": "1001"}}
    event_b = {**analytics_event, "connector": {"name": "conn-b", "id": "1002"}}

    collector = DockerCollector(str(tmp_path), "twingate", discovery_interval_seconds=0.1)

    async def _write() -> None:
        await asyncio.sleep(0.1)
        log_a.write_text(make_docker_log_line(f"ANALYTICS {json.dumps(event_a)}"))
        log_b.write_text(make_docker_log_line(f"ANALYTICS {json.dumps(event_b)}"))

    events, _ = await asyncio.gather(_collect_n(collector, 2, timeout=4.0), _write())
    await collector.close()

    assert len(events) == 2
    assert {e["connector"]["id"] for e in events} == {"1001", "1002"}


async def test_docker_collector_discovers_new_container_at_runtime(
    tmp_path: Path,
    analytics_event: dict[str, Any],
) -> None:
    """A connector that appears after startup (FC scale-up) is discovered and captured.

    The late-arriving container has a Created timestamp after the collector started, so
    its pre-written backlog is captured (seek-to-start), proving no loss in the window.
    """
    log_a = make_container(tmp_path, "a" * 64, "/twingate-connector-1")
    event_a = {**analytics_event, "connector": {"name": "conn-a", "id": "1001"}}
    event_b = {**analytics_event, "connector": {"name": "conn-b", "id": "1002"}}

    collector = DockerCollector(str(tmp_path), "twingate", discovery_interval_seconds=0.1)

    async def _write() -> None:
        await asyncio.sleep(0.1)
        log_a.write_text(make_docker_log_line(f"ANALYTICS {json.dumps(event_a)}"))
        await asyncio.sleep(0.3)
        # New connector scaled up after start: Created in the future forces seek-to-start.
        log_b = make_container(
            tmp_path, "b" * 64, "/twingate-connector-2", created="2099-01-01T00:00:00Z"
        )
        log_b.write_text(make_docker_log_line(f"ANALYTICS {json.dumps(event_b)}"))

    events, _ = await asyncio.gather(_collect_n(collector, 2, timeout=5.0), _write())
    await collector.close()

    assert {e["connector"]["id"] for e in events} == {"1001", "1002"}


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="open log files cannot be unlinked on Windows; container removal is a Linux-prod path",
)
async def test_docker_collector_survives_container_removal(
    tmp_path: Path,
    analytics_event: dict[str, Any],
) -> None:
    """Removing one container's log file does not stop capture from the others."""
    import shutil

    log_a = make_container(tmp_path, "a" * 64, "/twingate-connector-1")
    log_b = make_container(tmp_path, "b" * 64, "/twingate-connector-2")
    event_a = {**analytics_event, "connector": {"name": "conn-a", "id": "1001"}}
    event_b2 = {**analytics_event, "connector": {"name": "conn-b", "id": "1002"}}

    collector = DockerCollector(str(tmp_path), "twingate", discovery_interval_seconds=0.1)

    async def _run() -> list[dict[str, Any]]:
        collected: list[dict[str, Any]] = []

        async def _drive() -> None:
            async for event in collector.events():
                collected.append(event)
                if len(collected) >= 2:
                    break

        async def _write() -> None:
            await asyncio.sleep(0.1)
            log_a.write_text(make_docker_log_line(f"ANALYTICS {json.dumps(event_a)}"))
            await asyncio.sleep(0.3)
            shutil.rmtree(tmp_path / ("a" * 64))  # connector A scaled down
            await asyncio.sleep(0.2)
            log_b.write_text(make_docker_log_line(f"ANALYTICS {json.dumps(event_b2)}"))

        await asyncio.gather(asyncio.wait_for(_drive(), timeout=5.0), _write())
        return collected

    events = await _run()
    await collector.close()

    assert {e["connector"]["id"] for e in events} == {"1001", "1002"}


def test_find_matching_containers_ignores_non_hex_dirs(
    tmp_path: Path,
    analytics_event: dict[str, Any],
) -> None:
    """Only directories named like a 64-char hex container ID are considered."""
    from twingate_log_shipper.collectors.docker import _find_matching_containers

    # A bogus dir that matches the name filter but is not a real container ID.
    make_container(tmp_path, "x" * 64, "/twingate-fake")  # invalid: 'x' not hex
    valid = make_container(tmp_path, "a1b2c3" + "0" * 58, "/twingate-real")

    matches = _find_matching_containers(str(tmp_path), "twingate")

    assert len(matches) == 1
    assert matches[0][1] == valid


# ── JournaldCollector ─────────────────────────────────────────────────────────


@pytest.fixture
def mock_journal() -> Generator[tuple[MagicMock, MagicMock, object], None, None]:
    """Mock systemd.journal module for use in JournaldCollector tests.

    Yields (mock_journal_module, mock_reader, APPEND_SENTINEL).
    """
    APPEND = object()  # unique sentinel that matches journal.APPEND

    mock_reader = MagicMock()
    mock_reader.process.return_value = None  # default: NOP (causes sleep)
    mock_reader.__iter__ = MagicMock(return_value=iter([]))  # default: no entries

    mock_jrnl_module = MagicMock()
    mock_jrnl_module.LOG_INFO = 6
    mock_jrnl_module.APPEND = APPEND
    mock_jrnl_module.Reader.return_value = mock_reader

    mock_systemd_module = MagicMock()
    mock_systemd_module.journal = mock_jrnl_module

    with patch.dict(
        sys.modules,
        {
            "systemd": mock_systemd_module,
            "systemd.journal": mock_jrnl_module,
        },
    ):
        yield mock_jrnl_module, mock_reader, APPEND


def test_journald_collector_raises_if_systemd_not_installed() -> None:
    """JournaldCollector raises RuntimeError when systemd-python is not available."""
    with (
        patch.dict(sys.modules, {"systemd": None, "systemd.journal": None}),
        pytest.raises(RuntimeError, match="systemd-python"),
    ):
        JournaldCollector("twingate-connector.service")


async def test_journald_collector_yields_analytics_event(
    mock_journal: Any,
    analytics_event: dict[str, Any],
) -> None:
    """ANALYTICS entries from journald are parsed and yielded."""
    _mock_jrnl, mock_reader, APPEND = mock_journal

    entry = {"MESSAGE": f"ANALYTICS {json.dumps(analytics_event)}"}

    call_count = 0

    def _process() -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            mock_reader.__iter__ = MagicMock(return_value=iter([entry]))
            return APPEND
        return None  # NOP on subsequent calls → triggers sleep → test timeout

    mock_reader.process.side_effect = _process

    collector = JournaldCollector("twingate-connector.service")
    events: list[dict[str, Any]] = []

    async def _run() -> None:
        async for event in collector.events():
            events.append(event)
            break

    await asyncio.wait_for(_run(), timeout=2.0)
    await collector.close()

    assert len(events) == 1
    assert events[0]["event_type"] == "closed_connection"


async def test_journald_collector_skips_non_analytics(
    mock_journal: Any,
    analytics_event: dict[str, Any],
) -> None:
    """Non-ANALYTICS journal entries are silently skipped."""
    _mock_jrnl, mock_reader, APPEND = mock_journal

    entries = [
        {"MESSAGE": "INFO Connected to relay"},
        {"MESSAGE": f"ANALYTICS {json.dumps(analytics_event)}"},
    ]

    call_count = 0

    def _process() -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            mock_reader.__iter__ = MagicMock(return_value=iter(entries))
            return APPEND
        return None

    mock_reader.process.side_effect = _process

    collector = JournaldCollector("twingate-connector.service")
    events: list[dict[str, Any]] = []

    async def _run() -> None:
        async for event in collector.events():
            events.append(event)
            break

    await asyncio.wait_for(_run(), timeout=2.0)
    await collector.close()

    assert len(events) == 1
    assert events[0]["event_type"] == "closed_connection"


async def test_journald_collector_skips_malformed_json(
    mock_journal: Any,
    analytics_event: dict[str, Any],
) -> None:
    """Malformed ANALYTICS JSON in journal entries is skipped; valid events still yielded."""
    _mock_jrnl, mock_reader, APPEND = mock_journal

    entries = [
        {"MESSAGE": "ANALYTICS {not valid json}"},
        {"MESSAGE": f"ANALYTICS {json.dumps(analytics_event)}"},
    ]

    call_count = 0

    def _process() -> object:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            mock_reader.__iter__ = MagicMock(return_value=iter(entries))
            return APPEND
        return None

    mock_reader.process.side_effect = _process

    collector = JournaldCollector("twingate-connector.service")
    events: list[dict[str, Any]] = []

    async def _run() -> None:
        async for event in collector.events():
            events.append(event)
            break

    await asyncio.wait_for(_run(), timeout=2.0)
    await collector.close()

    assert len(events) == 1
    assert events[0]["event_type"] == "closed_connection"
