"""Unit tests for DockerCollector and JournaldCollector."""

import asyncio
import json
import sys
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import make_docker_log_line
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
