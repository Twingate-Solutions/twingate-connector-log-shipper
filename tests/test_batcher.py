"""Unit tests for the Batcher component."""

import asyncio
import gzip
import json
import re
from pathlib import Path
from typing import Any

from twingate_log_shipper.batcher import Batcher


def _make_batcher(
    event_queue: asyncio.Queue[dict[str, Any] | None],
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    *,
    s3_prefix: str = "test-prefix",
    batch_interval_seconds: int = 3600,
    batch_max_events: int = 10_000,
    batch_max_bytes: int = 10_485_760,
) -> Batcher:
    """Construct a Batcher with sensible test defaults."""
    return Batcher(
        event_queue=event_queue,
        upload_queue=upload_queue,
        s3_prefix=s3_prefix,
        batch_interval_seconds=batch_interval_seconds,
        batch_max_events=batch_max_events,
        batch_max_bytes=batch_max_bytes,
    )


# ── Count threshold ───────────────────────────────────────────────────────────


async def test_count_threshold_triggers_flush(
    event_queue: asyncio.Queue[dict[str, Any] | None],
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    analytics_event: dict[str, Any],
) -> None:
    """Flush fires when batch_max_events events are accumulated."""
    batcher = _make_batcher(event_queue, upload_queue, batch_max_events=3)

    for _ in range(3):
        await event_queue.put(analytics_event)
    await event_queue.put(None)

    await asyncio.wait_for(batcher.run(), timeout=5.0)

    assert not upload_queue.empty()
    path, _key = upload_queue.get_nowait()
    path.unlink(missing_ok=True)


# ── Byte threshold ────────────────────────────────────────────────────────────


async def test_byte_threshold_triggers_flush(
    event_queue: asyncio.Queue[dict[str, Any] | None],
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    analytics_event: dict[str, Any],
) -> None:
    """Flush fires when batch_max_bytes of serialised NDJSON are accumulated."""
    # Each event serialises to roughly 300-400 bytes; set threshold to 200 bytes
    # so first event exceeds it.
    batcher = _make_batcher(event_queue, upload_queue, batch_max_bytes=200)

    await event_queue.put(analytics_event)
    await event_queue.put(None)

    await asyncio.wait_for(batcher.run(), timeout=5.0)

    assert not upload_queue.empty()
    path, _key = upload_queue.get_nowait()
    path.unlink(missing_ok=True)


# ── Time threshold ────────────────────────────────────────────────────────────


async def test_time_threshold_triggers_flush(
    event_queue: asyncio.Queue[dict[str, Any] | None],
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    analytics_event: dict[str, Any],
) -> None:
    """Flush fires after batch_interval_seconds even with few events."""
    batcher = _make_batcher(event_queue, upload_queue, batch_interval_seconds=1)

    await event_queue.put(analytics_event)

    # Don't send sentinel — let the time trigger fire
    # Then send sentinel after a slight delay
    async def _send_sentinel() -> None:
        await asyncio.sleep(1.5)
        await event_queue.put(None)

    await asyncio.gather(
        asyncio.wait_for(batcher.run(), timeout=5.0),
        _send_sentinel(),
    )

    assert not upload_queue.empty()
    path, _key = upload_queue.get_nowait()
    path.unlink(missing_ok=True)


# ── Sentinel flush ────────────────────────────────────────────────────────────


async def test_sentinel_flushes_remaining_events(
    event_queue: asyncio.Queue[dict[str, Any] | None],
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    analytics_event: dict[str, Any],
) -> None:
    """None sentinel causes remaining events to be flushed before batcher exits."""
    batcher = _make_batcher(
        event_queue,
        upload_queue,
        batch_max_events=1000,  # won't trigger on count
        batch_interval_seconds=3600,  # won't trigger on time
    )

    for _ in range(5):
        await event_queue.put(analytics_event)
    await event_queue.put(None)

    await asyncio.wait_for(batcher.run(), timeout=5.0)

    assert not upload_queue.empty()
    path, _key = upload_queue.get_nowait()
    path.unlink(missing_ok=True)


async def test_empty_batch_not_flushed(
    event_queue: asyncio.Queue[dict[str, Any] | None],
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
) -> None:
    """If no events arrive before sentinel, upload_queue stays empty."""
    batcher = _make_batcher(event_queue, upload_queue)

    await event_queue.put(None)

    await asyncio.wait_for(batcher.run(), timeout=5.0)

    assert upload_queue.empty()


# ── Output format ─────────────────────────────────────────────────────────────


async def test_output_is_valid_gzipped_ndjson(
    event_queue: asyncio.Queue[dict[str, Any] | None],
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    analytics_event: dict[str, Any],
) -> None:
    """Batch file is gzip-compressed valid NDJSON (one JSON object per line)."""
    batcher = _make_batcher(event_queue, upload_queue, batch_max_events=2)

    await event_queue.put(analytics_event)
    await event_queue.put(analytics_event)
    await event_queue.put(None)

    await asyncio.wait_for(batcher.run(), timeout=5.0)

    path, _key = upload_queue.get_nowait()

    # Must decompress successfully
    raw = gzip.decompress(path.read_bytes()).decode()

    lines = [line for line in raw.splitlines() if line]
    assert len(lines) == 2
    for line in lines:
        obj = json.loads(line)
        assert obj["event_type"] == "closed_connection"

    path.unlink(missing_ok=True)


async def test_s3_key_format(
    event_queue: asyncio.Queue[dict[str, Any] | None],
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    analytics_event: dict[str, Any],
) -> None:
    """S3 key matches {prefix}/{YYYY}/{MM}/{DD}/{HH}-{MM}_{uuid8}.ndjson.gz"""
    batcher = _make_batcher(event_queue, upload_queue, s3_prefix="my-prefix", batch_max_events=1)

    await event_queue.put(analytics_event)
    await event_queue.put(None)

    await asyncio.wait_for(batcher.run(), timeout=5.0)

    path, key = upload_queue.get_nowait()
    path.unlink(missing_ok=True)

    pattern = r"^my-prefix/\d{4}/\d{2}/\d{2}/\d{2}-\d{2}_[0-9a-f]{8}\.ndjson\.gz$"
    assert re.match(pattern, key), f"S3 key {key!r} does not match expected pattern"


async def test_multiple_flushes_produce_multiple_uploads(
    event_queue: asyncio.Queue[dict[str, Any] | None],
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    analytics_event: dict[str, Any],
) -> None:
    """Two batches of max_events each produce two separate upload jobs."""
    batcher = _make_batcher(event_queue, upload_queue, batch_max_events=2)

    for _ in range(4):
        await event_queue.put(analytics_event)
    await event_queue.put(None)

    await asyncio.wait_for(batcher.run(), timeout=5.0)

    paths = []
    while not upload_queue.empty():
        p, _ = upload_queue.get_nowait()
        paths.append(p)

    assert len(paths) == 2
    for p in paths:
        p.unlink(missing_ok=True)
