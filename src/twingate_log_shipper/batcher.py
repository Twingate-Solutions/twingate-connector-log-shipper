"""Batch accumulator: collects events and writes gzipped NDJSON batch files."""

import asyncio
import gzip
import json
import os
import tempfile
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger(__name__)


def _write_compressed(path: Path, data: bytes) -> None:
    """Write gzip-compressed data to path (runs in a thread pool)."""
    with gzip.open(path, "wb") as gz:
        gz.write(data)


class Batcher:
    """Accumulates analytics events and flushes them as gzipped NDJSON files.

    Flush is triggered by whichever threshold is hit first:
    - ``batch_interval_seconds`` have elapsed since the last flush
    - ``batch_max_events`` events have accumulated
    - ``batch_max_bytes`` of serialised NDJSON have accumulated

    Each flush writes a gzip-compressed temp file and puts a
    ``(local_path, s3_key)`` tuple onto ``upload_queue`` for the Shipper.
    """

    def __init__(
        self,
        event_queue: asyncio.Queue[dict[str, Any] | None],
        upload_queue: asyncio.Queue[tuple[Path, str]],
        s3_prefix: str,
        batch_interval_seconds: int,
        batch_max_events: int,
        batch_max_bytes: int,
    ) -> None:
        """Initialise the batcher.

        Args:
            event_queue: Source queue of parsed event dicts.
                         A ``None`` sentinel signals graceful shutdown.
            upload_queue: Destination queue of ``(local_path, s3_key)`` tuples.
            s3_prefix: S3 key prefix (e.g. ``twingate-analytics``).
            batch_interval_seconds: Maximum age of a batch before forced flush.
            batch_max_events: Maximum events per batch before flush.
            batch_max_bytes: Maximum serialised bytes per batch before flush.
        """
        self._event_queue = event_queue
        self._upload_queue = upload_queue
        self._s3_prefix = s3_prefix
        self._interval = batch_interval_seconds
        self._max_events = batch_max_events
        self._max_bytes = batch_max_bytes

        self._batch: list[str] = []
        self._batch_bytes: int = 0
        self._last_flush: float = 0.0

    async def run(self) -> None:
        """Consume events from event_queue and flush batches as needed.

        Returns when a ``None`` sentinel is received from the queue,
        flushing any remaining events before returning.
        """
        self._last_flush = time.monotonic()

        while True:
            # Calculate remaining time until forced flush
            elapsed = time.monotonic() - self._last_flush
            remaining = max(0.01, self._interval - elapsed)

            try:
                event = await asyncio.wait_for(self._event_queue.get(), timeout=remaining)
            except TimeoutError:
                # Time threshold hit
                if self._batch:
                    await self._flush()
                continue

            if event is None:
                # Shutdown sentinel — flush remaining events and exit
                if self._batch:
                    await self._flush()
                break

            # Serialise and accumulate
            line = json.dumps(event, separators=(",", ":"))
            line_bytes = len(line.encode()) + 1  # +1 for \n
            self._batch.append(line)
            self._batch_bytes += line_bytes

            # Check count and byte thresholds
            if len(self._batch) >= self._max_events or self._batch_bytes >= self._max_bytes:
                await self._flush()

    async def _flush(self) -> None:
        """Write the current batch to a gzipped temp file and enqueue for upload."""
        if not self._batch:
            return

        event_count = len(self._batch)
        byte_count = self._batch_bytes

        # Build NDJSON content
        ndjson_content = "\n".join(self._batch) + "\n"
        ndjson_bytes = ndjson_content.encode()

        # Generate S3 key
        now = datetime.now(UTC)
        short_uuid = uuid.uuid4().hex[:8]
        s3_key = (
            f"{self._s3_prefix}/"
            f"{now.year:04d}/{now.month:02d}/{now.day:02d}/"
            f"{now.hour:02d}-{now.minute:02d}_{short_uuid}.ndjson.gz"
        )

        # Generate temp file path and write compressed data in thread pool
        tmp_fd, tmp_name = tempfile.mkstemp(suffix=".ndjson.gz", prefix="twingate_batch_")
        os.close(tmp_fd)
        tmp_path = Path(tmp_name)
        await asyncio.to_thread(_write_compressed, tmp_path, ndjson_bytes)

        log.info(
            "batch_flushed",
            event_count=event_count,
            uncompressed_bytes=byte_count,
            s3_key=s3_key,
            component="batcher",
        )

        await self._upload_queue.put((tmp_path, s3_key))

        # Reset accumulator
        self._batch = []
        self._batch_bytes = 0
        self._last_flush = time.monotonic()
