"""Entrypoint for twingate-log-shipper."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import subprocess
import sys
from typing import TYPE_CHECKING, Any

import structlog

from twingate_log_shipper.batcher import Batcher
from twingate_log_shipper.collectors.docker import DockerCollector
from twingate_log_shipper.config import ShipperConfig
from twingate_log_shipper.shipper import Shipper

if TYPE_CHECKING:
    from pathlib import Path

    from twingate_log_shipper.collectors.base import BaseCollector


def _configure_logging(log_level: str) -> None:
    """Configure structlog for JSON output to stdout."""
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper(), logging.INFO),
    )


def _create_collector(cfg: ShipperConfig) -> BaseCollector:
    """Instantiate the appropriate collector based on ``cfg.mode``.

    Raises:
        RuntimeError: If auto-detection fails or mode is unrecognised.
    """
    from twingate_log_shipper.collectors.journald import JournaldCollector

    mode = cfg.mode

    if mode == "auto":
        # Try Docker first if the containers directory exists
        if os.path.isdir(cfg.docker_log_path):
            return DockerCollector(cfg.docker_log_path, cfg.docker_container_name_filter)

        # Fall back to journald if the unit is active
        try:
            result = subprocess.run(
                ["systemctl", "is-active", cfg.journald_unit],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return JournaldCollector(cfg.journald_unit)
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass

        raise RuntimeError(
            f"Auto-detection failed: Docker containers dir '{cfg.docker_log_path}' not found "
            f"and '{cfg.journald_unit}' is not active. "
            "Set TWINGATE_SHIPPER_MODE=docker or TWINGATE_SHIPPER_MODE=journald explicitly."
        )

    if mode == "docker":
        return DockerCollector(cfg.docker_log_path, cfg.docker_container_name_filter)

    if mode == "journald":
        return JournaldCollector(cfg.journald_unit)

    raise RuntimeError(
        f"Unknown mode '{mode}'. Must be one of: auto, docker, journald. "
        "Set via TWINGATE_SHIPPER_MODE."
    )


async def _producer(
    collector: BaseCollector,
    event_queue: asyncio.Queue[dict[str, Any] | None],
) -> None:
    """Feed events from the collector into the event queue."""
    async for event in collector.events():
        await event_queue.put(event)


async def _async_main(cfg: ShipperConfig) -> int:
    """Run the log shipper. Returns exit code (0 = success, 1 = error)."""
    logger = structlog.get_logger("main")

    logger.info(
        "startup",
        mode=cfg.mode,
        s3_bucket=cfg.s3_bucket,
        s3_prefix=cfg.s3_prefix,
        s3_region=cfg.s3_region,
        s3_endpoint_url=cfg.s3_endpoint_url or "aws-default",
        batch_interval_seconds=cfg.batch_interval_seconds,
        batch_max_events=cfg.batch_max_events,
        batch_max_bytes=cfg.batch_max_bytes,
        upload_max_retries=cfg.upload_max_retries,
        component="main",
    )

    collector = _create_collector(cfg)

    event_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue(maxsize=10_000)
    # upload_queue carries (Path, s3_key) tuples from Batcher to Shipper.
    # Shipper expects Queue[tuple[Path, str] | None] because main.py puts a
    # None sentinel after batcher.run() returns. Batcher's signature omits
    # | None (it never sees the sentinel), so we suppress the invariant mismatch.
    upload_queue: asyncio.Queue[tuple[Path, str] | None] = asyncio.Queue(maxsize=1_000)

    batcher = Batcher(
        event_queue=event_queue,
        upload_queue=upload_queue,  # type: ignore[arg-type]
        s3_prefix=cfg.s3_prefix,
        batch_interval_seconds=cfg.batch_interval_seconds,
        batch_max_events=cfg.batch_max_events,
        batch_max_bytes=cfg.batch_max_bytes,
    )
    shipper = Shipper(
        upload_queue=upload_queue,
        s3_bucket=cfg.s3_bucket,
        s3_region=cfg.s3_region,
        s3_access_key_id=cfg.s3_access_key_id,
        s3_secret_access_key=cfg.s3_secret_access_key,
        s3_endpoint_url=cfg.s3_endpoint_url,
        upload_max_retries=cfg.upload_max_retries,
        upload_backoff_base=cfg.upload_backoff_base,
        upload_backoff_max=cfg.upload_backoff_max,
    )

    producer_task = asyncio.create_task(_producer(collector, event_queue), name="producer")
    batcher_task = asyncio.create_task(batcher.run(), name="batcher")
    shipper_task = asyncio.create_task(shipper.run(), name="shipper")

    shutdown_event = asyncio.Event()
    exit_code = 0

    def _handle_signal(signame: str) -> None:
        logger.info("shutdown_signal_received", signal=signame, component="main")
        shutdown_event.set()

    # NOTE: loop.add_signal_handler is Unix-only. On Windows this raises
    # NotImplementedError. This service runs as a Linux container or systemd
    # unit in production, so this is intentional.
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, _handle_signal, sig.name)

    # Wait for either a signal or the producer exiting unexpectedly
    shutdown_waiter = asyncio.create_task(shutdown_event.wait())
    _done, pending = await asyncio.wait(
        [producer_task, shutdown_waiter],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for _pending_task in pending:
        _pending_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await _pending_task

    if not shutdown_event.is_set():
        # Producer exited without a signal — unexpected
        logger.error("producer_exited_unexpectedly", component="main")
        exit_code = 1

    # ── Graceful shutdown sequence ─────────────────────────────────────────────
    logger.info("shutting_down", component="main")

    producer_task.cancel()
    with contextlib.suppress(asyncio.CancelledError, TimeoutError):
        await asyncio.wait_for(producer_task, timeout=2.0)

    await collector.close()

    # Signal batcher to stop, wait for it to flush
    await event_queue.put(None)
    half_timeout = cfg.shutdown_timeout_seconds / 2
    try:
        await asyncio.wait_for(batcher_task, timeout=half_timeout)
    except TimeoutError:
        logger.warning("batcher_shutdown_timeout", component="main")
        exit_code = 1

    # Signal shipper to stop, wait for final uploads
    await upload_queue.put(None)
    try:
        await asyncio.wait_for(shipper_task, timeout=half_timeout)
    except TimeoutError:
        logger.warning("shipper_shutdown_timeout", component="main")
        exit_code = 1

    logger.info("shutdown_complete", exit_code=exit_code, component="main")
    return exit_code


def run() -> None:
    """Synchronous entry point called by the installed console script."""
    cfg = ShipperConfig()  # type: ignore[call-arg]  # required fields come from env vars
    _configure_logging(cfg.log_level)
    exit_code = asyncio.run(_async_main(cfg))
    sys.exit(exit_code)


if __name__ == "__main__":
    run()
