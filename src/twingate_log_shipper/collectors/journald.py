"""systemd journal collector for Twingate connector analytics."""

import asyncio
import contextlib
import json
from collections.abc import AsyncGenerator
from typing import Any

import structlog

from twingate_log_shipper.collectors.base import BaseCollector

log = structlog.get_logger(__name__)

ANALYTICS_PREFIX = "ANALYTICS "
POLL_INTERVAL = 0.5  # seconds between journal polls


class JournaldCollector(BaseCollector):
    """Reads analytics events from the systemd journal.

    Requires the ``systemd-python`` package (``pip install systemd-python``),
    which is only available on Linux systems with libsystemd installed.

    Seeks to tail on startup — does not replay historical journal entries.
    """

    def __init__(self, unit: str) -> None:
        """Initialise the collector.

        Args:
            unit: systemd unit name, e.g. ``twingate-connector.service``.

        Raises:
            RuntimeError: If ``systemd-python`` is not installed.
        """
        try:
            import systemd.journal  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "journald mode requires systemd-python: pip install twingate-log-shipper[journald]"
            ) from exc

        self._unit = unit
        self._reader: Any | None = None

    async def events(self) -> AsyncGenerator[dict[str, Any], None]:
        """Yield parsed analytics events from the systemd journal."""
        import systemd.journal

        reader = systemd.journal.Reader()
        reader.log_level(systemd.journal.LOG_INFO)
        reader.add_match(_SYSTEMD_UNIT=self._unit)
        reader.seek_tail()
        reader.get_previous()  # consume tail marker so next entries are new
        self._reader = reader

        log.info("journald_collector_started", unit=self._unit)

        while True:
            change = reader.process()

            if change == systemd.journal.APPEND:
                for entry in reader:
                    message: str = entry.get("MESSAGE", "")
                    if not message.startswith(ANALYTICS_PREFIX):
                        continue
                    json_str = message[len(ANALYTICS_PREFIX) :]
                    try:
                        event: dict[str, Any] = json.loads(json_str)
                    except json.JSONDecodeError:
                        log.warning(
                            "analytics_parse_error",
                            raw=json_str[:200],
                            component="journald_collector",
                        )
                        continue
                    yield event
            else:
                await asyncio.sleep(POLL_INTERVAL)

    async def close(self) -> None:
        """Close the journal reader."""
        if self._reader is not None:
            with contextlib.suppress(Exception):
                self._reader.close()
            self._reader = None
