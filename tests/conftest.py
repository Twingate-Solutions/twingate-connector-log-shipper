"""Shared pytest fixtures for twingate-log-shipper tests."""

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def docker_log_dir(tmp_path: Path) -> Path:
    """Create a fake /var/lib/docker/containers directory with one twingate container."""
    container_id = "abc123def456" + "0" * 52  # 64-char hex ID
    container_dir = tmp_path / container_id
    container_dir.mkdir()
    (container_dir / "config.v2.json").write_text(json.dumps({"Name": "/twingate-connector"}))
    (container_dir / f"{container_id}-json.log").touch()
    return tmp_path


@pytest.fixture
def docker_log_file(docker_log_dir: Path) -> Path:
    """Return the log file path for the fake Docker container."""
    container_id = next(d.name for d in docker_log_dir.iterdir() if d.is_dir())
    return docker_log_dir / container_id / f"{container_id}-json.log"


@pytest.fixture
def analytics_event() -> dict[str, Any]:
    """Minimal valid analytics event dict matching the Twingate connector format."""
    return {
        "event_type": "closed_connection",
        "timestamp": 1698356150045,
        "user": {"email": "user@example.com", "id": "113256"},
        "resource": {"address": "app.example.com", "id": "2255492"},
        "connection": {
            "id": "e755ba99",
            "client_ip": "192.0.2.1",
            "resource_ip": "10.0.0.1",
            "resource_port": 443,
            "protocol": "tcp",
            "rx": 1000,
            "tx": 500,
            "duration": 3000,
            "tunnel_path": "direct",
            "tunnel_proto": "quic/udp",
        },
        "device": {"id": "200903"},
        "connector": {"name": "test-connector", "id": "84014"},
        "remote_network": {"name": "Test Network", "id": "6938"},
        "location": '{"geoip":{"city":"Seattle","country":"US","lat":47.6062,"lon":-122.3321,"region":"WA"}}',
        "relays": [],
    }


@pytest.fixture
def event_queue() -> asyncio.Queue[dict[str, Any] | None]:
    """Empty asyncio Queue for events (collector → batcher)."""
    return asyncio.Queue()


@pytest.fixture
def upload_queue() -> asyncio.Queue[tuple[Path, str] | None]:
    """Empty asyncio Queue for upload jobs (batcher → shipper)."""
    return asyncio.Queue()


def make_docker_log_line(message: str, stream: str = "stdout") -> str:
    """Wrap a plain-text message in the Docker JSON log file format.

    Not a pytest fixture — import explicitly: from tests.conftest import make_docker_log_line
    """
    return (
        json.dumps(
            {
                "log": message + "\n",
                "stream": stream,
                "time": "2026-03-27T00:00:00.000000000Z",
            }
        )
        + "\n"
    )


def make_docker_chunked_lines(message: str, chunk_size: int = 16384) -> str:
    """Emit a single container stdout line as multiple Docker json-file records.

    Mimics Docker's json-file driver splitting any line longer than ``chunk_size``
    (~16 KB in production) into several log records: every record carries a slice of
    the line in its ``log`` field, and only the **last** record's ``log`` ends with a
    newline. ``message`` is the logical line content WITHOUT a trailing newline.

    Not a pytest fixture — import explicitly.
    """
    parts = [message[i : i + chunk_size] for i in range(0, len(message), chunk_size)] or [""]
    records = []
    for idx, part in enumerate(parts):
        is_last = idx == len(parts) - 1
        records.append(
            json.dumps(
                {
                    "log": part + ("\n" if is_last else ""),
                    "stream": "stdout",
                    "time": "2026-03-27T00:00:00.000000000Z",
                }
            )
            + "\n"
        )
    return "".join(records)


def make_container(base: Path, container_id: str, name: str, created: str | None = None) -> Path:
    """Create a fake Docker container dir under ``base`` and return its json.log path.

    Writes a ``config.v2.json`` (with ``Name`` and optional ``Created`` ISO timestamp)
    and an empty ``<id>-json.log``. Not a pytest fixture — import explicitly.
    """
    container_dir = base / container_id
    container_dir.mkdir()
    config: dict[str, Any] = {"Name": name}
    if created is not None:
        config["Created"] = created
    (container_dir / "config.v2.json").write_text(json.dumps(config))
    log_file = container_dir / f"{container_id}-json.log"
    log_file.touch()
    return log_file
