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


def make_docker_log_line(message: str) -> str:
    """Wrap a plain-text message in the Docker JSON log file format.

    Not a pytest fixture — import explicitly: from tests.conftest import make_docker_log_line
    """
    return (
        json.dumps(
            {
                "log": message + "\n",
                "stream": "stdout",
                "time": "2026-03-27T00:00:00.000000000Z",
            }
        )
        + "\n"
    )
