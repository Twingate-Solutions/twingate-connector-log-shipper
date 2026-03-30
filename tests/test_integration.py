"""End-to-end integration test: DockerCollector → Batcher → Shipper → S3."""

import asyncio
import contextlib
import gzip
import json
import socket
import threading
import urllib.error
import urllib.request
from collections.abc import Generator
from pathlib import Path
from typing import Any

import boto3
import pytest
from botocore.exceptions import ClientError

from tests.conftest import make_docker_log_line
from twingate_log_shipper.batcher import Batcher
from twingate_log_shipper.collectors.docker import DockerCollector
from twingate_log_shipper.shipper import Shipper

# ── Moto server fixture ────────────────────────────────────────────────────────


def _free_port() -> int:
    """Return a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        _, port = s.getsockname()
        return int(port)


@pytest.fixture(scope="session")
def moto_s3_url() -> Generator[str, None, None]:
    """Start a moto S3 HTTP server and return its base URL.

    Uses werkzeug (a moto dependency) to serve the moto Flask app in a
    daemon thread. This allows aiobotocore (aiohttp-based) to connect to it.
    """
    import time

    from moto.server import create_backend_app
    from werkzeug.serving import make_server

    port = _free_port()
    app = create_backend_app("s3")
    server = make_server("127.0.0.1", port, app)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    # Wait for server to be ready (up to 2 seconds)
    url = f"http://127.0.0.1:{port}"
    for _ in range(20):
        try:
            urllib.request.urlopen(url, timeout=0.1)
            break
        except urllib.error.HTTPError:
            break  # Got a response (even 4xx/5xx) = server is ready
        except Exception:
            time.sleep(0.1)

    yield url

    server.shutdown()
    thread.join(timeout=2.0)


@pytest.fixture
def s3_bucket(moto_s3_url: str) -> tuple[Any, str]:
    """Create a test bucket in the moto server and return (boto3_client, bucket_name)."""
    bucket = "integration-test-bucket"
    s3 = boto3.client(
        "s3",
        region_name="us-east-1",
        endpoint_url=moto_s3_url,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
    )
    # Create bucket (may already exist from a previous test — ignore error)
    with contextlib.suppress(ClientError):
        s3.create_bucket(Bucket=bucket)
    return s3, bucket


# ── Integration test ──────────────────────────────────────────────────────────


async def test_end_to_end_docker_to_s3(
    docker_log_dir: Path,
    docker_log_file: Path,
    analytics_event: dict[str, Any],
    s3_bucket: tuple[Any, str],
    moto_s3_url: str,
) -> None:
    """Full pipeline: Docker log file → collector → batcher → shipper → S3 object."""
    s3_client, bucket_name = s3_bucket

    event_queue: asyncio.Queue[dict[str, Any] | None] = asyncio.Queue()
    upload_queue: asyncio.Queue[tuple[Path, str] | None] = asyncio.Queue()

    collector = DockerCollector(str(docker_log_dir), "twingate")

    batcher = Batcher(
        event_queue=event_queue,
        upload_queue=upload_queue,
        s3_prefix="twingate-analytics",
        batch_interval_seconds=1,
        batch_max_events=1,  # flush after 1 event
        batch_max_bytes=10_485_760,
    )

    shipper = Shipper(
        upload_queue=upload_queue,
        s3_bucket=bucket_name,
        s3_region="us-east-1",
        s3_access_key_id="testing",
        s3_secret_access_key="testing",
        s3_endpoint_url=moto_s3_url,
        upload_max_retries=0,
        upload_backoff_base=0.01,
        upload_backoff_max=0.1,
    )

    # ── Wire up tasks ──────────────────────────────────────────────────────────
    async def _producer() -> None:
        async for event in collector.events():
            await event_queue.put(event)

    producer_task = asyncio.create_task(_producer())
    batcher_task = asyncio.create_task(batcher.run())
    shipper_task = asyncio.create_task(shipper.run())

    # ── Write one event to the Docker log file ─────────────────────────────────
    await asyncio.sleep(0.05)  # let collector open the file
    log_line = make_docker_log_line(f"ANALYTICS {json.dumps(analytics_event)}")
    docker_log_file.write_text(log_line)

    # ── Wait for the upload to complete (max 5 seconds) ───────────────────────
    # NOTE: list_objects_v2 must run in a thread so the event loop stays free
    # to process aiobotocore's aiohttp I/O. The moto server is single-threaded;
    # blocking the event loop here causes a deadlock with aiohttp keep-alive.
    async def _wait_for_s3_object() -> None:
        for _ in range(50):
            objs = (await asyncio.to_thread(s3_client.list_objects_v2, Bucket=bucket_name)).get(
                "Contents", []
            )
            if objs:
                return
            await asyncio.sleep(0.1)
        raise AssertionError("No S3 object appeared within 5 seconds")

    await asyncio.wait_for(_wait_for_s3_object(), timeout=10.0)

    # ── Graceful shutdown ──────────────────────────────────────────────────────
    producer_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await producer_task
    await collector.close()

    await event_queue.put(None)
    await asyncio.wait_for(batcher_task, timeout=3.0)

    await upload_queue.put(None)
    await asyncio.wait_for(shipper_task, timeout=3.0)

    # ── Verify S3 object ───────────────────────────────────────────────────────
    objects = (await asyncio.to_thread(s3_client.list_objects_v2, Bucket=bucket_name)).get(
        "Contents", []
    )
    assert len(objects) >= 1

    # Fetch the object and verify content
    key = objects[0]["Key"]
    assert key.startswith("twingate-analytics/")
    assert key.endswith(".ndjson.gz")

    body = (await asyncio.to_thread(s3_client.get_object, Bucket=bucket_name, Key=key))[
        "Body"
    ].read()
    ndjson = gzip.decompress(body).decode()
    lines = [line for line in ndjson.splitlines() if line]
    assert len(lines) == 1

    parsed = json.loads(lines[0])
    assert parsed["event_type"] == "closed_connection"
    assert parsed["user"]["email"] == "user@example.com"
