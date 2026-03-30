"""Unit tests for the Shipper component."""

import asyncio
import gzip
from collections.abc import Generator
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from twingate_log_shipper.shipper import Shipper

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_client_error(code: str, status_code: int) -> ClientError:
    """Construct a botocore ClientError with a given error code and HTTP status."""
    return ClientError(
        error_response={
            "Error": {"Code": code, "Message": "test error"},
            "ResponseMetadata": {"HTTPStatusCode": status_code},
        },
        operation_name="PutObject",
    )


def _make_batch_file(tmp_path: Path, event_count: int = 1) -> Path:
    """Write a minimal gzipped NDJSON batch file and return its path."""
    lines = (
        "\n".join(f'{{"event_type":"closed_connection","n":{i}}}' for i in range(event_count))
        + "\n"
    )
    path = tmp_path / "batch.ndjson.gz"
    path.write_bytes(gzip.compress(lines.encode()))
    return path


def _make_shipper(
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    *,
    max_retries: int = 0,
    backoff_base: float = 0.01,
    backoff_max: float = 0.1,
) -> Shipper:
    """Construct a Shipper with test-friendly defaults (fast backoff, no real S3)."""
    return Shipper(
        upload_queue=upload_queue,
        s3_bucket="test-bucket",
        s3_region="us-east-1",
        s3_access_key_id="testing",
        s3_secret_access_key="testing",
        s3_endpoint_url=None,
        upload_max_retries=max_retries,
        upload_backoff_base=backoff_base,
        upload_backoff_max=backoff_max,
    )


@pytest.fixture
def mock_s3_client() -> AsyncMock:
    """An AsyncMock representing the aiobotocore S3 client."""
    client = AsyncMock()
    client.put_object = AsyncMock(return_value={"ResponseMetadata": {"HTTPStatusCode": 200}})
    return client


@pytest.fixture
def mock_aiobotocore(
    mock_s3_client: AsyncMock,
) -> Generator[tuple[MagicMock, AsyncMock], None, None]:
    """Patch aiobotocore.session.get_session to return a mock session.

    The mock session's create_client() returns an async context manager
    that yields mock_s3_client.

    Yields:
        tuple[MagicMock, AsyncMock]: (mock_session, mock_s3_client)
    """
    mock_ctx = AsyncMock()
    mock_ctx.__aenter__ = AsyncMock(return_value=mock_s3_client)
    mock_ctx.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.create_client = MagicMock(return_value=mock_ctx)

    with patch("aiobotocore.session.get_session") as mock_get:
        mock_get.return_value = mock_session
        yield mock_session, mock_s3_client


# ── Successful upload ─────────────────────────────────────────────────────────


async def test_successful_upload_calls_put_object(
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    mock_aiobotocore: Any,
    tmp_path: Path,
) -> None:
    """put_object is called with the correct bucket, key, and content metadata."""
    _mock_session, mock_client = mock_aiobotocore
    batch_file = _make_batch_file(tmp_path)
    s3_key = "test-prefix/2026/03/27/14-30_abc12345.ndjson.gz"

    await upload_queue.put((batch_file, s3_key))
    await upload_queue.put(None)

    shipper = _make_shipper(upload_queue)
    await asyncio.wait_for(shipper.run(), timeout=5.0)

    mock_client.put_object.assert_called_once()
    call_kwargs = mock_client.put_object.call_args.kwargs
    assert call_kwargs["Bucket"] == "test-bucket"
    assert call_kwargs["Key"] == s3_key
    assert call_kwargs["ContentEncoding"] == "gzip"
    assert call_kwargs["ContentType"] == "application/x-ndjson"


async def test_temp_file_deleted_after_successful_upload(
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    mock_aiobotocore: Any,
    tmp_path: Path,
) -> None:
    """The local batch file is deleted after a successful upload."""
    batch_file = _make_batch_file(tmp_path)
    assert batch_file.exists()

    await upload_queue.put((batch_file, "prefix/2026/03/27/14-00_abc12345.ndjson.gz"))
    await upload_queue.put(None)

    shipper = _make_shipper(upload_queue)
    await asyncio.wait_for(shipper.run(), timeout=5.0)

    assert not batch_file.exists()


# ── Retry logic ───────────────────────────────────────────────────────────────


async def test_retries_on_429_throttle(
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    mock_aiobotocore: Any,
    tmp_path: Path,
) -> None:
    """429 Throttling triggers retry; succeeds on second attempt."""
    _mock_session, mock_client = mock_aiobotocore
    throttle_err = _make_client_error("Throttling", 429)
    mock_client.put_object.side_effect = [
        throttle_err,
        {"ResponseMetadata": {"HTTPStatusCode": 200}},
    ]

    batch_file = _make_batch_file(tmp_path)
    await upload_queue.put((batch_file, "prefix/2026/03/27/14-00_abc12345.ndjson.gz"))
    await upload_queue.put(None)

    shipper = _make_shipper(upload_queue, max_retries=1)
    await asyncio.wait_for(shipper.run(), timeout=5.0)

    assert mock_client.put_object.call_count == 2
    assert not batch_file.exists()


async def test_retries_on_500_internal_error(
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    mock_aiobotocore: Any,
    tmp_path: Path,
) -> None:
    """500 InternalError triggers retry; succeeds on second attempt."""
    _mock_session, mock_client = mock_aiobotocore
    server_err = _make_client_error("InternalError", 500)
    mock_client.put_object.side_effect = [
        server_err,
        {"ResponseMetadata": {"HTTPStatusCode": 200}},
    ]

    batch_file = _make_batch_file(tmp_path)
    await upload_queue.put((batch_file, "prefix/2026/03/27/14-00_def67890.ndjson.gz"))
    await upload_queue.put(None)

    shipper = _make_shipper(upload_queue, max_retries=1)
    await asyncio.wait_for(shipper.run(), timeout=5.0)

    assert mock_client.put_object.call_count == 2
    assert not batch_file.exists()


# ── Fatal errors ──────────────────────────────────────────────────────────────


async def test_access_denied_not_retried(
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    mock_aiobotocore: Any,
    tmp_path: Path,
) -> None:
    """AccessDenied is fatal — no retry, batch discarded, temp file deleted."""
    _mock_session, mock_client = mock_aiobotocore
    mock_client.put_object.side_effect = _make_client_error("AccessDenied", 403)

    batch_file = _make_batch_file(tmp_path)
    await upload_queue.put((batch_file, "prefix/2026/03/27/14-00_abc12345.ndjson.gz"))
    await upload_queue.put(None)

    shipper = _make_shipper(upload_queue, max_retries=3)
    await asyncio.wait_for(shipper.run(), timeout=5.0)

    assert mock_client.put_object.call_count == 1  # no retry
    assert not batch_file.exists()


async def test_no_such_bucket_not_retried(
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    mock_aiobotocore: Any,
    tmp_path: Path,
) -> None:
    """NoSuchBucket is fatal — no retry, batch discarded, temp file deleted."""
    _mock_session, mock_client = mock_aiobotocore
    mock_client.put_object.side_effect = _make_client_error("NoSuchBucket", 404)

    batch_file = _make_batch_file(tmp_path)
    await upload_queue.put((batch_file, "prefix/2026/03/27/14-00_abc12345.ndjson.gz"))
    await upload_queue.put(None)

    shipper = _make_shipper(upload_queue, max_retries=3)
    await asyncio.wait_for(shipper.run(), timeout=5.0)

    assert mock_client.put_object.call_count == 1
    assert not batch_file.exists()


async def test_all_retries_exhausted_discards_batch(
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    mock_aiobotocore: Any,
    tmp_path: Path,
) -> None:
    """When all retries are exhausted, batch is discarded and temp file is deleted."""
    _mock_session, mock_client = mock_aiobotocore
    mock_client.put_object.side_effect = _make_client_error("ServiceUnavailable", 503)

    batch_file = _make_batch_file(tmp_path)
    await upload_queue.put((batch_file, "prefix/2026/03/27/14-00_abc12345.ndjson.gz"))
    await upload_queue.put(None)

    shipper = _make_shipper(upload_queue, max_retries=2)
    await asyncio.wait_for(shipper.run(), timeout=5.0)

    assert mock_client.put_object.call_count == 3  # 1 initial + 2 retries
    assert not batch_file.exists()


async def test_temp_file_deleted_after_all_retries_exhausted(
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    mock_aiobotocore: Any,
    tmp_path: Path,
) -> None:
    """Temp file is always cleaned up even when upload fails permanently."""
    _mock_session, mock_client = mock_aiobotocore
    mock_client.put_object.side_effect = _make_client_error("InternalError", 500)

    batch_file = _make_batch_file(tmp_path)
    assert batch_file.exists()

    await upload_queue.put((batch_file, "prefix/2026/03/27/14-00_abc12345.ndjson.gz"))
    await upload_queue.put(None)

    shipper = _make_shipper(upload_queue, max_retries=1)
    await asyncio.wait_for(shipper.run(), timeout=5.0)

    assert not batch_file.exists()


# ── Shutdown sentinel ─────────────────────────────────────────────────────────


async def test_none_sentinel_stops_shipper(
    upload_queue: asyncio.Queue[tuple[Path, str] | None],
    mock_aiobotocore: tuple[MagicMock, AsyncMock],
) -> None:
    """None sentinel on the queue causes shipper.run() to return cleanly."""
    await upload_queue.put(None)

    shipper = _make_shipper(upload_queue)
    # Should complete without error or timeout
    await asyncio.wait_for(shipper.run(), timeout=5.0)
