"""S3-compatible batch uploader with exponential backoff and retry."""

import asyncio
import contextlib
import random
import time
from pathlib import Path

import structlog
from botocore.config import Config
from botocore.exceptions import ClientError

log = structlog.get_logger(__name__)

# HTTP status codes that warrant retry
_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# S3 error codes that are never retryable
_FATAL_ERROR_CODES = {"AccessDenied", "NoSuchBucket", "InvalidAccessKeyId"}


class Shipper:
    """Uploads gzipped NDJSON batch files to an S3-compatible endpoint.

    Consumes ``(local_path, s3_key)`` tuples from ``upload_queue``.
    A ``None`` sentinel on the queue signals graceful shutdown.
    """

    def __init__(
        self,
        upload_queue: asyncio.Queue[tuple[Path, str] | None],
        s3_bucket: str,
        s3_region: str,
        s3_access_key_id: str,
        s3_secret_access_key: str,
        s3_endpoint_url: str | None,
        upload_max_retries: int,
        upload_backoff_base: float,
        upload_backoff_max: float,
    ) -> None:
        """Initialise the shipper.

        Args:
            upload_queue: Source queue of ``(local_path, s3_key)`` tuples.
                          ``None`` signals shutdown.
            s3_bucket: Target S3 bucket name.
            s3_region: AWS region (used even for non-AWS S3 providers).
            s3_access_key_id: S3 access key ID.
            s3_secret_access_key: S3 secret access key.
            s3_endpoint_url: Custom endpoint URL (e.g. MinIO, Cloudflare R2).
                             ``None`` means standard AWS S3.
            upload_max_retries: Maximum retry attempts per upload (0 = no retry).
            upload_backoff_base: Exponential backoff base in seconds.
            upload_backoff_max: Maximum backoff duration in seconds.
        """
        self._upload_queue = upload_queue
        self._bucket = s3_bucket
        self._region = s3_region
        self._access_key_id = s3_access_key_id
        self._secret_access_key = s3_secret_access_key
        self._endpoint_url = s3_endpoint_url
        self._max_retries = upload_max_retries
        self._backoff_base = upload_backoff_base
        self._backoff_max = upload_backoff_max

    async def run(self) -> None:
        """Consume upload queue until None sentinel received."""
        import aiobotocore.session

        session = aiobotocore.session.get_session()
        boto_config = Config(
            connect_timeout=10,
            read_timeout=30,
            retries={"max_attempts": 1},  # we manage retries ourselves
        )

        async with session.create_client(
            "s3",
            region_name=self._region,
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key_id,
            aws_secret_access_key=self._secret_access_key,
            config=boto_config,
        ) as client:
            while True:
                item = await self._upload_queue.get()
                if item is None:
                    break
                local_path, s3_key = item
                await self._upload_with_retry(client, local_path, s3_key)

    async def _upload_with_retry(
        self,
        client: object,
        local_path: Path,
        s3_key: str,
    ) -> None:
        """Attempt upload with exponential backoff. Always deletes the temp file."""
        try:
            file_bytes = await asyncio.to_thread(local_path.read_bytes)
            file_size = len(file_bytes)
        except OSError as exc:
            log.error(
                "batch_file_read_error",
                path=str(local_path),
                error=str(exc),
                component="shipper",
            )
            return

        last_exc: Exception | None = None
        succeeded = False

        for attempt in range(self._max_retries + 1):
            try:
                t0 = time.monotonic()
                await client.put_object(  # type: ignore[attr-defined]
                    Bucket=self._bucket,
                    Key=s3_key,
                    Body=file_bytes,
                    ContentEncoding="gzip",
                    ContentType="application/x-ndjson",
                )
                elapsed = time.monotonic() - t0
                log.info(
                    "batch_uploaded",
                    s3_key=s3_key,
                    file_size=file_size,
                    attempt=attempt,
                    elapsed_seconds=round(elapsed, 3),
                    component="shipper",
                )
                succeeded = True
                break  # upload succeeded

            except ClientError as exc:
                error_code: str = exc.response["Error"]["Code"]
                status_code: int = exc.response["ResponseMetadata"]["HTTPStatusCode"]

                if error_code in _FATAL_ERROR_CODES:
                    log.error(
                        "batch_upload_fatal",
                        s3_key=s3_key,
                        error_code=error_code,
                        status_code=status_code,
                        component="shipper",
                    )
                    last_exc = exc
                    break  # non-retryable

                if status_code not in _RETRYABLE_STATUS_CODES:
                    log.error(
                        "batch_upload_non_retryable",
                        s3_key=s3_key,
                        error_code=error_code,
                        status_code=status_code,
                        component="shipper",
                    )
                    last_exc = exc
                    break

                last_exc = exc
                if attempt < self._max_retries:
                    backoff = min(
                        self._backoff_base**attempt + random.uniform(0, 1),
                        self._backoff_max,
                    )
                    log.warning(
                        "batch_upload_retry",
                        s3_key=s3_key,
                        attempt=attempt,
                        next_attempt_in=round(backoff, 2),
                        error_code=error_code,
                        component="shipper",
                    )
                    await asyncio.sleep(backoff)

            except Exception as exc:
                last_exc = exc
                if attempt < self._max_retries:
                    backoff = min(
                        self._backoff_base**attempt + random.uniform(0, 1),
                        self._backoff_max,
                    )
                    log.warning(
                        "batch_upload_network_error",
                        s3_key=s3_key,
                        attempt=attempt,
                        next_attempt_in=round(backoff, 2),
                        error=str(exc),
                        component="shipper",
                    )
                    await asyncio.sleep(backoff)

        if not succeeded:
            log.error(
                "batch_upload_failed_discard",
                s3_key=s3_key,
                file_size=file_size,
                max_retries=self._max_retries,
                error=str(last_exc),
                component="shipper",
            )

        # Always clean up temp file regardless of success or failure
        with contextlib.suppress(OSError):
            await asyncio.to_thread(local_path.unlink)
