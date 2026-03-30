"""Configuration for twingate-log-shipper.

All settings are provided via environment variables prefixed with
TWINGATE_SHIPPER_. For example:
    TWINGATE_SHIPPER_S3_BUCKET=my-bucket
    TWINGATE_SHIPPER_MODE=docker
"""

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class ShipperConfig(BaseSettings):
    """Root configuration model for the log shipper service."""

    model_config = SettingsConfigDict(env_prefix="TWINGATE_SHIPPER_")

    # ── Collector mode ────────────────────────────────────────────────────────
    mode: str = "auto"  # "auto", "docker", "journald"

    # ── Docker collector ──────────────────────────────────────────────────────
    docker_log_path: str = "/var/lib/docker/containers"
    docker_container_name_filter: str = "twingate/connector"

    # ── journald collector ────────────────────────────────────────────────────
    journald_unit: str = "twingate-connector.service"

    # ── S3 destination ────────────────────────────────────────────────────────
    s3_endpoint_url: str | None = None  # None = AWS S3; set for MinIO, R2, B2, etc.
    s3_bucket: str
    s3_prefix: str = "twingate-analytics"
    s3_region: str = "us-east-1"
    s3_access_key_id: str = Field(repr=False)
    s3_secret_access_key: str = Field(repr=False)

    # ── Batching thresholds (first threshold hit triggers flush) ──────────────
    batch_interval_seconds: int = Field(default=60, ge=10, le=3600)
    batch_max_events: int = Field(default=10_000, ge=100)
    batch_max_bytes: int = Field(default=10_485_760, ge=1_048_576)  # 10 MB default

    # ── Upload retry ──────────────────────────────────────────────────────────
    upload_max_retries: int = Field(default=3, ge=0, le=10)
    upload_backoff_base: float = Field(default=2.0, ge=1.0)
    upload_backoff_max: float = Field(default=60.0, ge=5.0)

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    shutdown_timeout_seconds: int = 30

    # ── Logging ───────────────────────────────────────────────────────────────
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
