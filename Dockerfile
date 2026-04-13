# Pinned to digest for supply-chain reproducibility.
# To update: docker pull python:3.12-slim && docker inspect python:3.12-slim --format '{{index .RepoDigests 0}}'
FROM python:3.12-slim@sha256:804ddf3251a60bbf9c92e73b7566c40428d54d0e79d3428194edf40da6521286

WORKDIR /app

# Create non-root user before installing anything
RUN groupadd -r -g 1000 shipper \
 && useradd -r -u 1000 -g shipper -s /usr/sbin/nologin shipper

# Install runtime dependencies (layer cached separately from source)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source and install package (no dev extras, no editable mode)
COPY pyproject.toml .
COPY src/ ./src/
RUN pip install --no-cache-dir --no-deps .

# Runtime environment
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Drop to non-root
USER shipper

ENTRYPOINT ["twingate-log-shipper"]
