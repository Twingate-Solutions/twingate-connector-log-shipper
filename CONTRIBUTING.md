# Contributing to twingate-log-shipper

This is an experimental, unsupported project published under the Twingate Solutions GitHub org. Pull requests and bug reports are welcome. Please keep in mind that response times may vary and there are no SLAs.

---

## Development Setup

**Requirements:** Python 3.12+ and a Unix-like environment (Linux or macOS). Windows is not supported for development due to the `asyncio` subprocess usage and optional `systemd-python` dependency.

```bash
# Clone the repo
git clone https://github.com/twingate-solutions/twingate-connector-log-shipper.git
cd twingate-connector-log-shipper

# Create a virtual environment using Python 3.12
python3.12 -m venv .venv

# Activate the virtual environment
source .venv/bin/activate

# Install the package with all dev dependencies
pip install -e ".[dev]"
```

> **Note:** The `journald` collector depends on `systemd-python`, which is Linux-only and requires `libsystemd-dev` to be present at install time. This dependency is intentionally excluded from the `dev` extras. If you need to work on the journald collector, install it separately:
>
> ```bash
> pip install -e ".[dev,journald]"
> ```

---

## Running Tests

Run the full test suite:

```bash
pytest tests/ -v
```

Run unit tests only (skip integration tests):

```bash
pytest tests/ -v -k "not integration"
```

Run a specific test file:

```bash
pytest tests/test_batcher.py -v
```

**Integration tests** (`test_integration.py`) spin up a local HTTP server using moto's S3 server mode. This requires `moto[s3,server]` and `werkzeug`, both of which are included in the `dev` extras. The server starts and stops automatically as part of the test session — no external services are needed.

---

## Linting and Type Checking

All three of the following must pass before opening a PR:

```bash
# Check for lint errors
ruff check src/ tests/

# Check (and apply) formatting
ruff format src/ tests/

# Run static type checking
mypy src/
```

To check formatting without making changes:

```bash
ruff format --check src/ tests/
```

---

## Code Style

- **Python 3.12+** — use modern type hints (`str | None`, not `Optional[str]`), `match` statements where appropriate
- **Async throughout** — prefer `async def` and `asyncio`; avoid blocking I/O on the event loop
- **Pydantic v2** for configuration models
- **structlog** for all logging — never use `print()` for diagnostic output
- **Type hints on every function signature** — no bare `def foo(x):`
- **Docstrings on every public class and method**
- Code must pass `ruff check`, `ruff format --check`, and `mypy src/` without errors or warnings

---

## Adding a New Collector

To add support for a new log source (e.g., a different container runtime or log aggregator):

1. Create a new file in `src/twingate_log_shipper/collectors/`, e.g. `myruntime.py`.

2. Subclass `BaseCollector` from `collectors/base.py` and implement the required interface:

   ```python
   from twingate_log_shipper.collectors.base import BaseCollector

   class MyRuntimeCollector(BaseCollector):
       """Collector for MyRuntime log source."""

       async def events(self) -> AsyncGenerator[dict, None]:
           """Yield parsed ANALYTICS events as dicts."""
           ...

       async def close(self) -> None:
           """Clean up any open handles or connections."""
           ...
   ```

3. Filter for lines starting with `ANALYTICS `, strip the prefix, and parse the remaining text as JSON before yielding. Malformed lines should be logged at `WARN` level and skipped — never raise.

4. Register the new collector in `_create_collector()` in `src/twingate_log_shipper/main.py`, adding a new branch for the collector mode name.

5. Add unit tests in `tests/test_collectors.py` covering at minimum: correct filtering of ANALYTICS lines, correct skipping of non-ANALYTICS lines, and handling of malformed JSON.

---

## Submitting a Pull Request

1. Fork the repository and create a feature branch from `main`:

   ```bash
   git checkout -b feature/my-change
   ```

2. Write tests first where practical. All new behaviour should have accompanying tests.

3. Ensure the full check suite passes before opening a PR:

   ```bash
   ruff check src/ tests/
   ruff format --check src/ tests/
   mypy src/
   pytest tests/ -v
   ```

4. Open a pull request against the `main` branch with a clear description of what the change does and why.

For bug reports, please include: the deployment mode (Docker sidecar, Docker host, or systemd), Python version, relevant log output, and steps to reproduce.
