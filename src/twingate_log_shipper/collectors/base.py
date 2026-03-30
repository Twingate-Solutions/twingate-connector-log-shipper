"""Abstract base class for log collectors."""

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from typing import Any


class BaseCollector(ABC):
    """Abstract async generator interface for log collectors.

    Concrete subclasses must implement ``events()`` as an async generator
    that yields one parsed analytics event dict per analytics log line.
    """

    @abstractmethod
    async def events(self) -> AsyncGenerator[dict[str, Any], None]:
        """Yield parsed analytics event dicts from the log source.

        Implementations must:
        - Filter only lines starting with ``ANALYTICS ``
        - Strip the ``ANALYTICS `` prefix (10 characters including the space)
        - Parse the remaining string as JSON
        - Yield the resulting dict
        - Silently skip non-ANALYTICS lines
        - Log WARN and skip malformed JSON lines
        - Never raise an exception that terminates iteration
        """
        # Required to make this method an async generator function.
        return
        yield

    @abstractmethod
    async def close(self) -> None:
        """Release resources held by the collector (file handles, etc.)."""
        ...
