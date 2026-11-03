import logging
import random
import time
from collections.abc import Callable
from typing import Any

import httpx

logger = logging.getLogger(__name__)

RETRYABLE = frozenset({429, 500, 502, 503, 504})


def get_json(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    attempts: int = 5,
    base_delay: float = 0.5,
    sleep: Callable[[float], None] | None = None,
) -> Any:
    """GET with retries and exponential backoff with jitter for transient failures."""
    for attempt in range(1, attempts + 1):
        try:
            response = client.get(url, params=params)
        except httpx.TransportError as error:
            reason = type(error).__name__
        else:
            if response.status_code not in RETRYABLE:
                response.raise_for_status()
                return response.json()
            reason = f"HTTP {response.status_code}"
        if attempt == attempts:
            raise RuntimeError(f"GET {url} failed after {attempts} attempts: {reason}")
        delay = random.uniform(0, base_delay * 2 ** (attempt - 1))
        logger.warning("GET %s failed with %s, retry %d in %.1fs", url, reason, attempt, delay)
        (sleep or time.sleep)(delay)
    raise AssertionError("unreachable")
