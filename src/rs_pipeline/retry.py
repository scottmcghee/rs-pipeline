"""Exponential backoff for rate-limited third-party calls."""

from __future__ import annotations

import logging
import re
import time
from collections.abc import Callable

from rs_pipeline import config

logger = logging.getLogger(__name__)


_RATE_LIMIT = re.compile(r"\b429\b|too many requests|rate[ -]?limit", re.IGNORECASE)


def is_rate_limit(exc: BaseException) -> bool:
    return bool(_RATE_LIMIT.search(str(exc)))


def with_backoff[T](
    call: Callable[[], T],
    *,
    what: str,
    retry_if: Callable[[BaseException], bool] = is_rate_limit,
    retries: int = config.RATE_LIMIT_MAX_RETRIES,
    base_delay: float = config.RATE_LIMIT_BACKOFF_BASE,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Run *call*; on an error accepted by *retry_if*, wait and try again.

    Waits ``base_delay * 2**attempt`` seconds between attempts. The last
    error, or any error *retry_if* rejects, propagates.
    """
    for attempt in range(retries + 1):
        try:
            return call()
        except Exception as exc:
            if attempt == retries or not retry_if(exc):
                raise
            delay = base_delay * 2**attempt
            logger.warning(
                "%s rate-limited (attempt %d); retrying in %.0fs", what, attempt + 1, delay
            )
            sleep(delay)
    raise AssertionError("unreachable")
