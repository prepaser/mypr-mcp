"""Bounded formatting helpers for errors crossing runtime boundaries."""

from __future__ import annotations

from typing import Any


def safe_error_details(exc: BaseException, limit: int = 1024) -> tuple[str, bool]:
    """Format and bound an exception, returning whether the result was truncated."""

    if limit < 1:
        raise ValueError("error limit must be positive")
    try:
        name = type(exc).__name__
    except BaseException:
        name = "Exception"
    try:
        detail = str(exc)
    except BaseException:
        detail = "<unprintable exception>"
    value = f"{name}: {detail}"
    raw = value.encode("utf-8", "replace")
    return raw[:limit].decode("utf-8", "ignore"), len(raw) > limit


def safe_error(exc: BaseException, limit: int = 1024) -> str:
    """Format an exception without allowing its representation to escape."""

    return safe_error_details(exc, limit)[0]


def safe_text(value: Any, limit: int) -> str:
    """Bound arbitrary text by UTF-8 bytes while preserving valid characters."""

    if limit < 1:
        raise ValueError("text limit must be positive")
    try:
        text = value if isinstance(value, str) else str(value)
    except BaseException:
        text = "<unprintable value>"
    raw = text.encode("utf-8", "replace")
    return raw[:limit].decode("utf-8", "ignore")
