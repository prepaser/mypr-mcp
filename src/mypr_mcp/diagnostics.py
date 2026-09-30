"""Bounded formatting helpers for errors crossing runtime boundaries."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from typing import Any


class RPCError(RuntimeError):
    """An error returned by a workspace manager or another RPC boundary."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "rpc_error",
        operation: str | None = None,
        details: Mapping[str, Any] | None = None,
        error_type: str | None = None,
    ) -> None:
        self.code = code
        self.operation = operation
        self.details = dict(details or {})
        self.error_type = error_type
        super().__init__(str(message))


def safe_error_details(exc: BaseException, limit: int = 1024) -> tuple[str, bool]:
    """Format and bound an exception, returning whether the result was truncated."""

    if limit < 1:
        raise ValueError("error limit must be positive")
    try:
        name = type(exc).__name__
    except BaseException:
        name = "Exception"
    try:
        detail = str.__str__(str(exc))
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
        text = str.__str__(value) if isinstance(value, str) else str(value)
        text = str.__str__(text)
    except BaseException:
        text = "<unprintable value>"
    raw = text.encode("utf-8", "replace")
    return raw[:limit].decode("utf-8", "ignore")


def _json_details(
    value: Any,
    *,
    depth: int = 0,
    budget: list[int],
    nodes: list[int],
) -> Any:
    """Return a small JSON-compatible copy of untrusted exception details."""

    if budget[0] <= 0 or nodes[0] <= 0:
        return "<truncated>"
    if depth > 3:
        return "<depth-limit>"
    nodes[0] -= 1
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        text = str(value)
        if len(text) > 64:
            text = safe_text(text, 64)
            budget[0] -= len(text.encode("utf-8"))
            return text
        budget[0] -= len(text)
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            budget[0] -= 4
            return None
        text = repr(value)
        budget[0] -= len(text)
        return value
    if isinstance(value, str):
        text = safe_text(value, min(1024, budget[0]))
        budget[0] -= len(text.encode("utf-8", "replace"))
        return text
    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            if budget[0] <= 0 or nodes[0] <= 0:
                break
            name = safe_text(key, 128)
            budget[0] -= len(name.encode("utf-8", "replace")) + 4
            result[name] = _json_details(item, depth=depth + 1, budget=budget, nodes=nodes)
        return result
    if isinstance(value, (list, tuple, set, frozenset)):
        result = []
        for item in value:
            if budget[0] <= 0 or nodes[0] <= 0:
                break
            result.append(_json_details(item, depth=depth + 1, budget=budget, nodes=nodes))
        return result
    text = safe_text(value, min(256, budget[0]))
    budget[0] -= len(text.encode("utf-8", "replace"))
    return text


def error_info(
    exc: BaseException,
    operation: str | None = None,
    *,
    details: Mapping[str, Any] | None = None,
    limit: int = 1024,
) -> dict[str, Any]:
    """Create a bounded, JSON-compatible structured error description.

    The legacy string representation remains available through :func:`safe_error`.
    ``error_info`` is deliberately conservative: exception text and caller details
    are bounded before they cross a manager or client boundary.
    """

    if limit < 128:
        raise ValueError("error limit must be at least 128 bytes")
    message_limit = max(1, limit - 192)
    message, message_truncated = safe_error_details(exc, message_limit)
    code = "rpc_error"
    error_type = "Exception"
    budget = [max(0, limit - len(message.encode("utf-8", "replace")) - 160)]
    nodes = [128]
    metadata_failed = False
    try:
        code = _error_code(exc)
        error_type = type(exc).__name__
        if isinstance(exc, RPCError):
            code = exc.code
            operation = operation or exc.operation
            error_type = exc.error_type or error_type
            context = exc.details
        else:
            code = getattr(exc, "code", None) or code
            context = details if details is not None else getattr(exc, "details", None)
        source_details = context if isinstance(context, Mapping) else {}
        bounded_details = _json_details(source_details, budget=budget, nodes=nodes)
        for key in ("line", "column", "path"):
            value = getattr(exc, key, None)
            if value is not None and key not in bounded_details:
                bounded_details[key] = _json_details(value, budget=budget, nodes=nodes)
    except BaseException:
        bounded_details = {}
        metadata_failed = True
    result: dict[str, Any] = {
        "code": safe_text(code, 64),
        "type": safe_text(error_type, 64),
        "message": message,
        "truncated": message_truncated or budget[0] <= 0 or metadata_failed,
    }
    if operation is not None:
        result["operation"] = safe_text(operation, 128)
    if bounded_details:
        result["details"] = bounded_details
    def encoded_size() -> int:
        return len(
            json.dumps(
                result, ensure_ascii=False, separators=(",", ":"), allow_nan=False
            ).encode()
        )

    if encoded_size() > limit:
        result.pop("details", None)
        result["truncated"] = True
    if encoded_size() > limit:
        result.pop("operation", None)
    for key in ("type", "code", "message"):
        while encoded_size() > limit and len(result[key]) > 1:
            result[key] = result[key][: max(1, len(result[key]) // 2)]
            result["truncated"] = True
    return result


def error_details(
    exc: BaseException,
    operation: str | None = None,
    *,
    details: Mapping[str, Any] | None = None,
    limit: int = 1024,
) -> dict[str, Any]:
    """Compatibility alias for :func:`error_info`."""

    return error_info(exc, operation, details=details, limit=limit)


def error_response(
    exc: BaseException,
    operation: str | None = None,
    *,
    details: Mapping[str, Any] | None = None,
    limit: int = 1024,
) -> dict[str, Any]:
    """Build the failure half of the manager wire response."""

    info = error_info(exc, operation, details=details, limit=limit)
    return {"ok": False, "error": info["message"], "error_info": info}


def _error_code(exc: BaseException) -> str:
    if isinstance(exc, TimeoutError):
        return "timeout"
    if isinstance(exc, (ValueError, TypeError)):
        return "invalid_request"
    if isinstance(exc, FileNotFoundError):
        return "not_found"
    if isinstance(exc, PermissionError):
        return "permission_denied"
    if isinstance(exc, ConnectionError):
        return "connection_error"
    return "rpc_error"
