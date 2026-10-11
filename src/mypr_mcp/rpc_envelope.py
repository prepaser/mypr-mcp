"""JSON object envelopes shared by manager RPC clients."""

from __future__ import annotations

import json
from typing import Any

from .diagnostics import RPCError


def decode_object(data: bytes, message: str) -> dict[str, Any]:
    try:
        response = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RPCError(message, code="protocol_error") from exc
    if not isinstance(response, dict):
        raise RPCError(message, code="protocol_error")
    return response
