"""Validation and wire-format handling for native network scans."""

from __future__ import annotations

import base64
import binascii
import math
import socket
import sys
from collections.abc import Mapping
from typing import Any

_DEFAULT_MAX_PROBES = 1_000_000
_DEFAULT_MAX_DURATION = 3600.0
_DEFAULT_BANNER_TIMEOUT = 0.5
_DEFAULT_BANNER_BYTES = 1024
_DEFAULT_RESPONSE_BYTES = 1024
_MAX_PAYLOAD_BYTES = 4096
_MAX_PAYLOAD_B64 = ((_MAX_PAYLOAD_BYTES + 2) // 3) * 4

TCP_STATES = ("open", "closed", "timeout", "unreachable")
UDP_STATES = ("open", "closed", "filtered", "unreachable", "open|filtered")


def encode_payload(payload: bytes | None) -> str | None:
    """Encode an optional bounded binary payload for the JSON wire format."""

    if payload is None:
        return None
    if type(payload) is not bytes:
        raise TypeError("payload must be bytes or None")
    if len(payload) > _MAX_PAYLOAD_BYTES:
        raise ValueError("payload must be at most 4096 bytes")
    return base64.b64encode(payload).decode("ascii")


def decode_payload(payload_b64: str | None) -> bytes | None:
    """Decode a strict, bounded standard Base64 payload."""

    if payload_b64 is None:
        return None
    if type(payload_b64) is not str:
        raise TypeError("payload_b64 must be a Base64 string or None")
    if len(payload_b64) > _MAX_PAYLOAD_B64:
        raise ValueError("payload_b64 is too large")
    try:
        encoded = payload_b64.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ValueError("payload_b64 must contain ASCII Base64 data") from exc
    try:
        decoded = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("payload_b64 is not valid Base64") from exc
    if len(decoded) > _MAX_PAYLOAD_BYTES:
        raise ValueError("payload_b64 decodes to more than 4096 bytes")
    if base64.b64encode(decoded) != encoded:
        raise ValueError("payload_b64 must use canonical padded Base64")
    return decoded


def ensure_udp_available() -> None:
    """Ensure the platform exposes the error-queue APIs used by UDP scans."""

    missing = []
    if sys.platform != "linux":
        raise RuntimeError("native UDP scanning requires Linux error-queue support")
    for name in ("recvmsg", "recvmsg_into"):
        if not hasattr(socket.socket, name):
            missing.append(f"socket.socket.{name}")
    for name in ("IP_RECVERR", "IPV6_RECVERR", "MSG_ERRQUEUE", "MSG_TRUNC"):
        if not hasattr(socket, name):
            missing.append(f"socket.{name}")
    if missing:
        raise RuntimeError(
            "native UDP scanning requires Linux error-queue support; missing "
            + ", ".join(missing)
        )


def _default(value: Any, fallback: Any) -> Any:
    return fallback if value is None else value


def _positive_number(name: str, value: Any) -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a positive finite number")
    try:
        finite = math.isfinite(value)
    except OverflowError:
        finite = False
    if not finite or value <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return value


def _positive_integer(name: str, value: Any, *, maximum: int | None = None) -> int:
    if type(value) is not int:
        raise TypeError(f"{name} must be a positive integer")
    if value < 1 or maximum is not None and value > maximum:
        bound = f" between 1 and {maximum}" if maximum is not None else ""
        raise ValueError(f"{name} must be a positive integer{bound}")
    return value


def _boolean(name: str, value: Any) -> bool:
    if type(value) is not bool:
        raise TypeError(f"{name} must be a boolean")
    return value


def _mode(config: Mapping[str, Any]) -> str:
    mode = config.get("mode")
    protocol = config.get("protocol")
    if mode is None:
        mode = protocol
    if mode is None:
        mode = "tcp"
    if type(mode) is not str:
        raise TypeError("mode must be 'tcp' or 'udp'")
    if mode not in {"tcp", "udp"}:
        raise ValueError("mode must be 'tcp' or 'udp'")
    if protocol is not None:
        if type(protocol) is not str:
            raise TypeError("protocol must be 'tcp' or 'udp'")
        if protocol not in {"tcp", "udp"}:
            raise ValueError("protocol must be 'tcp' or 'udp'")
        if protocol != mode:
            raise ValueError("mode and protocol must match")
    return mode


def normalize_native_config(config: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalize a native TCP or UDP scan configuration.

    Unknown fields are retained so the manager can pass scan-specific metadata
    through this common boundary without losing it.
    """

    if not isinstance(config, Mapping):
        raise TypeError("scan config must be a mapping")
    normalized = dict(config)
    mode = _mode(config)
    normalized["mode"] = mode
    normalized["protocol"] = mode

    defaults = {
        "concurrency": 64 if mode == "tcp" else 16,
        "rate": 200 if mode == "tcp" else 20,
        "timeout": 1.0 if mode == "tcp" else 2.0,
        "retries": 0 if mode == "tcp" else 1,
        "per_host_rate": None if mode == "tcp" else 1.0,
    }
    normalized["concurrency"] = _positive_integer(
        "concurrency", _default(config.get("concurrency"), defaults["concurrency"]), maximum=1024
    )
    normalized["rate"] = _positive_number(
        "rate", _default(config.get("rate"), defaults["rate"])
    )
    normalized["timeout"] = _positive_number(
        "timeout", _default(config.get("timeout"), defaults["timeout"])
    )
    retries = _default(config.get("retries"), defaults["retries"])
    if type(retries) is not int:
        raise TypeError("retries must be an integer between 0 and 3")
    if retries < 0 or retries > 3:
        raise ValueError("retries must be between 0 and 3")
    normalized["retries"] = retries

    max_probes = config.get("max_probes", _DEFAULT_MAX_PROBES)
    if max_probes is not None:
        normalized["max_probes"] = _positive_integer("max_probes", max_probes)
    else:
        normalized["max_probes"] = None
    max_duration = config.get("max_duration", _DEFAULT_MAX_DURATION)
    if max_duration is not None:
        normalized["max_duration"] = _positive_number("max_duration", max_duration)
    else:
        normalized["max_duration"] = None

    for name in ("continue_after_output_limit", "open_only", "banner", "capture_response"):
        normalized[name] = _boolean(name, config.get(name, False))
    family = _default(config.get("family"), "any")
    if type(family) is not str:
        raise TypeError("family must be any, ipv4, or ipv6")
    if family not in {"any", "ipv4", "ipv6"}:
        raise ValueError("family must be any, ipv4, or ipv6")
    normalized["family"] = family

    normalized["banner_timeout"] = _positive_number(
        "banner_timeout", _default(config.get("banner_timeout"), _DEFAULT_BANNER_TIMEOUT)
    )
    normalized["banner_bytes"] = _positive_integer(
        "banner_bytes", _default(config.get("banner_bytes"), _DEFAULT_BANNER_BYTES), maximum=4096
    )
    normalized["response_bytes"] = _positive_integer(
        "response_bytes",
        _default(config.get("response_bytes"), _DEFAULT_RESPONSE_BYTES),
        maximum=4096,
    )

    probe = config.get("probe")
    if mode == "tcp":
        if probe is not None:
            raise ValueError("probe is only available for UDP scans")
        normalized["probe"] = None
    else:
        probe = "auto" if probe is None else probe
        if type(probe) is not str:
            raise TypeError("probe must be auto, empty, dns, or ntp")
        if probe not in {"auto", "empty", "dns", "ntp"}:
            raise ValueError("probe must be auto, empty, dns, or ntp")
        normalized["probe"] = probe

    payload_b64 = config.get("payload_b64")
    payload = decode_payload(payload_b64)
    normalized["payload_b64"] = encode_payload(payload)
    if payload is not None:
        if mode == "tcp":
            raise ValueError("payload is only available for UDP scans")
        if normalized["probe"] != "auto":
            raise ValueError("payload can only be used with the auto UDP probe")

    if mode == "tcp":
        if normalized["capture_response"]:
            raise ValueError("capture_response is only available for UDP scans")
        if normalized["response_bytes"] != _DEFAULT_RESPONSE_BYTES:
            raise ValueError("response_bytes is only available for UDP scans")
    elif normalized["banner"] or (
        normalized["banner_timeout"] != _DEFAULT_BANNER_TIMEOUT
        or normalized["banner_bytes"] != _DEFAULT_BANNER_BYTES
    ):
        raise ValueError("UDP scans do not support banner options")
    per_host_rate = config.get("per_host_rate")
    if per_host_rate is None:
        per_host_rate = defaults["per_host_rate"]
    if per_host_rate is not None:
        per_host_rate = _positive_number("per_host_rate", per_host_rate)
    normalized["per_host_rate"] = per_host_rate

    if mode == "udp":
        ensure_udp_available()
    return normalized


__all__ = [
    "TCP_STATES",
    "UDP_STATES",
    "decode_payload",
    "encode_payload",
    "ensure_udp_available",
    "normalize_native_config",
]
