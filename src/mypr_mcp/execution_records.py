"""Bounded JSON persistence for execution records."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .file_io import read_bytes
from .json_utils import json_bytes
from .rpc_limits import MAX_EXECUTION_RECORD_BYTES


def execution_record_bytes(record: Mapping[str, Any]) -> bytes:
    """Encode a durable execution record using its byte-level contract."""
    data = json_bytes(record, separators=(",", ":"))
    if len(data) > MAX_EXECUTION_RECORD_BYTES:
        raise ValueError(
            f"execution record exceeds {MAX_EXECUTION_RECORD_BYTES} bytes"
        )
    return data


def read_execution_record(path: Path) -> dict[str, Any]:
    """Read and validate a bounded durable execution record."""
    value = json.loads(
        read_bytes(
            Path(path),
            max_bytes=MAX_EXECUTION_RECORD_BYTES,
            follow_symlinks=False,
        ).decode("utf-8")
    )
    if not isinstance(value, dict):
        raise ValueError("persisted execution record must be an object")
    return value
