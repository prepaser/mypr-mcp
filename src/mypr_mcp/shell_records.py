"""Bounded reads and confirmed terminal fields for persisted shell jobs."""

import json
import math
from pathlib import Path
from typing import Any

from .file_io import read_bytes

_MAX_WARNING_TEXT = 256


def load_metadata(path: Path) -> tuple[dict[str, Any], dict[str, str] | None]:
    try:
        metadata = json.loads(read_bytes(path, max_bytes=1024 * 1024))
    except (TypeError, ValueError, UnicodeError) as exc:
        return {}, {
            "code": "shell_metadata_corrupt",
            "text": f"Persisted shell metadata is invalid and was omitted: {exc}"[
                :_MAX_WARNING_TEXT
            ],
        }
    if not isinstance(metadata, dict):
        return {}, {
            "code": "shell_metadata_corrupt",
            "text": "Persisted shell metadata is not an object and was omitted.",
        }
    return metadata, None


def terminal_fields(ident: str, metadata: dict[str, Any]) -> dict[str, Any] | None:
    state = metadata.get("state")
    result = metadata.get("result")
    error = metadata.get("error")
    finished = metadata.get("finished_at")
    if (
        metadata.get("id") != ident
        or not isinstance(state, str)
        or state not in {"succeeded", "failed", "cancelled"}
        or not isinstance(result, dict)
        or type(result.get("returncode")) is not int
        or (state == "succeeded" and result["returncode"] != 0)
        or (state == "succeeded" and error is not None)
        or (state == "failed" and result["returncode"] == 0 and error is None)
        or (error is not None and not isinstance(error, str))
        or isinstance(finished, bool)
        or not isinstance(finished, (int, float))
    ):
        return None
    try:
        finished = float(finished)
    except OverflowError:
        return None
    if not math.isfinite(finished):
        return None
    return {"state": state, "result": result, "error": error, "finished": finished}


def recovery_warnings(
    record: dict[str, Any], metadata: dict[str, Any]
) -> tuple[list[dict[str, str]], bool]:
    warnings = [{
        "code": "history_output_incomplete",
        "text": "Shell completion was recovered; History output publication may be incomplete.",
    }]
    for source in (record, metadata):
        values = source.get("warnings", [])
        if not isinstance(values, list):
            continue
        for value in values:
            if not isinstance(value, dict):
                continue
            code, text = value.get("code"), value.get("text")
            if not isinstance(code, str) or not isinstance(text, str):
                continue
            warning = {"code": code[:_MAX_WARNING_TEXT], "text": text[:_MAX_WARNING_TEXT]}
            if warning not in warnings:
                if len(warnings) == 4:
                    return warnings, True
                warnings.append(warning)
    return warnings, (
        bool(record.get("warnings_truncated"))
        or bool(metadata.get("warnings_truncated"))
    )
