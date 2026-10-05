"""UTF-8 JSON that preserves filesystem surrogate escapes."""

import json
from typing import Any


def json_bytes(value: Any, **kwargs: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, **kwargs).encode("utf-8", "backslashreplace")


def json_text(value: Any, **kwargs: Any) -> str:
    return json_bytes(value, **kwargs).decode("utf-8")
