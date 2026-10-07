"""UTF-8 JSON that preserves filesystem surrogate escapes."""

import hashlib
import json
from typing import Any

SOURCE_HASH_ENCODING = "utf-8-surrogatepass"

def json_bytes(value: Any, **kwargs: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, **kwargs).encode("utf-8", "backslashreplace")


def json_text(value: Any, **kwargs: Any) -> str:
    return json_bytes(value, **kwargs).decode("utf-8")


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", "backslashreplace")).hexdigest()


def source_sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8", "surrogatepass")).hexdigest()
