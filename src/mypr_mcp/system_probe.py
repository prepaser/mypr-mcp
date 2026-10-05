"""One-shot, isolated system collector entrypoint."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


def _write(value, json_text):
    text = json_text(value, allow_nan=False)
    stream = getattr(sys.stdout, "buffer", None)
    if stream is None:
        print(text)
    else:
        stream.write(text.encode("utf-8") + b"\n")
        stream.flush()


def main():
    source = str(Path(__file__).resolve().parent.parent)
    json_text = None
    sys.path.insert(0, source)
    try:
        importlib.import_module("mypr_mcp")
    finally:
        sys.path.remove(source)
    try:
        from mypr_mcp.json_utils import json_text
        from mypr_mcp.system_tools import _bounded

        request = json.loads(sys.stdin.buffer.read(65537))
        section = request.pop("section")
        if section.startswith("gpu:"):
            from mypr_mcp.system_gpu import collect

            request["vendor"] = section.split(":", 1)[1]
            result = collect(request)
        else:
            from mypr_mcp.system_base import collect

            result = collect(section, request)
        output_limit = 768 * 1024 if section in {"sockets", "process_detail"} else 32768
        _write({"ok": True, "data": _bounded(result, output_limit)}, json_text)
    except Exception as exc:
        value = {"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:512]}"}
        if json_text is None:
            print(json.dumps(value))
        else:
            _write(value, json_text)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
