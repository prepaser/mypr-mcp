"""One-shot, isolated system collector entrypoint."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path


def main():
    source = str(Path(__file__).resolve().parent.parent)
    sys.path.insert(0, source)
    try:
        importlib.import_module("mypr_mcp")
    finally:
        sys.path.remove(source)
    try:
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
        print(json.dumps({"ok": True, "data": _bounded(result)}, allow_nan=False))
    except Exception as exc:
        print(json.dumps({"ok": False, "error": f"{type(exc).__name__}: {str(exc)[:512]}"}))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
