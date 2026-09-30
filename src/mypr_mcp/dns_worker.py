"""Guarded DNS resolver worker."""

from __future__ import annotations

import json
import socket
import sys


def main() -> int:
    try:
        request = json.load(sys.stdin)
        result = []
        for family, kind, proto, canonname, sockaddr in socket.getaddrinfo(
            request["host"],
            request.get("port"),
            request.get("family", socket.AF_UNSPEC),
            request.get("type", socket.SOCK_STREAM),
            request.get("proto", 0),
            0,
        ):
            result.append(
                {
                    "family": family,
                    "type": kind,
                    "proto": proto,
                    "canonical_name": canonname,
                    "sockaddr": sockaddr,
                }
            )
        response = {"ok": True, "result": result}
    except socket.gaierror as exc:
        response = {
            "ok": False,
            "kind": "gaierror",
            "errno": exc.errno,
            "error": exc.strerror or str(exc),
        }
    except Exception as exc:
        response = {"ok": False, "kind": type(exc).__name__, "error": str(exc)[:2048]}
    sys.stdout.write(json.dumps(response, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
