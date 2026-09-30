import asyncio
from types import SimpleNamespace

import pytest

from mypr_mcp.runtime import _UNHANDLED, Runtime


@pytest.mark.asyncio
async def test_history_dispatch_isolated_from_request_context():
    runtime = Runtime.__new__(Runtime)
    calls = []

    async def io(function, *args, **kwargs):
        calls.append((function, args, kwargs))
        return {"items": [], "next_cursor": None}

    runtime.io = io
    runtime.history = SimpleNamespace(list=object())

    result = await runtime._dispatch_history("history_list", {"limit": 3})

    assert result == {"items": [], "next_cursor": None}
    assert calls[0][1] == ()
    assert calls[0][2] == {"client_id": None, "limit": 3, "cursor": None}
    assert await runtime._dispatch_history("unknown", {}) is _UNHANDLED


@pytest.mark.asyncio
async def test_code_config_uses_bridge_lsp_cas_api():
    runtime = Runtime.__new__(Runtime)
    calls = []

    async def get_lsp():
        calls.append(("get",))
        return {"servers": {"python": {}}, "revision": "current"}

    async def save_lsp(definitions, expected_servers):
        calls.append(("save", definitions, expected_servers))
        return {"revision": "next"}

    runtime.mcp = SimpleNamespace(get_lsp=get_lsp, save_lsp=save_lsp)
    result = await runtime.code_config(
        {
            "method": "set_lsp",
            "definitions": {},
            "expected_revision": "stale",
            "expected_servers": {"python": {}},
        }
    )

    assert result == {"revision": "next"}
    assert calls == [
        ("save", {}, {"python": {}}),
    ]


def test_dispatch_admission_guards_are_centralized():
    runtime = Runtime.__new__(Runtime)
    runtime.restarting = "ticket"
    runtime.stopping = asyncio.Event()

    with pytest.raises(RuntimeError, match="restarting"):
        runtime._check_dispatch_admission("scan_start")

    runtime.restarting = None
    runtime.stopping.set()
    with pytest.raises(RuntimeError, match="stopping"):
        runtime._check_dispatch_admission("shell_start")
