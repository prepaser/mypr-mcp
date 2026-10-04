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
    runtime.settings = SimpleNamespace(applying=False)
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


@pytest.mark.asyncio
@pytest.mark.parametrize("op", ["execute", "poll"])
@pytest.mark.parametrize("override", [None, 0, 10])
async def test_execution_dispatch_resolves_configured_wait(tmp_path, op, override):
    runtime = Runtime(tmp_path)
    runtime.healthy = True
    runtime.execute_wait_ms = 123
    runtime.poll_wait_ms = 456
    connection = {"client_id": "alice"}

    async def admit(*_args):
        return {"id": "a" * 32}

    async def poll(_ident, _cursor=0, wait_ms=None, **_kwargs):
        return {"wait_ms": wait_ms}

    runtime.admit_execution = admit
    runtime.poll = poll
    request = {"exec_id": "a" * 32}
    if override is not None:
        request["wait_ms"] = override
    result = await runtime._dispatch_execution(
        op, request, client="alice", connection_id="connection", connection=connection,
        requested_client=None, generation=None,
    )
    expected = getattr(runtime, f"{op}_wait_ms") if override is None else override
    assert result == {"wait_ms": expected}


@pytest.mark.asyncio
@pytest.mark.parametrize("wait_ms", [-1, 30001, True, "1000"])
async def test_execute_rejects_invalid_wait_before_submission(tmp_path, wait_ms):
    runtime = Runtime(tmp_path)
    runtime.healthy = True

    async def admit(*_args):
        pytest.fail("invalid wait must not submit Python code")

    runtime.admit_execution = admit
    with pytest.raises(ValueError, match="wait_ms"):
        await runtime._dispatch_execution(
            "execute", {"wait_ms": wait_ms}, client="alice", connection_id="connection",
            connection={"client_id": "alice"}, requested_client=None, generation=None,
        )
