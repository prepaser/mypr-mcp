from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from jupyter_client.session import Session

from mypr_mcp.code_tools import CodeTools
from mypr_mcp.kernel_boot import _kernel_class


def _definition(command: str = "fake-lsp", timeout: float = 10.0) -> dict:
    return {"command": [command], "languages": ["python"], "timeout": timeout}


def _server(definition: dict):
    server = SimpleNamespace(
        name="fake",
        command=tuple(definition["command"]),
        languages=frozenset(definition["languages"]),
        timeout=definition["timeout"],
        process=SimpleNamespace(returncode=None),
        _failure=None,
        _operation_lock=asyncio.Lock(),
        closed=False,
    )
    server.status = lambda: {
        "name": server.name,
        "running": not server.closed,
        "timeout": server.timeout,
    }

    async def aclose():
        server.closed = True

    server.aclose = aclose
    return server


@pytest.mark.asyncio
async def test_apply_definitions_defers_busy_change_without_partial_update(tmp_path, monkeypatch):
    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(tmp_path / "global.toml"))
    code = CodeTools(tmp_path)
    old = {"fake": _definition()}
    new = {"fake": _definition(timeout=20)}
    server = _server(old["fake"])
    code._definitions = old
    code._config_revision = "old"
    code._servers["fake"] = server
    await server._operation_lock.acquire()
    try:
        result = await code.apply_definitions(new, "new")
        assert result["applied"] is False
        assert result["deferred"] == ["fake"]
        assert code._definitions == old
        assert code._config_revision == "old"
        assert code._servers["fake"] is server
    finally:
        server._operation_lock.release()
        await code.aclose()


@pytest.mark.asyncio
async def test_apply_definitions_retries_deferred_component(tmp_path, monkeypatch):
    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(tmp_path / "global.toml"))
    code = CodeTools(tmp_path)
    old = {"fake": _definition()}
    new = {"fake": _definition(timeout=20)}
    server = _server(old["fake"])
    code._definitions = old
    code._config_revision = "old"
    code._servers["fake"] = server
    await server._operation_lock.acquire()
    try:
        deferred = await code.apply_definitions(new, "new")
        assert deferred["applied"] is False
        server._operation_lock.release()
        result = await code.apply_definitions(new, "new")
        assert result["applied"] is True
        assert result["changed"] == ["fake"]
        assert code._definitions == new
        assert code._config_revision == "new"
        assert server.closed is True
        assert code._servers == {}
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_apply_definitions_force_closes_busy_component(tmp_path, monkeypatch):
    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(tmp_path / "global.toml"))
    code = CodeTools(tmp_path)
    old = {"fake": _definition()}
    new = {"fake": _definition(timeout=20)}
    server = _server(old["fake"])
    code._definitions = old
    code._servers["fake"] = server
    await server._operation_lock.acquire()
    try:
        result = await code.apply_definitions(new, "new", force=True)
        assert result["applied"] is True
        assert result["deferred"] == []
        assert server.closed is True
    finally:
        server._operation_lock.release()
        await code.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("force", [False, True])
async def test_apply_definitions_does_not_wait_for_code_lock(tmp_path, monkeypatch, force):
    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(tmp_path / "global.toml"))
    code = CodeTools(tmp_path)
    old = {"fake": _definition()}
    new = {"fake": _definition(timeout=20)}
    code._definitions = old
    code._config_revision = "old"
    await code._lock.acquire()
    try:
        result = await asyncio.wait_for(code.apply_definitions(new, "new", force=force), 0.1)
        assert result["applied"] is False
        assert result["deferred"] == ["LSP configuration is busy"]
        assert result["reason"] == "LSP configuration is busy"
        assert code._definitions == old
        assert code._config_revision == "old"
    finally:
        code._lock.release()
        await code.aclose()


@pytest.mark.asyncio
async def test_configure_and_remove_send_named_workspace_change(tmp_path, monkeypatch):
    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(tmp_path / "global.toml"))
    calls = []

    async def rpc(method, definitions=None, **fields):
        calls.append((method, definitions, fields))
        return {"servers": definitions, "revision": "next"}

    code = CodeTools(tmp_path, config_rpc=rpc)
    definition = _definition()
    server = _server(definition)
    code._definitions = {"fake": definition}
    code._config_revision = "old"
    code._servers["fake"] = server
    try:
        await code.configure("fake", definition["command"], definition["languages"], timeout=20)
        set_call = next(call for call in calls if call[0] == "set_lsp")
        assert set_call[2]["name"] == "fake"
        assert set_call[2]["definition"]["timeout"] == 20.0
        await code.remove("fake")
        remove_call = [call for call in calls if call[0] == "set_lsp"][-1]
        assert remove_call[2]["name"] == "fake"
        assert remove_call[2]["definition"] is None
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_reload_replaces_only_changed_servers(tmp_path, monkeypatch):
    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(tmp_path / "global.toml"))
    old = {"fake": _definition()}
    new = {"fake": _definition(timeout=20)}

    async def rpc(method, definitions=None, **_fields):
        if method == "applied_lsp":
            return None
        assert method == "get_lsp"
        return {"servers": new, "revision": "new"}

    code = CodeTools(tmp_path, config_rpc=rpc)
    server = _server(old["fake"])
    code._definitions = old
    code._config_revision = "old"
    code._servers["fake"] = server
    try:
        result = await code.reload()
        assert result["applied"] is True
        assert result["changed"] == ["fake"]
        assert server.closed is True
        assert code._definitions == new
    finally:
        await code.aclose()


class _Session(Session):
    def __init__(self):
        super().__init__()
        self.sent = []

    def send(self, stream, message_type, content, parent, *, ident):
        self.sent.append((stream, message_type, content, parent, ident))


@pytest.mark.asyncio
async def test_kernel_config_reload_control_applies_outside_user_executor(monkeypatch):
    monkeypatch.setenv("MYPR_GENERATION", "generation")
    applied = []

    class Code:
        _definitions = {"fake": _definition()}
        _config_revision = "composite"
        _lsp_apply_sequence = 1
        _kernel_generation = "generation"

        async def apply_definitions(self, definitions, revision, *, force=False):
            applied.append((definitions, revision, force))
            return {"applied": True, "changed": []}

    kernel_type = _kernel_class()
    kernel = kernel_type()
    kernel.session = _Session()
    kernel._mypr_workspace = SimpleNamespace(code=Code())
    parent = {
        "metadata": {
            "mypr_control": "config_reload",
            "generation": "generation",
            "config": {"lsp": {"servers": {"fake": _definition()}, "revision": "composite"}},
        }
    }
    await kernel_type.execute_request(kernel, "stream", "ident", parent)
    assert applied == [({"fake": _definition()}, "composite", False)]
    assert kernel.session.sent[0][2]["config_result"]["applied"] is True
    assert kernel.session.sent[0][2]["_mypr_applied_lsp"]["sequence"] == 1


@pytest.mark.asyncio
async def test_kernel_config_reload_rejects_expired_generation(monkeypatch):
    monkeypatch.setenv("MYPR_GENERATION", "current")
    called = False

    class Code:
        async def apply_definitions(self, *_args, **_kwargs):
            nonlocal called
            called = True

    kernel_type = _kernel_class()
    kernel = kernel_type()
    kernel.session = _Session()
    kernel._mypr_workspace = SimpleNamespace(code=Code())
    parent = {
        "metadata": {
            "mypr_control": "config_reload",
            "generation": "expired",
            "config": {"lsp": {"servers": {}, "revision": "composite"}},
        }
    }
    await kernel_type.execute_request(kernel, "stream", "ident", parent)
    assert called is False
    assert kernel.session.sent[0][2]["status"] == "error"
    assert kernel.session.sent[0][2]["ename"] == "RuntimeError"
