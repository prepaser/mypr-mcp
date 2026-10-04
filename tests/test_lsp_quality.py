from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from mypr_mcp import change_plans
from mypr_mcp import code_tools as code_tools_module
from mypr_mcp.code_tools import CodeTools
from mypr_mcp.lsp_edits import EditError, EditPlanStore, PlannedOperation, sha256


def _running_server(command: tuple[str, ...]):
    server = SimpleNamespace(
        command=command,
        languages=frozenset({"python"}),
        process=SimpleNamespace(returncode=None),
        _failure=None,
        timeout=10.0,
        _operation_lock=asyncio.Lock(),
    )
    server.status = lambda: {"name": "fake", "timeout": server.timeout}

    async def aclose():
        server.closed = True
        return None

    server.closed = False
    server.aclose = aclose
    return server


def _plan(store: EditPlanStore, root: Path, marker: str):
    path = root / f"{marker}.py"
    return store.create(
        root,
        [PlannedOperation("update", path, b"old", marker.encode(), "old")],
        "generation",
        marker,
        server="fake",
    )


@pytest.mark.asyncio
async def test_reused_lsp_cas_failure_keeps_active_timeout_and_definitions(tmp_path):
    command = ("fake-lsp",)

    async def config_rpc(method, definitions, **_options):
        assert method == "set_lsp"
        assert definitions["fake"]["timeout"] == 20.0
        raise RuntimeError("CAS failed")

    code = CodeTools(tmp_path, config_rpc=config_rpc)
    server = _running_server(command)
    code._servers["fake"] = server
    code._definitions = {
        "fake": {"command": list(command), "languages": ["python"], "timeout": 10.0}
    }
    code._config_revision = "old"
    try:
        with pytest.raises(RuntimeError, match="CAS failed"):
            await code.configure("fake", command, ["python"], timeout=20)
        assert server.timeout == 10.0
        assert code._definitions["fake"]["timeout"] == 10.0
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_aclose_finishes_cleanup_when_cancelled_while_waiting_for_lock(tmp_path):
    code = CodeTools(tmp_path)
    server = _running_server(("fake-lsp",))
    code._servers["fake"] = server
    await code._lock.acquire()
    task = asyncio.create_task(code.aclose())
    await asyncio.sleep(0)
    task.cancel()
    code._lock.release()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert server.closed is True
    assert code._servers == {}
    await code.aclose()


@pytest.mark.asyncio
async def test_close_waits_for_all_servers_before_reporting_failures(tmp_path):
    code = CodeTools(tmp_path)
    failed = _running_server(("failed-lsp",))
    gated = _running_server(("gated-lsp",))
    started = asyncio.Event()
    release = asyncio.Event()

    async def fail():
        raise RuntimeError("failed to close")

    async def wait_for_release():
        started.set()
        await release.wait()
        gated.closed = True

    failed.aclose = fail
    gated.aclose = wait_for_release
    code._servers.update(failed=failed, gated=gated)
    task = asyncio.create_task(code.close())
    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.sleep(0)
    assert not task.done()
    release.set()
    with pytest.raises(BaseExceptionGroup, match="language server cleanup failed"):
        await task
    assert gated.closed is True
    assert code._servers == {}


@pytest.mark.asyncio
async def test_reload_serializes_config_snapshot_with_configure(tmp_path):
    started = asyncio.Event()
    release = asyncio.Event()
    command = ("fake-lsp",)
    old_definition = {
        "fake": {"command": list(command), "languages": ["python"], "timeout": 10.0}
    }

    async def config_rpc(method, definitions=None, **_options):
        if method == "get_lsp":
            started.set()
            await release.wait()
            return {"servers": old_definition, "revision": "old"}
        if method == "applied_lsp":
            return None
        assert method == "set_lsp"
        return {"servers": definitions, "revision": "new"}

    code = CodeTools(tmp_path, config_rpc=config_rpc)
    server = _running_server(command)
    code._servers["fake"] = server
    code._definitions = old_definition
    code._config_revision = "old"
    try:
        reload_task = asyncio.create_task(code.reload())
        await asyncio.wait_for(started.wait(), timeout=1)
        configure_task = asyncio.create_task(
            code.configure("fake", command, ["python"], timeout=20)
        )
        await asyncio.sleep(0)
        assert not configure_task.done()
        release.set()
        await reload_task
        await configure_task
        assert code._definitions["fake"]["timeout"] == 20.0
        assert server.timeout == 20.0
    finally:
        release.set()
        await code.aclose()


@pytest.mark.asyncio
async def test_action_cache_is_bounded_and_generation_aware(tmp_path, monkeypatch):
    monkeypatch.setattr(code_tools_module, "MAX_ACTIONS", 1)
    code = CodeTools(tmp_path)
    server = _running_server(("fake-lsp",))
    path = tmp_path / "sample.py"
    path.write_text("foo\n", encoding="utf-8")
    document = SimpleNamespace(path=path, uri=path.as_uri(), version=1, text="foo\n")
    server.documents = {}
    server.name = "fake"
    server.generation = "generation"
    server.documents[document.uri] = document

    async def code_actions(*_args, **_kwargs):
        return document, [{"title": "Replace", "edit": {"changes": {}}}]

    server.code_actions = code_actions
    code._servers["fake"] = server
    try:
        first = (await code.actions("fake", path, 1, 1))["actions"][0]["action_id"]
        second = (await code.actions("fake", path, 1, 1))["actions"][0]["action_id"]
        assert first not in code._actions
        assert second in code._actions
        server.generation = "new-generation"
        with pytest.raises(EditError, match="unknown or expired"):
            await code.prepare_action(second)
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_reused_lsp_cancel_while_operation_is_locked_does_not_persist(tmp_path):
    command = ("fake-lsp",)
    entered = asyncio.Event()

    async def config_rpc(*_args, **_options):
        entered.set()
        return {"revision": "new"}

    code = CodeTools(tmp_path, config_rpc=config_rpc)
    server = _running_server(command)
    code._servers["fake"] = server
    code._definitions = {
        "fake": {"command": list(command), "languages": ["python"], "timeout": 10.0}
    }
    code._config_revision = "old"
    await server._operation_lock.acquire()
    try:
        task = asyncio.create_task(code.configure("fake", command, ["python"], timeout=20))
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not entered.is_set()
        assert server.timeout == 10.0
        assert code._definitions["fake"]["timeout"] == 10.0
    finally:
        server._operation_lock.release()
        await code.aclose()


@pytest.mark.asyncio
async def test_reused_lsp_cancel_after_persist_commits_runtime_timeout(tmp_path):
    command = ("fake-lsp",)
    committed = asyncio.Event()
    release = asyncio.Event()

    async def config_rpc(method, _definitions, **_options):
        committed.set()
        await release.wait()
        if method == "applied_lsp":
            return None
        return {"servers": _definitions, "revision": "new"}

    code = CodeTools(tmp_path, config_rpc=config_rpc)
    server = _running_server(command)
    code._servers["fake"] = server
    code._definitions = {
        "fake": {"command": list(command), "languages": ["python"], "timeout": 10.0}
    }
    code._config_revision = "old"
    try:
        task = asyncio.create_task(code.configure("fake", command, ["python"], timeout=20))
        await asyncio.wait_for(committed.wait(), timeout=1)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert server.timeout == 20.0
        assert code._definitions["fake"]["timeout"] == 20.0
    finally:
        release.set()
        await code.aclose()


@pytest.mark.asyncio
async def test_remove_lsp_cancellation_finishes_persistence_and_cleanup(tmp_path):
    command = ("fake-lsp",)
    definitions = {
        "fake": {"command": list(command), "languages": ["python"], "timeout": 10.0}
    }
    durable = definitions.copy()
    entered = asyncio.Event()
    release = asyncio.Event()

    async def config_rpc(method, candidate=None, **_options):
        if method == "set_lsp":
            durable.clear()
            durable.update(candidate)
            entered.set()
            await release.wait()
            return {"servers": dict(durable), "revision": "new"}
        assert method == "applied_lsp"

    code = CodeTools(tmp_path, config_rpc=config_rpc)
    server = _running_server(command)
    code._servers["fake"] = server
    code._definitions = definitions.copy()
    code._config_revision = "old"
    task = asyncio.create_task(code.remove("fake"))
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert durable == {}
        assert code._definitions == {}
        assert code._config_revision == "new"
        assert code._servers == {}
        assert server.closed is True
    finally:
        release.set()
        await code.aclose()


@pytest.mark.asyncio
async def test_replacement_lsp_cancel_after_persist_publishes_and_closes_old(
    tmp_path, monkeypatch
):
    class ReplacementServer:
        instances = []

        def __init__(self, root, name, command, languages, timeout):
            self.root = root
            self.name = name
            self.command = command
            self.languages = languages
            self.timeout = timeout
            self.process = SimpleNamespace(returncode=None)
            self._failure = None
            self._operation_lock = asyncio.Lock()
            self.generation = "replacement"
            self.closed = False
            self.instances.append(self)

        async def start(self):
            return None

        def status(self):
            return {"name": self.name, "timeout": self.timeout}

        async def aclose(self):
            self.closed = True

    monkeypatch.setattr(code_tools_module, "_LanguageServer", ReplacementServer)
    committed = asyncio.Event()
    release = asyncio.Event()
    old_command = ("old-lsp",)
    new_command = ("new-lsp",)

    async def config_rpc(method, definitions, **_options):
        assert definitions["fake"]["command"] == list(new_command)
        committed.set()
        await release.wait()
        if method == "applied_lsp":
            return None
        return {"servers": definitions, "revision": "new"}

    code = CodeTools(tmp_path, config_rpc=config_rpc)
    old = _running_server(old_command)
    code._servers["fake"] = old
    code._definitions = {
        "fake": {"command": list(old_command), "languages": ["python"], "timeout": 10.0}
    }
    code._config_revision = "old"
    try:
        task = asyncio.create_task(code.configure("fake", new_command, ["python"], timeout=20))
        await asyncio.wait_for(committed.wait(), timeout=1)
        task.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        replacement = ReplacementServer.instances[-1]
        assert code._servers["fake"] is replacement
        assert old.closed is True
        assert code._definitions["fake"]["command"] == list(new_command)
    finally:
        release.set()
        await code.aclose()


@pytest.mark.asyncio
async def test_apply_consumes_admitted_plan_if_durable_file_disappears(tmp_path):
    path = tmp_path / "sample.py"
    path.write_bytes(b"old")
    calls = 0
    code = None

    class Filesystem:
        async def _apply_lsp_plan(self, plan):
            nonlocal calls
            calls += 1
            (code._plans._durable.root / f"{plan.ident}.json").unlink()
            path.write_bytes(plan.operations[0].new or b"")
            return {"changed": [str(path)]}

    code = CodeTools(tmp_path, fs=Filesystem())
    server = _running_server(("fake-lsp",))
    server.generation = "generation"
    code._servers["fake"] = server
    plan = code._plans.create(
        tmp_path,
        [PlannedOperation("update", path, b"old", b"new", sha256(b"old"))],
        "generation",
        "replace",
        server="fake",
    )
    try:
        result = await code.apply_edit(plan.ident)
        assert result["applied"] is True
        assert calls == 1
        assert path.read_bytes() == b"new"
    finally:
        await code.aclose()


def test_lsp_plan_cache_drops_durable_evictions(tmp_path, monkeypatch):
    monkeypatch.setattr(change_plans, "MAX_PLANS", 2)
    store = EditPlanStore(tmp_path, max_plans=8)
    first = _plan(store, tmp_path, "first")
    second = _plan(store, tmp_path, "second")
    third = _plan(store, tmp_path, "third")

    with pytest.raises(EditError, match="expired or does not exist"):
        store.get(first.ident)
    assert second.ident in store._plans
    assert third.ident in store._plans


def test_lsp_plan_cache_loads_are_bounded(tmp_path):
    producer = EditPlanStore(tmp_path, max_plans=8)
    plans = [_plan(producer, tmp_path, marker) for marker in ("one", "two", "three")]
    consumer = EditPlanStore(tmp_path, max_plans=2)

    for plan in plans:
        assert consumer.get(plan.ident).ident == plan.ident
    assert len(consumer._plans) == 2
    assert plans[0].ident not in consumer._plans


def test_lsp_plan_cache_pruning_uses_metadata_without_decoding_payloads(tmp_path, monkeypatch):
    store = EditPlanStore(tmp_path, max_plans=8)
    first = _plan(store, tmp_path, "first")
    second = _plan(store, tmp_path, "second")
    calls = 0
    original = store._durable.load

    def load(ident):
        nonlocal calls
        calls += 1
        return original(ident)

    monkeypatch.setattr(store._durable, "load", load)
    assert store.get(second.ident).ident == second.ident
    assert calls == 1
    assert first.ident in store._plans


def test_lsp_plan_cache_does_not_bypass_workspace_identity(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    other = tmp_path / "other"
    other.mkdir()
    store = EditPlanStore(workspace)
    plan = _plan(store, workspace, "sample")
    store._durable.workspace = other

    with pytest.raises(EditError, match="another workspace"):
        store.get(plan.ident)
    assert plan.ident not in store._plans


def test_lsp_plan_cache_does_not_bypass_durable_expiry_or_removal(tmp_path):
    store = EditPlanStore(tmp_path)
    plan = _plan(store, tmp_path, "sample")
    target = store._durable.root / f"{plan.ident}.json"
    old = time.time() - 7200
    os.utime(target, (old, old))
    with pytest.raises(EditError, match="expired"):
        store.get(plan.ident)
    assert plan.ident not in store._plans

    plan = _plan(store, tmp_path, "removed")
    store._durable.remove(plan.ident)
    with pytest.raises(EditError, match="expired or does not exist"):
        store.get(plan.ident)
    assert plan.ident not in store._plans
