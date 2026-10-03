from __future__ import annotations

import asyncio
import json
import sys
from types import SimpleNamespace

import pytest

from mypr_mcp.dependency_service import DependencyService
from mypr_mcp.diagnostics import RPCError
from mypr_mcp.http_tools import HTTPTools
from mypr_mcp.runtime import Runtime, _recover_runs
from mypr_mcp.runtime_registry import list_managers


@pytest.mark.parametrize(
    "bad", [[], None, 42, {"state": []}, {"id": "bad", "state": "succeeded", "finished": 10**1000}]
)
def test_corrupt_run_does_not_hide_valid_recovered_execution(tmp_path, capsys, bad):
    runs = tmp_path / "runs"
    runs.mkdir()
    (runs / "bad.json").write_text(json.dumps(bad))
    good = {"id": "good", "state": "running", "client_id": "client"}
    (runs / "good.json").write_text(json.dumps(good))
    recovered = []
    history = SimpleNamespace(record=lambda _kind, value: recovered.append(value))

    _recover_runs(tmp_path, history)

    assert len(recovered) == 1 and recovered[0]["id"] == "good"
    assert recovered[0]["state"] == "lost"
    assert json.loads((runs / "good.json").read_text())["state"] == "lost"
    assert "Skipping execution record bad.json" in capsys.readouterr().err


async def test_reset_registration_failure_removes_prior_generation(tmp_path, monkeypatch):
    import mypr_mcp.runtime as module
    import mypr_mcp.runtime_registry as registry

    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(tmp_path / "config.toml"))
    runtime = Runtime(tmp_path)
    try:
        await runtime._register_manager()
        old = runtime.generation
        assert list_managers(runtime.config_store.global_path)[0]["generation"] == old

        async def noop(*args, **kwargs):
            return None

        def unavailable(*args, **kwargs):
            raise PermissionError("registry unavailable")

        runtime.close_kernel = noop
        runtime.start_kernel = noop
        runtime.lose_python_tasks = noop
        runtime.close_shells = noop
        runtime.new_shells = lambda: object()
        runtime.mcp = SimpleNamespace(close=noop)
        monkeypatch.setattr(module, "MCPBridge", lambda *args, **kwargs: runtime.mcp)
        monkeypatch.setattr(module, "ScanService", lambda *args: object())
        with monkeypatch.context() as patch:
            patch.setattr(registry, "register", unavailable)

            await runtime.reset(None)

            assert runtime.generation != old
            assert runtime.registry_error.endswith("PermissionError: registry unavailable")
            assert list_managers(runtime.config_store.global_path) == []
    finally:
        await runtime._unregister_manager()


async def test_registry_cleanup_retries_recorded_generation_after_permission_recovery(
    tmp_path, monkeypatch
):
    import mypr_mcp.runtime_registry as registry

    monkeypatch.setenv("MYPR_GLOBAL_CONFIG", str(tmp_path / "config.toml"))
    runtime = Runtime(tmp_path)
    try:
        await runtime._register_manager()
        old = runtime.generation
        runtime.generation = "new"

        def unavailable(*args, **kwargs):
            raise PermissionError("registry unavailable")

        with monkeypatch.context() as patch:
            patch.setattr(registry, "register", unavailable)
            patch.setattr(registry, "unregister", unavailable)
            await runtime._register_manager()
            assert list_managers(runtime.config_store.global_path)[0]["generation"] == old

        await runtime._unregister_manager()
        assert list_managers(runtime.config_store.global_path) == []
    finally:
        await runtime._unregister_manager()


async def test_browser_auto_install_uses_dependency_events_and_active_count(tmp_path):
    runtime = Runtime(tmp_path)
    runtime.healthy = True
    runtime.py = sys.executable
    runtime.km = SimpleNamespace(provisioner=SimpleNamespace(pid=1))
    entered, release = asyncio.Event(), asyncio.Event()
    installed = False
    events = []
    launches = []

    async def prepare(browser, *, track):
        nonlocal installed
        entered.set()
        await release.wait()
        installed = True
        return {"installed": True}

    async def launch(browser, **options):
        launches.append((browser, options))
        return {"generation": runtime.generation}

    async def inventory(names):
        return {
            name: {"name": f"browser:{name}", "status": "installed" if installed else "missing"}
            for name in names
        }

    async def record(state, fields):
        events.append((state, fields))

    runtime.browser = SimpleNamespace(prepare=prepare, ensure=launch)
    service = DependencyService(
        tmp_path, runtime.py, {}, None, runtime._install_dependency_browser, record,
    )

    async def packages(names):
        return {name: {"name": name, "status": "installed"} for name in names}

    service._packages = packages
    service._browsers = inventory
    runtime.dependencies = service
    request = asyncio.create_task(runtime._dispatch({
        "op": "browser_server", "client_id": "review-client", "exec_id": "cell-1",
    }))
    try:
        await asyncio.wait_for(entered.wait(), 1)
        assert service.active_count == 1
        release.set()
        await request
        assert [state for state, _ in events] == ["started", "succeeded"]
        assert events[0][1]["client_id"] == "review-client"
        assert events[0][1]["exec_id"] == "cell-1"
        assert launches[0][1]["install"] is False
    finally:
        release.set()
        await asyncio.gather(request, return_exceptions=True)
        await service.close()


@pytest.mark.parametrize("installed", [False, True])
async def test_disabled_browser_auto_install_keeps_policy(tmp_path, installed):
    runtime = Runtime(tmp_path)
    runtime.healthy = True
    runtime.py = sys.executable
    calls = []

    async def launch(browser, **options):
        calls.append(options)
        return {}

    async def inventory(names):
        return {name: {"status": "installed" if installed else "missing"} for name in names}

    runtime.browser = SimpleNamespace(ensure=launch)
    service = DependencyService(tmp_path, runtime.py, {"auto_install": False}, None, None)
    service._browsers = inventory
    runtime.dependencies = service
    try:
        if installed:
            await runtime._dispatch({"op": "browser_server"})
            assert calls[0]["install"] is False
        else:
            with pytest.raises(RPCError) as failed:
                await runtime._dispatch({"op": "browser_server"})
            assert failed.value.code == "dependency_missing"
            assert calls == []
    finally:
        await service.close()


@pytest.mark.parametrize("options", [{"executable_path": "/custom/browser"}, {"channel": "chrome"}])
async def test_custom_browser_does_not_prepare_managed_engine(tmp_path, options):
    runtime = Runtime(tmp_path)
    runtime.healthy = True
    calls = []

    async def forbidden(*args, **kwargs):
        raise AssertionError("custom browser must not install managed engines")

    async def launch(browser, **kwargs):
        calls.append(kwargs)
        return {}

    runtime.dependencies = SimpleNamespace(ensure=forbidden)
    runtime.browser = SimpleNamespace(ensure=launch)

    await runtime._dispatch({"op": "browser_server", "launch_options": options})

    assert calls[0]["install"] is False


@pytest.mark.parametrize("html", ["\ud800", "x" * (16 * 1024 * 1024 + 1)])
async def test_invalid_html_rejected_before_install(tmp_path, html):
    async def forbidden(*args, **kwargs):
        raise AssertionError("invalid HTML must not prepare packages")

    tools = HTTPTools(tmp_path, ensure_dependencies=forbidden)
    with pytest.raises(ValueError, match="invalid Unicode|input limit"):
        await tools.extract_html(html)


async def test_corrupt_history_get_returns_diagnostic_without_polling(tmp_path):
    runtime = Runtime(tmp_path)
    record = {"id": "a" * 32, "kind": "execution", "corrupt": True}
    runtime.history = SimpleNamespace(get=lambda _: record)

    async def io(function, *args, **kwargs):
        return function(*args, **kwargs)

    async def forbidden(*args, **kwargs):
        raise AssertionError("corrupt history must not poll missing execution metadata")

    runtime.io = io
    runtime.poll = forbidden
    assert await runtime._dispatch_history("history_get", {"id": record["id"]}) == record
    with pytest.raises(RPCError) as failed:
        await runtime._dispatch_history("history_task_read", {"id": record["id"]})
    assert failed.value.code == "history_corrupt"


@pytest.mark.parametrize("value", [[], {"state": "succeeded"}])
async def test_corrupt_cold_execution_poll_has_classified_error(tmp_path, value):
    runtime = Runtime(tmp_path)
    ident = "a" * 32
    runs = runtime.root / "runs"
    runs.mkdir()
    (runs / f"{ident}.json").write_text(json.dumps(value))

    async def io(function, *args, **kwargs):
        return function(*args, **kwargs)

    runtime.io = io
    with pytest.raises(RPCError) as failed:
        await runtime.poll(ident, wait_ms=0)
    assert failed.value.code == "execution_metadata_corrupt"
    assert failed.value.details["outcome_unknown"] is True


async def test_attach_rejects_corrupt_history_before_creating_handle(monkeypatch):
    import mypr_mcp.kernel_api as api

    async def rpc(*args, **kwargs):
        return {"id": "a" * 32, "kind": "execution", "corrupt": True}

    monkeypatch.setattr(api, "_rpc", rpc)
    manager = api.TaskManager()
    with pytest.raises(RPCError) as failed:
        await manager.attach("a" * 32)
    assert failed.value.code == "history_corrupt"
    assert manager.list() == []
