from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import mypr_mcp.cells as cells
import mypr_mcp.kernel_api as kernel_api
from mypr_mcp.cells import CellExecutor, CellHandle
from mypr_mcp.diagnostics import safe_error, safe_error_details


def test_safe_error_handles_bad_repr_and_utf8_limit():
    class Bad(BaseException):
        def __str__(self):
            raise RuntimeError("cannot format")

    assert safe_error(Bad()) == "Bad: <unprintable exception>"
    assert len(safe_error(ValueError("가" * 2000), 1024).encode()) <= 1024
    assert safe_error(ValueError("\ud800")) == "ValueError: ?"
    assert safe_error_details(ValueError("x" * 1024), 1024)[1]
    assert not safe_error_details(ValueError("x" * 1000), 1024)[1]
    output = kernel_api.OutputBuffer()
    output.write("\ud800")
    assert output.get() == "?"
    assert kernel_api._bounded_history_output("\ud800") == "?"


@pytest.mark.asyncio
async def test_python_reporter_falls_back_for_terminal_event_failure(monkeypatch):
    events = []

    async def fake_rpc(op, **fields):
        if op == "task_event":
            raise OSError("manager temporarily unavailable")
        events.append((op, fields))

    monkeypatch.setattr(kernel_api, "_rpc", fake_rpc)

    class Fatal(BaseException):
        pass

    async def fail():
        raise Fatal("boom")

    manager = kernel_api.TaskManager()
    handle = manager.start(fail())
    await asyncio.gather(handle._task, return_exceptions=True)
    await asyncio.gather(*list(manager._reporters))

    assert handle.status()["status"] == "failed"
    assert [op for op, _ in events] == ["task_terminal"]
    assert events[0][1]["event"]["state"] == "failed"
    assert events[0][1]["event"]["error"] == "Fatal: boom"


@pytest.mark.asyncio
async def test_completed_handles_are_bounded_and_cancel_is_idempotent(monkeypatch):
    async def fake_rpc(*args, **kwargs):
        return None

    monkeypatch.setattr(kernel_api, "_rpc", fake_rpc)
    monkeypatch.setattr(kernel_api, "COMPLETED_TASKS", 2)
    manager = kernel_api.TaskManager()
    handles = [manager.start(asyncio.sleep(0)) for _ in range(3)]
    await asyncio.gather(*(handle._task for handle in handles))
    await asyncio.gather(*list(manager._reporters))

    assert len(manager._handles) == 2
    assert handles[2].id in manager._handles
    assert await handles[2].cancel() is False
    assert handles[2].result() is None


@pytest.mark.asyncio
async def test_pruned_explicit_task_ids_cannot_be_reused(monkeypatch):
    async def fake_rpc(*args, **kwargs):
        return None

    monkeypatch.setattr(kernel_api, "_rpc", fake_rpc)
    monkeypatch.setattr(kernel_api, "COMPLETED_TASKS", 0)
    manager = kernel_api.TaskManager()
    first = manager.start(asyncio.sleep(0), task_id="stable")
    await first
    await asyncio.gather(*list(manager._reporters))

    with pytest.raises(ValueError, match="already exists"):
        manager.start(asyncio.sleep(0), task_id="stable")


@pytest.mark.parametrize("ident", ["", 0, False])
async def test_invalid_task_ids_close_rejected_coroutine(ident):
    pending = asyncio.sleep(0)
    with pytest.raises(ValueError, match="non-empty string"):
        kernel_api.TaskManager().start(pending, task_id=ident)
    assert pending.cr_frame is None


@pytest.mark.asyncio
async def test_generated_ids_do_not_create_an_unbounded_tombstone_ledger(monkeypatch):
    async def fake_rpc(*args, **kwargs):
        return None

    monkeypatch.setattr(kernel_api, "_rpc", fake_rpc)
    monkeypatch.setattr(kernel_api, "COMPLETED_TASKS", 0)
    manager = kernel_api.TaskManager()
    handles = [manager.start(asyncio.sleep(0)) for _ in range(1000)]
    await asyncio.gather(*(handle._task for handle in handles))
    await asyncio.gather(*list(manager._reporters))

    assert manager._counter == 1000
    assert not manager._explicit_ids
    assert not hasattr(manager, "_used_ids")


@pytest.mark.asyncio
async def test_handle_registration_rejects_reserved_and_active_collisions(monkeypatch):
    async def fake_rpc(*args, **kwargs):
        return None

    monkeypatch.setattr(kernel_api, "_rpc", fake_rpc)
    manager = kernel_api.TaskManager()
    for ident in (
        "a" * 32,
        "task-local-1",
        "python:" + "b" * 32 + ":task",
        "remote-watch:job",
    ):
        with pytest.raises(ValueError, match="reserved generated namespace"):
            manager.start(asyncio.sleep(0), task_id=ident)

    custom = manager.start(asyncio.sleep(0), task_id="custom")
    with pytest.raises(ValueError, match="already exists"):
        manager._track(SimpleNamespace(id="custom"), generated=True)
    await custom
    await asyncio.gather(*list(manager._reporters))


@pytest.mark.asyncio
async def test_hidden_explicit_ids_remain_reserved(monkeypatch):
    async def fake_rpc(*args, **kwargs):
        return None

    monkeypatch.setattr(kernel_api, "_rpc", fake_rpc)
    manager = kernel_api.TaskManager()
    hidden = manager.start(asyncio.sleep(0), task_id="hidden", visible=False)
    await hidden
    with pytest.raises(ValueError, match="already exists"):
        manager.start(asyncio.sleep(0), task_id="hidden", visible=False)


@pytest.mark.asyncio
async def test_lifecycle_gate_rejects_new_tasks_until_aborted():
    manager = kernel_api.TaskManager()
    manager._begin_lifecycle()
    pending = asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="lifecycle transition"):
        manager.start(pending)
    assert pending.cr_frame is None

    manager._abort_lifecycle()
    handle = manager.start(asyncio.sleep(0))
    await handle
    await asyncio.gather(*manager._reporters)


@pytest.mark.asyncio
async def test_remote_task_does_not_poll_with_opaque_read_cursor(monkeypatch):
    calls = []

    async def fake_rpc(op, **fields):
        calls.append((op, fields))
        if op == "shell_read" and len(calls) == 1:
            return {
                "state": "running",
                "output": [],
                "cursor": "mypr-shell1.cursor",
                "has_more": False,
            }
        if op == "shell_read":
            raise kernel_api.RPCError("manager temporarily unavailable")
        raise AssertionError(f"unexpected {op}")

    monkeypatch.setattr(kernel_api, "_rpc", fake_rpc)
    handle = kernel_api.RemoteTask("a" * 32, kernel_api.TaskManager())

    await asyncio.wait_for(handle._monitor, 1)

    assert [op for op, _ in calls] == ["shell_read", "shell_read"]
    assert handle.status()["status"] == "lost"
    with pytest.raises(kernel_api.RPCError, match="manager temporarily unavailable"):
        await handle


@pytest.mark.parametrize(("method", "operation"), [("reset", "reset"), ("restart", "restart")])
@pytest.mark.asyncio
async def test_workspace_lifecycle_gate_blocks_task_start_during_rpc(
    monkeypatch, tmp_path, method, operation
):
    entered = asyncio.Event()
    release = asyncio.Event()

    async def fake_rpc(op, **fields):
        if op == operation:
            entered.set()
            await release.wait()
            return {"accepted": True}
        return None

    monkeypatch.setattr(kernel_api, "_rpc", fake_rpc)
    workspace = kernel_api.Workspace(tmp_path)

    async def current_cell():
        workspace.tasks._handles["cell"] = SimpleNamespace(
            id="cell", _task=asyncio.current_task(), status=lambda: {"status": "running"}
        )
        with kernel_api.execution_context({"client_id": "client", "exec_id": "cell"}):
            try:
                await getattr(workspace, method)()
            except kernel_api.ResetRequested:
                return

    current = asyncio.create_task(current_cell())
    await entered.wait()
    pending = asyncio.sleep(0)
    with pytest.raises(RuntimeError, match="lifecycle transition"):
        workspace.tasks.start(pending)
    assert pending.cr_frame is None
    release.set()
    await current


@pytest.mark.parametrize(("method", "operation"), [("reset", "reset"), ("restart", "restart")])
@pytest.mark.asyncio
async def test_workspace_lifecycle_gate_aborts_on_rpc_failure(
    monkeypatch, tmp_path, method, operation
):
    async def failed_rpc(op, **fields):
        if op == operation:
            raise kernel_api.RPCError("manager rejected lifecycle request")
        return None

    monkeypatch.setattr(kernel_api, "_rpc", failed_rpc)
    workspace = kernel_api.Workspace(tmp_path)

    async def current_cell():
        workspace.tasks._handles["cell"] = SimpleNamespace(
            id="cell", _task=asyncio.current_task(), status=lambda: {"status": "running"}
        )
        with kernel_api.execution_context({"client_id": "client", "exec_id": "cell"}):
            with pytest.raises(kernel_api.RPCError, match="manager rejected"):
                await getattr(workspace, method)()

    await asyncio.create_task(current_cell())
    handle = workspace.tasks.start(asyncio.sleep(0))
    await handle
    await asyncio.gather(*workspace.tasks._reporters)


@pytest.mark.asyncio
async def test_cell_terminal_uses_manager_rpc_fallback(monkeypatch):
    calls = []

    async def fake_rpc(op, **fields):
        calls.append((op, fields))

    monkeypatch.setattr(cells, "_rpc", fake_rpc)

    class Session:
        def send(self, *args, **kwargs):
            raise OSError("closed IOPub")

    class Kernel:
        session = Session()
        iopub_socket = object()

    class Shell:
        execution_count = 0

    task = asyncio.create_task(asyncio.sleep(0))
    await task
    handle = CellHandle("e" * 32, task, kernel_api.OutputBuffer(), generation="generation")
    executor = CellExecutor(Kernel(), Shell(), kernel_api.TaskManager())
    executor._send_terminal(handle, {}, "failed", ValueError("broken"))
    await asyncio.sleep(0)

    info = calls[0][1].pop("error_info")
    assert info["type"] == "ValueError"
    assert info["operation"] == "cell.execute"
    assert info["message"] == "ValueError: broken"
    assert calls == [
        (
            "cell_terminal",
            {
                "exec_id": "e" * 32,
                "generation": "generation",
                "state": "failed",
                "error": "ValueError: broken",
                "error_truncated": False,
            },
        )
    ]


@pytest.mark.asyncio
async def test_cell_terminal_fallback_timeout_completes_handle(monkeypatch):
    async def blocked_rpc(*args, **kwargs):
        await asyncio.Event().wait()

    monkeypatch.setattr(cells, "_rpc", blocked_rpc)
    monkeypatch.setattr(cells, "_CELL_REPORT_TIMEOUT", 0.01)

    class Session:
        def send(self, *args, **kwargs):
            raise OSError("closed IOPub")

    class Kernel:
        session = Session()
        iopub_socket = object()

    class Shell:
        execution_count = 0

    task = asyncio.create_task(asyncio.sleep(0))
    await task
    handle = CellHandle("f" * 32, task, kernel_api.OutputBuffer(), generation="generation")
    executor = CellExecutor(Kernel(), Shell(), kernel_api.TaskManager())
    executor._send_terminal(handle, {}, "failed", ValueError("broken"))

    await asyncio.sleep(0.05)
    assert handle.status()["status"] == "queued"
