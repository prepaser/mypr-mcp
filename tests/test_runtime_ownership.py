from __future__ import annotations

import asyncio
import contextlib
import gc
import os
import signal
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from weakref import WeakValueDictionary

import pytest

import mypr_mcp.cli as cli
import mypr_mcp.runtime as runtime_module
from mypr_mcp.runtime import Runtime

_SOURCE_ROOT = str(Path(cli.__file__).resolve().parents[1])


def _process_alive(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/stat", encoding="ascii") as file:
            fields = file.read().rsplit(") ", 1)[1].split()
    except FileNotFoundError:
        return False
    return fields[0] not in {"Z", "X"}


async def _wait_for_file(path: Path) -> None:
    async with asyncio.timeout(5):
        while not await asyncio.to_thread(path.exists):  # noqa: ASYNC110
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_startup_manager_guard_stops_detached_descendant(tmp_path):
    guard = Path(cli.__file__).with_name("process_guard.py")
    child_pid_file = tmp_path / "child.pid"
    manager = (
        "import subprocess, sys, time; "
        "child=subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)', "
        "'--'], start_new_session=True); "
        "open(sys.argv[1], 'w').write(str(child.pid)); time.sleep(30)"
    )
    proc = subprocess.Popen(  # noqa: ASYNC220
        [
            sys.executable,
            str(guard),
            "--parent-pid",
            str(os.getpid()),
            "--tree",
            "--",
            sys.executable,
            "-c",
            manager,
            str(child_pid_file),
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    try:
        await _wait_for_file(child_pid_file)
        child_pid = int(child_pid_file.read_text())
        await cli._stop_spawned(proc)
        assert proc.poll() is not None
        assert not _process_alive(child_pid)
    finally:
        if proc.poll() is None:
            await cli._stop_spawned(proc)


@pytest.mark.asyncio
async def test_manager_survives_ensure_client_exit(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    pid_file = tmp_path / "manager.pid"
    client = """
import asyncio, subprocess, sys
from pathlib import Path
import mypr_mcp.cli as cli

real_popen = subprocess.Popen
pid_file = Path(sys.argv[1])
workspace = Path(sys.argv[2])

def start_manager(_args, **kwargs):
    assert _args == [sys.executable, '-m', 'mypr_mcp.cli', '_manager']
    assert kwargs['start_new_session']
    process = real_popen([sys.executable, '-c', 'import time; time.sleep(30)'], **kwargs)
    pid_file.write_text(str(process.pid))
    return process

async def find_runtime(_workspace):
    return None

async def rpc(*_args, **_kwargs):
    return {'healthy': True}

cli.subprocess.Popen = start_manager
cli.find_runtime = find_runtime
cli.manager_running = lambda _workspace: False
cli.rpc = rpc
asyncio.run(cli.ensure(workspace))
"""
    child = await asyncio.create_subprocess_exec(
        sys.executable,
        "-c",
        client,
        str(pid_file),
        str(workspace),
        cwd=workspace,
        env={**os.environ, "PYTHONPATH": _SOURCE_ROOT},
    )
    assert await asyncio.wait_for(child.wait(), 10) == 0
    manager_pid = int(pid_file.read_text())
    try:
        assert _process_alive(manager_pid)
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(manager_pid, signal.SIGTERM)
        async with asyncio.timeout(2):
            while await asyncio.to_thread(_process_alive, manager_pid):  # noqa: ASYNC110
                await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_manager_sigkill_stops_runtime_command_tree(tmp_path):
    manager_pid_file = tmp_path / "manager.pid"
    child_pid_file = tmp_path / "installer-child.pid"
    worker = (
        "import subprocess, sys, time; "
        "child=subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], "
        "start_new_session=True); "
        "open(sys.argv[1], 'w').write(str(child.pid)); time.sleep(30)"
    )
    manager = """
import asyncio, os, sys
from pathlib import Path
from mypr_mcp.runtime import Runtime

async def main():
    runtime = Runtime.__new__(Runtime)
    command = asyncio.create_task(runtime.command(sys.executable, '-c', sys.argv[2], sys.argv[3]))
    async with asyncio.timeout(5):
        while not Path(sys.argv[3]).exists():
            await asyncio.sleep(0.01)
    Path(sys.argv[1]).write_text(str(os.getpid()))
    await command

asyncio.run(main())
"""
    proc = subprocess.Popen(  # noqa: ASYNC220
        [
            sys.executable,
            "-c",
            manager,
            str(manager_pid_file),
            worker,
            str(child_pid_file),
        ],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    child_pid = None
    try:
        await _wait_for_file(manager_pid_file)
        await _wait_for_file(child_pid_file)
        manager_pid = int(manager_pid_file.read_text())
        child_pid = int(child_pid_file.read_text())
        assert manager_pid == proc.pid
        os.kill(manager_pid, signal.SIGKILL)
        async with asyncio.timeout(10):
            await asyncio.to_thread(proc.wait)
        assert proc.returncode == -signal.SIGKILL
    finally:
        if proc.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGTERM)
            await asyncio.to_thread(proc.wait)
        if child_pid is not None:
            async with asyncio.timeout(10):
                while await asyncio.to_thread(_process_alive, child_pid):  # noqa: ASYNC110
                    await asyncio.sleep(0.01)


@pytest.mark.asyncio
async def test_runtime_command_timeout_stops_process_group(tmp_path, monkeypatch):
    child_pid_file = tmp_path / "child.pid"
    child = "import time; time.sleep(30)"
    parent = (
        "import subprocess, sys, time; "
        "child=subprocess.Popen([sys.executable, '-c', sys.argv[2]]); "
        "open(sys.argv[1], 'w').write(str(child.pid)); time.sleep(30)"
    )
    monkeypatch.setattr(runtime_module, "COMMAND_TIMEOUT", 0.5)
    runtime = Runtime.__new__(Runtime)

    with pytest.raises(TimeoutError):
        await runtime.command(sys.executable, "-c", parent, str(child_pid_file), child)

    child_pid = int(child_pid_file.read_text())
    async with asyncio.timeout(2):
        while await asyncio.to_thread(_process_alive, child_pid):  # noqa: ASYNC110
            await asyncio.sleep(0.01)


def test_restart_guard_checks_python_tasks_with_origin():
    runtime = Runtime.__new__(Runtime)
    runtime.execs = {}
    runtime.task_records = {"python-task": {"kind": "python", "state": "running"}}
    runtime.shells = SimpleNamespace(active=[])
    origin = {"id": "origin", "state": "running"}
    runtime.execs[origin["id"]] = origin

    with pytest.raises(RuntimeError, match="active work"):
        runtime._check_restart_busy(origin, False)

    runtime._check_restart_busy(origin, True)
    runtime.task_records["python-task"]["state"] = "succeeded"
    runtime._check_restart_busy(origin, False)


@pytest.mark.asyncio
async def test_task_event_admission_is_serialized_with_restart():
    runtime = Runtime.__new__(Runtime)
    runtime._admission_lock = asyncio.Lock()
    runtime.stopping = asyncio.Event()
    runtime.resetting = False
    runtime.restarting = None
    recorded = []

    async def record(event, client, connection_id, op):
        recorded.append((event["state"], client, connection_id, op))

    runtime.record_task_event = record
    await runtime.admit_task_event({"state": "running"}, "c", "x", "task_event")
    runtime.restarting = "ticket"
    with pytest.raises(RuntimeError, match="not accepting"):
        await runtime.admit_task_event({"state": "running"}, "c", "x", "task_event")
    await runtime.admit_task_event({"state": "cancelled"}, "c", "x", "task_terminal")

    assert [event[0] for event in recorded] == ["running", "cancelled"]


def test_task_locks_are_weak_but_shared_while_referenced():
    runtime = Runtime.__new__(Runtime)
    runtime._task_locks = WeakValueDictionary()
    locks = [runtime.task_lock(str(index)) for index in range(500)]
    shared = runtime.task_lock("shared")
    second_reference = runtime.task_lock("shared")

    assert shared is second_reference
    assert len(runtime._task_locks) == 501

    del locks, shared, second_reference
    gc.collect()
    assert not runtime._task_locks
