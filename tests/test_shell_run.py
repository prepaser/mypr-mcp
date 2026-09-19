import asyncio
import os
import sys

import pytest
import pytest_asyncio

import mypr_mcp.kernel_api as api
from mypr_mcp.services import Shells


@pytest_asyncio.fixture
async def shell(monkeypatch, tmp_path):
    service = Shells(tmp_path)

    async def rpc(op, **args):
        if op == "shell_start":
            return await service.start(**args)
        if op == "shell_poll":
            return await service.poll(args["id"], args["cursor"])
        if op == "shell_cancel":
            return await service.cancel(args["id"])
        if op == "shell_write":
            return await service.write(args["id"], args["text"], eof=args["eof"])
        raise AssertionError(op)

    monkeypatch.setattr(api, "_rpc", rpc)
    commands = api.Shell(api.TaskManager())
    yield commands, service
    await service.close()


async def test_run_argv_stdin_streams_and_failed_exit(shell, tmp_path):
    commands, _ = shell
    arg = "with spaces; $(touch unexpected)"
    result = await commands.run(
        [
            sys.executable,
            "-c",
            "import os,sys; print(sys.argv[1]); print(sys.stdin.read()); "
            'print(os.getenv("VALUE"), file=sys.stderr); sys.exit(7)',
            arg,
        ],
        cwd=tmp_path,
        env={"VALUE": "custom"},
        input="hello\nworld",
    )
    assert result["state"] == "failed"
    assert result["returncode"] == 7
    assert result["stdout"] == arg + "\nhello\nworld\n"
    assert result["stderr"] == "custom\n"
    assert not result["truncated"]
    assert not (tmp_path / "unexpected").exists()
    output = commands._tasks.get(result["id"]).output()
    assert result["stdout"] in output
    assert result["stderr"] in output


async def test_run_large_stdin_and_output_do_not_deadlock(shell):
    commands, _ = shell
    text = "hello 🌏\n" * 30000
    result = await commands.run(
        [sys.executable, "-c", "import sys; sys.stdout.write(sys.stdin.read())"],
        input=text,
        max_bytes=100,
        timeout=10,
    )
    assert result["returncode"] == 0
    assert result["truncated"]
    assert len(result["stdout"].encode()) <= 100
    assert text.startswith(result["stdout"])
    assert commands._tasks.get(result["id"]).output() == text


async def test_run_timeout_cancels_process_and_keeps_output(shell):
    commands, service = shell
    result = await commands.run(
        [sys.executable, "-c", 'import time; print("started", flush=True); time.sleep(60)'],
        timeout=0.3,
    )
    assert result["timed_out"]
    assert result["state"] == "cancelled"
    assert result["stdout"] == "started\n"
    assert not service.active


async def test_run_check_error_keeps_result(shell):
    commands, _ = shell
    with pytest.raises(api.ShellError) as caught:
        await commands.run("printf failure >&2; exit 9", check=True)
    assert caught.value.result["returncode"] == 9
    assert caught.value.result["stderr"] == "failure"


async def test_run_cancellation_stops_managed_work(shell, monkeypatch):
    commands, service = shell
    started = asyncio.Event()
    original = commands.start

    async def start(*args, **kwargs):
        handle = await original(*args, **kwargs)
        started.set()
        return handle

    monkeypatch.setattr(commands, "start", start)
    task = asyncio.create_task(commands.run([sys.executable, "-c", "import time; time.sleep(60)"]))
    async with asyncio.timeout(5):
        await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not service.active


async def test_run_keeps_current_env_by_default(shell):
    commands, _ = shell
    result = await commands.run([sys.executable, "-c", 'import os; print(os.environ["PATH"])'])
    assert result["stdout"].strip() == os.environ["PATH"]


async def test_cancel_during_launch_still_cleans_up_process(shell, monkeypatch):
    commands, service = shell
    launched = asyncio.Event()
    release = asyncio.Event()
    original = commands.start

    async def start(*args, **kwargs):
        handle = await original(*args, **kwargs)
        launched.set()
        await release.wait()
        return handle

    monkeypatch.setattr(commands, "start", start)
    task = asyncio.create_task(commands.run([sys.executable, "-c", "import time; time.sleep(60)"]))
    async with asyncio.timeout(5):
        await launched.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
    assert not service.active


async def test_interactive_stdin_can_be_written_and_closed(shell):
    commands, _ = shell
    handle = await commands.start(
        [
            sys.executable,
            "-u",
            "-c",
            "import sys; [print(line.upper(), end='') for line in sys.stdin]",
        ],
        stdin=True,
    )
    assert (await handle.write("one\n"))["bytes"] == 4
    await handle.write("two\n", eof=True)
    assert await handle == {"returncode": 0}
    assert handle.output() == "ONE\nTWO\n"
    with pytest.raises(ValueError, match="not running"):
        await handle.write("three\n")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_bytes": -1},
        {"max_bytes": True},
        {"timeout": -1},
        {"timeout": float("nan")},
        {"timeout": True},
    ],
)
async def test_run_rejects_invalid_limits_before_start(shell, kwargs):
    commands, service = shell
    with pytest.raises(ValueError):
        await commands.run("true", **kwargs)
    assert not service._jobs
