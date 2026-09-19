from __future__ import annotations

import asyncio
import contextlib
import os
import re
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from mypr_mcp.services import Shells


async def wait_for(shells: Shells, job_id: str, *, text: str | None = None) -> dict:
    cursor = 0
    events = []
    for _ in range(500):
        result = await shells.poll(job_id, cursor)
        cursor = result["cursor"]
        events.extend(result["output"])
        output = "".join(event["text"] for event in events)
        if text is None:
            if result["state"] not in {"running", "cancelling"}:
                result["output"] = events
                return result
        elif text in output:
            result["output"] = events
            return result
        await asyncio.sleep(0.01)
    pytest.fail("shell did not produce the expected result")


@pytest.mark.asyncio
async def test_pty_is_a_controlling_terminal_and_merges_output(tmp_path):
    shells = Shells(tmp_path)
    try:
        job = await shells.start(
            [
                sys.executable,
                "-u",
                "-c",
                "import os,sys; print(os.isatty(0), os.isatty(1), os.ttyname(0), flush=True); "
                "print('stderr', file=sys.stderr, flush=True)",
            ],
            pty=True,
        )
        result = await wait_for(shells, job["id"])
        output = "".join(event["text"] for event in result["output"])
        assert "True True /dev/pts/" in output
        assert "stderr" in output
        assert [event["stream"] for event in result["output"]] == ["stdout"]
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_pty_supports_input_eof_and_resize(tmp_path):
    shells = Shells(tmp_path)
    try:
        job = await shells.start(
            [
                sys.executable,
                "-u",
                "-c",
                "import os; print(os.get_terminal_size(), flush=True); "
                "print('value=' + input(), flush=True); print(os.get_terminal_size(), flush=True)",
            ],
            pty=True,
        )
        await wait_for(shells, job["id"], text="columns=80")
        assert await shells.resize(job["id"], 40, 100) == {
            "id": job["id"],
            "rows": 40,
            "cols": 100,
        }
        await shells.write(job["id"], "hello\n", eof=True)
        result = await wait_for(shells, job["id"])
        output = "".join(event["text"] for event in result["output"])
        assert "value=hello" in output
        assert "columns=100" in output
        assert result["result"] == {"returncode": 0}
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_pty_ctrl_c_interrupts_child_without_killing_guard(tmp_path):
    shells = Shells(tmp_path)
    try:
        job = await shells.start(
            [
                sys.executable,
                "-u",
                "-c",
                "import time\n"
                "print('ready', flush=True)\n"
                "try:\n"
                "    time.sleep(30)\n"
                "except KeyboardInterrupt:\n"
                "    print('interrupted', flush=True)\n",
            ],
            pty=True,
        )
        await wait_for(shells, job["id"], text="ready")
        await shells.write(job["id"], "\x03")
        result = await wait_for(shells, job["id"])
        output = "".join(event["text"] for event in result["output"])
        assert "interrupted" in output
        assert result["state"] == "succeeded"
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_pty_writer_finishes_when_child_exits(tmp_path):
    shells = Shells(tmp_path)
    try:
        job = await shells.start([sys.executable, "-c", "import time; time.sleep(.2)"], pty=True)
        writer = asyncio.create_task(shells.write(job["id"], "x" * (16 * 1024 * 1024)))
        with pytest.raises((ValueError, BrokenPipeError, ConnectionResetError, OSError)):
            await asyncio.wait_for(writer, 5)
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_pty_supervisor_ignores_terminal_stop_and_quit(tmp_path):
    shells = Shells(tmp_path)
    try:
        quit_job = await shells.start("sleep 30", pty=True)
        await asyncio.sleep(0.1)
        await shells.write(quit_job["id"], "\x1c")
        quit_result = await wait_for(shells, quit_job["id"])
        assert quit_result["result"] == {"returncode": -signal.SIGQUIT}

        stop_job = await shells.start("sleep 30", pty=True)
        await asyncio.sleep(0.1)
        await shells.write(stop_job["id"], "\x1a")
        await asyncio.sleep(0.1)
        assert (await shells.poll(stop_job["id"]))["state"] == "running"
        stop_result = await shells.cancel(stop_job["id"])
        assert stop_result["state"] == "cancelled"
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_pty_rejects_invalid_resize_and_non_pty_resize(tmp_path):
    shells = Shells(tmp_path)
    try:
        with pytest.raises(ValueError):
            await shells.start("true", pty=True, rows=0)
        job = await shells.start("sleep 1")
        with pytest.raises(ValueError, match="pty=True"):
            await shells.resize(job["id"], 24, 80)
        await shells.cancel(job["id"])
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_pty_cancel_cleans_process_groups_created_by_interactive_shell(tmp_path):
    shells = Shells(tmp_path)
    try:
        job = await shells.start(
            ["/bin/bash", "-i", "-c", "sleep 30 & echo CHILD=$!; wait"],
            pty=True,
        )
        result = await wait_for(shells, job["id"], text="CHILD=")
        match = re.search(r"CHILD=(\d+)", "".join(event["text"] for event in result["output"]))
        assert match is not None
        child_pid = int(match[1])
        result = await shells.cancel(job["id"])
        assert result["state"] == "cancelled"
        assert not shells.active
        with pytest.raises(ProcessLookupError):
            os.kill(child_pid, 0)
    finally:
        await shells.close()


def test_pty_parent_death_terminates_child(tmp_path: Path):
    source_root = Path(__file__).resolve().parents[1]
    manager = """
import asyncio, os, signal, sys
from pathlib import Path
from mypr_mcp.services import Shells
async def main():
    shells = Shells(Path(sys.argv[1]))
    job = await shells.start([
        sys.executable, '-u', '-c',
        "import os,signal,time; signal.signal(signal.SIGHUP,signal.SIG_IGN); "
        "signal.signal(signal.SIGTERM,signal.SIG_IGN); "
        "print('CHILD=' + str(os.getpid()), flush=True); "
        "time.sleep(30)",
    ], pty=True)
    while True:
        result = await shells.poll(job['id'])
        text = ''.join(event['text'] for event in result['output'])
        if 'CHILD=' in text:
            print(text.split('CHILD=', 1)[1].split()[0], flush=True)
            break
        await asyncio.sleep(.01)
    os.kill(os.getpid(), signal.SIGKILL)
asyncio.run(main())
"""
    environment = dict(os.environ, PYTHONPATH=str(source_root / "src"))
    process = subprocess.Popen(
        [sys.executable, "-c", manager, str(tmp_path)],
        cwd=source_root,
        env=environment,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert process.stdout is not None
    child_pid = int(process.stdout.readline())
    assert process.wait(timeout=5) == -signal.SIGKILL
    try:
        for _ in range(200):
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.02)
        else:
            pytest.fail("PTY child survived parent death")
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.kill(child_pid, signal.SIGKILL)
