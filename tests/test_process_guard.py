from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from mypr_mcp.services import Shells


@pytest.mark.asyncio
async def test_shell_result_is_readable_after_completed_job_eviction(tmp_path: Path):
    shells = Shells(tmp_path, completed_records=0)
    try:
        started = await shells.start("printf persisted")
        for _ in range(500):
            result = await shells.poll(started["id"])
            if result["state"] in {"succeeded", "failed", "cancelled"}:
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("shell did not finish")
        assert started["id"] not in shells._jobs
        assert result["state"] == "succeeded"
        assert result["output"] == [{"stream": "stdout", "text": "persisted"}]
        journal = tmp_path / ".mypr" / "jobs" / f"{started['id']}.jsonl"
        metadata = tmp_path / ".mypr" / "jobs" / f"{started['id']}.json"
        assert journal.exists()
        assert metadata.exists()
    finally:
        await shells.close()


def test_parent_death_terminates_shell_group(tmp_path: Path):
    source_root = Path(__file__).resolve().parents[1]
    manager = """
import asyncio
import os
import signal
import sys
from pathlib import Path
from mypr_mcp.services import Shells

async def main():
    shells = Shells(Path(sys.argv[1]))
    job = await shells.start("sleep 30")
    print(shells._jobs[job["id"]].process.pid, flush=True)
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
        for _ in range(100):
            try:
                os.kill(child_pid, 0)
            except ProcessLookupError:
                break
            time.sleep(0.02)
        else:
            pytest.fail("shell child survived parent death")
    finally:
        with contextlib.suppress(ProcessLookupError):
            os.kill(child_pid, signal.SIGKILL)


def test_parent_death_kills_term_ignoring_descendant_after_command_exits(tmp_path):
    command = (
        "import os, signal, sys, time\n"
        "if os.fork(): sys.exit(0)\n"
        "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
        "print(os.getpid(), flush=True)\n"
        "time.sleep(30)\n"
    )
    manager = """
import asyncio, json, os, signal, sys
from pathlib import Path
from mypr_mcp.services import Shells
async def main():
    shells = Shells(Path(sys.argv[1]))
    job = await shells.start([sys.executable, '-u', '-c', sys.argv[2]])
    while True:
        result = await shells.poll(job['id'])
        if result['output']:
            group = shells._jobs[job['id']].group_id
            print(json.dumps([group, int(result['output'][0]['text'])]), flush=True)
            break
        await asyncio.sleep(.01)
    await asyncio.sleep(.2)
    os.kill(os.getpid(), signal.SIGKILL)
asyncio.run(main())
"""
    with subprocess.Popen(
        [sys.executable, "-c", manager, str(tmp_path), command],
        stdout=subprocess.PIPE,
        text=True,
    ) as process:
        assert process.stdout is not None
        group, descendant = json.loads(process.stdout.readline())
        try:
            assert process.wait(timeout=5) == -signal.SIGKILL
            for _ in range(300):
                if not Shells._group_has_live_members(group):
                    break
                time.sleep(0.02)
            else:
                pytest.fail(f"descendant {descendant} survived manager death")
        finally:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(group, signal.SIGKILL)


async def test_guard_preserves_signal_returncode(tmp_path):
    shells = Shells(tmp_path)
    try:
        job = await shells.start(
            [sys.executable, "-c", "import os,signal; os.kill(os.getpid(),signal.SIGTERM)"]
        )
        for _ in range(300):
            result = await shells.poll(job["id"])
            if result["state"] == "failed":
                break
            await asyncio.sleep(0.01)
        assert result["result"] == {"returncode": -signal.SIGTERM}
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_guard_isolated_from_python_environment(tmp_path):
    shells = Shells(tmp_path)
    try:
        job = await shells.start(["/bin/echo", "ok"], env={"PYTHONHOME": "/nonexistent"})
        for _ in range(300):
            result = await shells.poll(job["id"])
            if result["state"] != "running":
                break
            await asyncio.sleep(0.01)
        assert result["state"] == "succeeded"
        assert "ok" in "".join(event["text"] for event in result["output"])
    finally:
        await shells.close()


@pytest.mark.asyncio
async def test_guard_closes_parent_output_fds_while_descendant_runs(tmp_path):
    shells = Shells(tmp_path)
    try:
        job = await shells.start("sleep 30 >/dev/null 2>&1 & exit 0")
        record = shells._jobs[job["id"]]
        await asyncio.wait_for(asyncio.gather(*record.readers), 5)
        assert record.state == "running"
        assert all(reader.done() for reader in record.readers)
    finally:
        await shells.close()
