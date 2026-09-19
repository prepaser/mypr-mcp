from __future__ import annotations

import json

from conftest import execute, mcp_session, result_text


async def test_large_remote_read_and_silent_waiters_over_mcp(workspace):
    async with mcp_session(workspace) as session:
        result = await execute(
            session,
            "import asyncio, json, sys\n"
            "job = await ws.shell.start([sys.executable, '-c', \"print('x'*1500000, end='')\"])\n"
            "cursor = None\n"
            "size = 0\n"
            "for _ in range(2048):\n"
            "    page = await job.read(cursor=cursor, max_bytes=32000, wait_ms=1000)\n"
            "    size += len(page['output'])\n"
            "    cursor = page['cursor']\n"
            "    if page['state'] == 'succeeded' and not page['has_more']:\n"
            "        break\n"
            "await job\n"
            "silent = ws.tasks.start(asyncio.sleep(.1))\n"
            "started = asyncio.get_running_loop().time()\n"
            "waiters = await asyncio.gather(silent.read(wait_ms=5000), "
            "silent.expect('absent', timeout=5))\n"
            "print(json.dumps({'size': size, 'cached': len(job.output()), "
            "'waited': asyncio.get_running_loop().time()-started, 'waiters': waiters}))",
        )
        assert result["state"] == "succeeded", result.get("error")
        info = json.loads(result_text(result))
        assert info["size"] == info["cached"] == 1500000
        assert info["waited"] < 2
        assert info["waiters"][1]["reason"] == "eof"


async def test_python_stream_selection_and_live_history_attach(workspace):
    async with mcp_session(workspace) as session:
        result = await execute(
            session,
            "import asyncio, json, sys, os\n"
            "async def mixed():\n"
            "    print('out', end='')\n"
            "    print('err', end='', file=sys.stderr)\n"
            "    print('tail', end='')\n"
            "    await asyncio.sleep(.2)\n"
            "job = ws.tasks.start(mixed())\n"
            "attached = await ws.tasks.attach('python:' + "
            "os.environ['MYPR_GENERATION'] + ':' + job.id)\n"
            "await job\n"
            "stdout = await job.read(stream='stdout')\n"
            "stderr = await job.read(stream='stderr')\n"
            "print(json.dumps({'same': attached is job, 'stdout': stdout['output'], "
            "'stderr': stderr['output']}))",
        )
        assert result["state"] == "succeeded", result.get("error")
        info = json.loads(result_text(result))
        assert info == {"same": True, "stdout": "outtail", "stderr": "err"}
        recorded = await execute(
            session,
            "await asyncio.sleep(.1)\n"
            "print(json.dumps('python:' + os.environ['MYPR_GENERATION'] + ':' + job.id))",
        )
        history_id = json.loads(result_text(recorded))
        reset = await execute(session, "await ws.reset()", wait_ms=15000)
        assert reset["state"] == "succeeded"
        restored = await execute(
            session,
            f"import json\njob = await ws.tasks.attach({history_id!r})\n"
            "print(json.dumps({'all': (await job.read())['output'], "
            "'stdout': (await job.read(stream='stdout'))['output'], "
            "'stderr': (await job.read(stream='stderr'))['output']}))",
        )
        assert restored["state"] == "succeeded", restored.get("error")
        assert json.loads(result_text(restored)) == {
            "all": "outerrtail",
            "stdout": "outtail",
            "stderr": "err",
        }


def test_shell_pages_defer_multibyte_character_that_does_not_fit(tmp_path):
    from mypr_mcp.journal import append_events
    from mypr_mcp.services import Shells

    events = [
        {"type": "stream", "stream": "stdout", "text": "ab"},
        {"type": "stream", "stream": "stdout", "text": "é!"},
    ]
    first, index, offset = Shells._page_events(events, 0, 0, stream=None, max_bytes=3)
    assert "".join(event["text"] for event in first) == "ab"
    second, end, _ = Shells._page_events(events, index, offset, stream=None, max_bytes=3)
    assert "".join(event["text"] for event in second) == "é!"
    assert end == 2
    journal = tmp_path / "output.jsonl"
    append_events(journal, events)
    first, index, offset, more = Shells._read_journal_page(journal, 0, 0, stream=None, max_bytes=3)
    assert "".join(event["text"] for event in first) == "ab"
    assert more
    second, _, _, more = Shells._read_journal_page(journal, index, offset, stream=None, max_bytes=3)
    assert "".join(event["text"] for event in second) == "é!"
    assert not more


async def test_cancel_and_monitor_do_not_merge_the_same_output_twice(monkeypatch):
    import asyncio

    import mypr_mcp.kernel_api as api

    entered = asyncio.Event()
    release = asyncio.Event()

    async def rpc(op, **fields):
        if op == "shell_cancel":
            return None
        assert op == "shell_read"
        if fields.get("wait_ms"):
            entered.set()
            await release.wait()
        else:
            release.set()
        return {
            "state": "cancelled",
            "output": [{"stream": "stdout", "text": "once"}],
            "cursor": "done",
            "has_more": False,
        }

    monkeypatch.setattr(api, "_rpc", rpc)
    job = api.RemoteTask("a" * 32, api.TaskManager())
    await entered.wait()
    await job.cancel()
    await job._monitor
    assert job.output() == "once"
