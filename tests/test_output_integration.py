from __future__ import annotations

import ast
import json
from pathlib import Path

from conftest import execute, mcp_session, result_text


def decode(value: dict) -> object:
    result = ast.literal_eval(result_text(value).strip())
    return json.loads(result) if isinstance(result, str) else result


async def test_job_read_and_expect_page_output(workspace: Path):
    async with mcp_session(workspace) as session:
        result = await execute(
            session,
            "import json\n"
            "job = await ws.shell.start(\"printf 'hello'; sleep .05; printf ' world'\")\n"
            "first = await job.read(max_bytes=5, wait_ms=1000)\n"
            "matched = await job.expect('world', timeout=2)\n"
            "json.dumps({'first': first, 'matched': matched})",
        )
        payload = decode(result)
        assert payload["first"]["output"] == "hello"
        assert payload["matched"]["matched"]
        assert payload["matched"]["reason"] == "match"


async def test_attach_persisted_python_task_is_read_only(workspace: Path):
    async with mcp_session(workspace) as session:
        result = await execute(
            session,
            "import asyncio, json\n"
            "async def emit_once():\n"
            "    print('saved output')\n"
            "    return 9\n"
            "task = ws.tasks.start(emit_once())\n"
            "await task\n"
            "await asyncio.sleep(.1)\n"
            "items = await ws.history.list()\n"
            "python = next(item for item in items['items'] if item.get('kind') == 'python')\n"
            "python['history_id']",
        )
        history_id = result_text(result).strip(" '\n")
        await execute(session, "await ws.reset()", wait_ms=15_000)
        attached = await execute(
            session,
            f"import json\n"
            f"attached = await ws.tasks.attach({history_id!r})\n"
            "json.dumps({'status': attached.status(), 'output': attached.output()})",
        )
        payload = decode(attached)
        assert payload["status"]["read_only"] is True
        assert "saved output" in payload["output"]
