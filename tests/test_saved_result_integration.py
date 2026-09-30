import ast
import json

from conftest import execute, mcp_session, result_text


async def test_saved_task_result_can_be_attached_after_reset(workspace):
    async with mcp_session(workspace) as session:
        saved = await execute(session, "\n".join([
            "import asyncio, json",
            "ws.local['job'] = ws.tasks.start(asyncio.sleep(0, result={'answer': 42}), "
            "persist_result=True)",
            "await ws.local['job']",
            "await ws.local['job'].wait_saved()",
            "print(json.dumps({'history_id': 'python:' + (await ws.status())['generation'] "
            "+ ':' + ws.local['job'].id, 'saved': ws.local['job'].status()['result_persisted']}))",
        ]))
        assert saved["state"] == "succeeded", saved
        info = json.loads(result_text(saved))
        assert info["saved"] is True
        reset = await execute(session, "await ws.reset()")
        assert reset["state"] == "succeeded", reset
        attached = await execute(session, "\n".join([
            f"ws.local['restored'] = await ws.tasks.attach({info['history_id']!r})",
            "await ws.local['restored'].wait_saved()",
            "print(ws.local['restored'].result())",
        ]))
        assert attached["state"] == "succeeded", attached
        assert ast.literal_eval(result_text(attached).strip()) == {"answer": 42}
