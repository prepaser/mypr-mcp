from __future__ import annotations

import asyncio
import json

from conftest import decode_result, execute, mcp_session, poll_until_done, result_text


async def test_module_skill_and_filesystem_apis_over_mcp(workspace):
    async with mcp_session(workspace) as session:
        result = await execute(
            session,
            "import json\n"
            "module = await ws.modules.write('demo', 'VALUE = 3\\n')\n"
            "checked = await ws.modules.check('demo', test_code='assert VALUE == 3')\n"
            "loaded = ws.modules.load('demo')\n"
            "changed = await ws.modules.write('demo', 'VALUE = 8\\n', "
            "expected_hash=module['revision'])\n"
            "reloaded = ws.modules.reload('demo')\n"
            "skill_text = '---\\nname: demo\\n---\\n# Demo\\n'\n"
            "skill = await ws.skills.write('demo', skill_text)\n"
            "validation = await ws.skills.validate('demo')\n"
            "preview = await ws.skills.write('demo', '---\\nname: demo\\n---\\n# Changed\\n', "
            "expected_hash=skill['revision'], dry_run=True)\n"
            "await ws.fs.write('data/item.txt', 'hello\\n', create_parents=True)\n"
            "tree = await ws.fs.tree('data', depth=2)\n"
            "metadata = await ws.fs.stat('data/item.txt')\n"
            "print(json.dumps({'checked': checked, 'values': [loaded.VALUE, reloaded.VALUE], "
            "'changed': changed['revision'], 'validation': validation, 'preview': preview, "
            "'tree': tree, 'metadata': metadata}))",
        )
        payload = json.loads(result_text(result))
        assert payload["checked"]["valid"] is True
        assert payload["values"] == [3, 8]
        assert payload["changed"]
        assert payload["validation"]["valid"] is True
        assert payload["preview"]["dry_run"] is True
        assert payload["preview"]["changed"] is True
        assert payload["tree"]["entries"][0]["path"] == "data/item.txt"
        assert payload["metadata"]["kind"] == "file"
        assert payload["metadata"]["size"] == 6


async def test_workspace_module_and_skill_validation_failures_over_mcp(workspace):
    async with mcp_session(workspace) as session:
        bad_module = await execute(session, "await ws.modules.check('bad', 'if:')")
        assert bad_module["state"] == "failed"
        assert "SyntaxError" in bad_module["error"]

        bad_skill = await execute(
            session,
            "await ws.skills.write('bad', '---\\ntags: [broken\\n---\\n# Bad\\n')",
        )
        assert bad_skill["state"] == "failed"
        assert "Invalid skill" in bad_skill["error"]


async def test_structured_messages_reply_and_filtered_wait_over_mcp(workspace):
    async with (
        mcp_session(workspace, client_id="sender") as sender,
        mcp_session(workspace, client_id="receiver") as receiver,
    ):
        sent = await execute(
            sender,
            "import json\n"
            "message = await ws.messages.send('receiver', 'question', "
            "data={'kind': 'review'})\n"
            "print(json.dumps(message))",
        )
        original = json.loads(result_text(sent))
        assert original["data"] == {"kind": "review"}
        assert original["reply_to"] is None

        received = await execute(
            receiver,
            "page = await ws.messages.read(sender='sender')\npage['messages'][0]['text']",
        )
        assert result_text(received).strip(" '\n") == "question"

        waiting = decode_result(
            await sender.call_tool(
                "execute",
                {
                    "code": (
                        "import json\n"
                        "page = await ws.messages.read(sender='receiver', "
                        f"reply_to={original['id']}, wait_ms=5000)\n"
                        "print(json.dumps(page))"
                    ),
                    "wait_ms": 0,
                },
            )
        )
        assert waiting["state"] in {"queued", "running"}

        reply = await execute(
            receiver,
            "import json\n"
            f"message = await ws.messages.reply({original['id']}, 'answer', data={{'ok': True}})\n"
            "print(json.dumps(message))",
        )
        response = json.loads(result_text(reply))
        assert response["reply_to"] == original["id"]
        assert response["data"] == {"ok": True}

        completed = await poll_until_done(sender, waiting["exec_id"], initial=waiting)
        page = json.loads(result_text(completed))
        assert page["messages"][0]["reply_to"] == original["id"]
        assert page["messages"][0]["data"] == {"ok": True}


async def test_workspace_lock_coordinates_two_mcp_clients(workspace):
    async with (
        mcp_session(workspace, client_id="first") as first,
        mcp_session(workspace, client_id="second") as second,
    ):
        started = decode_result(
            await first.call_tool(
                "execute",
                {
                    "code": (
                        "import asyncio\n"
                        "ws.local['release'] = asyncio.Event()\n"
                        "async with ws.locks.acquire('build'):\n"
                        "    print('first-held')\n"
                        "    await ws.local['release'].wait()"
                    ),
                    "wait_ms": 0,
                },
            )
        )
        assert started["state"] in {"queued", "running"}

        waiting = decode_result(
            await second.call_tool(
                "execute",
                {
                    "code": (
                        "async with ws.locks.acquire('build', timeout=5):\n    print('second-held')"
                    ),
                    "wait_ms": 100,
                },
            )
        )
        assert waiting["state"] in {"queued", "running"}
        await asyncio.sleep(0.05)

        lock_state = await execute(first, "import json\nprint(json.dumps(ws.locks.list()))")
        assert any(item.get("waiting") for item in json.loads(result_text(lock_state)))

        released = await execute(first, "ws.local['release'].set()")
        assert released["state"] == "succeeded"
        completed = await poll_until_done(second, waiting["exec_id"], initial=waiting)
        assert completed["state"] == "succeeded"
        assert "second-held" in result_text(completed)
