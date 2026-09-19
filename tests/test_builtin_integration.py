import json

from conftest import execute, mcp_session, result_text


async def test_builtin_file_search_patch_and_shell_work_together(workspace):
    async with mcp_session(workspace) as session:
        result = await execute(
            session,
            "import json, sys\n"
            "ws.local['created'] = await ws.fs.write('demo/value.txt', "
            "'hello world\\n', create_parents=True)\n"
            "ws.local['page'] = await ws.fs.read('demo/value.txt')\n"
            "ws.local['patch'] = await ws.fs.patch('demo/value.txt', "
            "[{'old': 'world', 'new': 'agent'}], "
            "expected_hash=ws.local['page']['revision'])\n"
            "ws.local['search'] = await ws.fs.search('agent', paths='demo')\n"
            "ws.local['search_job'] = await ws.tasks.attach(ws.local['search']['id'])\n"
            "await ws.local['search_job']\n"
            "assert ws.local['search_job'].status()['status'] == 'succeeded'\n"
            "ws.local['run'] = await ws.shell.run([sys.executable, '-c', "
            "'import sys; print(sys.stdin.read().upper())'], "
            "input=(await ws.fs.read('demo/value.txt'))['text'])\n"
            "print(json.dumps({'matches': ws.local['search']['matches'], "
            "'run': ws.local['run'], 'patch': ws.local['patch']}))",
        )
        assert result["state"] == "succeeded", result
        payload = json.loads(result_text(result))
        assert payload["matches"][0]["path"].endswith("demo/value.txt")
        assert payload["matches"][0]["line"] == 1
        assert payload["run"]["stdout"] == "HELLO AGENT\n\n"
        assert payload["run"]["returncode"] == 0
        assert payload["patch"]["changed"]
        stale = await execute(
            session,
            "await ws.fs.write('demo/value.txt', 'stale', "
            "expected_hash=ws.local['page']['revision'])",
        )
        assert stale["state"] == "failed"
        assert "Revision mismatch" in stale["error"]
        assert (workspace / "demo/value.txt").read_text() == "hello agent\n"


async def test_shell_run_nonzero_and_interactive_stdin_over_mcp(workspace):
    async with mcp_session(workspace) as session:
        result = await execute(
            session,
            "import json, sys\n"
            "ws.local['bad'] = await ws.shell.run('printf bad >&2; exit 3')\n"
            "ws.local['input_job'] = await ws.shell.start([sys.executable, '-u', '-c', "
            "'import sys; print(sys.stdin.read())'], stdin=True)\n"
            "await ws.local['input_job'].write('one\\ntwo', eof=True)\n"
            "await ws.local['input_job']\n"
            "print(json.dumps({'bad': ws.local['bad'], 'text': ws.local['input_job'].output()}))",
        )
        assert result["state"] == "succeeded", result
        payload = json.loads(result_text(result))
        assert payload["bad"]["returncode"] == 3
        assert payload["bad"]["stderr"] == "bad"
        assert payload["text"] == "one\ntwo\n"
