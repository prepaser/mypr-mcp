import json

from conftest import execute, mcp_session, result_text


async def test_multi_file_patch_and_terminal_over_mcp(workspace):
    (workspace / "old.txt").write_text("before\n")
    (workspace / "remove.txt").write_text("unused\n")
    patch = """*** Begin Patch
*** Update File: old.txt
*** Move to: moved.txt
@@
-before
+after
*** Add File: new/added.txt
+created
*** Delete File: remove.txt
*** End Patch"""
    async with mcp_session(workspace) as session:
        result = await execute(
            session,
            f"ws.local['preview'] = await ws.fs.apply_patch({patch!r}, dry_run=True)\n"
            "assert (ws.workspace / 'old.txt').exists()\n"
            "assert not (ws.workspace / 'moved.txt').exists()\n"
            f"ws.local['patched'] = await ws.fs.apply_patch({patch!r})\n"
            "import json, sys\n"
            "ws.local['terminal'] = await ws.shell.run([sys.executable, '-c', "
            "'import os; print(os.isatty(0), os.isatty(1), os.isatty(2)); "
            'print(os.get_terminal_size()); open("/dev/tty").close()\'], '
            "pty=True, rows=31, cols=101, timeout=10)\n"
            "print(json.dumps(ws.local['terminal']))",
        )
        assert result["state"] == "succeeded", result
        payload = json.loads(result_text(result))
        assert payload["returncode"] == 0
        assert "True True True" in payload["stdout"]
        assert "columns=101, lines=31" in payload["stdout"]
        assert payload["stderr"] == ""
        assert not (workspace / "old.txt").exists()
        assert not (workspace / "remove.txt").exists()
        assert (workspace / "moved.txt").read_text() == "after\n"
        assert (workspace / "new/added.txt").read_text() == "created\n"


async def test_terminal_resize_and_input_through_handle(workspace):
    async with mcp_session(workspace) as session:
        result = await execute(
            session,
            "import sys, json\n"
            "ws.local['terminal'] = await ws.shell.start([sys.executable, '-u', '-c', "
            '\'import os; text=input(); print("received:"+text); '
            "print(os.get_terminal_size())'], pty=True)\n"
            "await ws.local['terminal'].resize(37, 99)\n"
            "await ws.local['terminal'].write('hello terminal\\n')\n"
            "await ws.local['terminal']\n"
            "print(json.dumps(ws.local['terminal'].output()))",
        )
        assert result["state"] == "succeeded", result
        output = json.loads(result_text(result))
        assert "received:hello terminal" in output
        assert "columns=99, lines=37" in output
