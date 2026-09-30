from __future__ import annotations

import asyncio
import os
import signal
import sys
import textwrap
from pathlib import Path

import pytest

SOURCE_ROOT = str(Path(__file__).resolve().parents[1] / "src")


def _alive(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/stat", encoding="ascii") as stream:
            state = stream.read().rsplit(") ", 1)[1].split()[0]
    except FileNotFoundError:
        return False
    return state not in {"Z", "X"}


async def _wait_for(path: Path) -> None:
    async with asyncio.timeout(5):
        while not await asyncio.to_thread(path.exists):  # noqa: ASYNC110
            await asyncio.sleep(0.01)


@pytest.mark.asyncio
@pytest.mark.skipif(sys.platform != "linux", reason="process_guard tree mode is Linux-only")
async def test_lsp_guard_stops_server_tree_after_client_exit(tmp_path: Path):
    server = tmp_path / "fake_lsp.py"
    descendant_file = tmp_path / "descendant.pid"
    process_id_file = tmp_path / "process-id"
    server.write_text(
        textwrap.dedent(
            f"""
            import json
            import subprocess
            import sys
            import time
            from pathlib import Path

            descendant = subprocess.Popen(
                [sys.executable, "-c", "import time; time.sleep(30)"],
                start_new_session=True,
            )
            Path({str(descendant_file)!r}).write_text(str(descendant.pid))

            def read_message():
                headers = {{}}
                while True:
                    line = sys.stdin.buffer.readline()
                    if not line:
                        return None
                    if line in (b"\\r\\n", b"\\n"):
                        break
                    key, value = line.decode("ascii").split(":", 1)
                    headers[key.lower()] = value.strip()
                return json.loads(sys.stdin.buffer.read(int(headers["content-length"])))

            def send(message):
                body = json.dumps(message, separators=(",", ":")).encode()
                sys.stdout.buffer.write(
                    f"Content-Length: {{len(body)}}\\r\\n\\r\\n".encode() + body
                )
                sys.stdout.buffer.flush()

            while True:
                message = read_message()
                if message is None:
                    break
                if message.get("method") == "initialize":
                    Path({str(process_id_file)!r}).write_text(
                        str(message["params"].get("processId"))
                    )
                    send({{
                        "jsonrpc": "2.0",
                        "id": message["id"],
                        "result": {{"capabilities": {{}}}},
                    }})
                elif message.get("method") == "shutdown":
                    send({{"jsonrpc": "2.0", "id": message["id"], "result": None}})
                elif message.get("method") == "exit":
                    break
            time.sleep(30)
            """
        ),
        encoding="utf-8",
    )
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    ready = tmp_path / "ready"
    helper = tmp_path / "client.py"
    helper.write_text(
        textwrap.dedent(
            f"""
            import asyncio
            import os
            from pathlib import Path
            from mypr_mcp.code_tools import CodeTools

            async def main():
                code = CodeTools(Path({str(workspace)!r}))
                await code.configure(
                    "fake",
                    [os.environ["PYTHON"], {str(server)!r}],
                    ["python"],
                    persist=False,
                )
                Path({str(ready)!r}).write_text("ready")
                await asyncio.Event().wait()

            asyncio.run(main())
            """
        ),
        encoding="utf-8",
    )
    env = {**os.environ, "PYTHONPATH": SOURCE_ROOT, "PYTHON": sys.executable}
    client = await asyncio.create_subprocess_exec(
        sys.executable,
        str(helper),
        cwd=workspace,
        env=env,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        await _wait_for(ready)
        await _wait_for(descendant_file)
        await _wait_for(process_id_file)
        assert int(process_id_file.read_text()) == client.pid
        descendant_pid = int(descendant_file.read_text())
        client.send_signal(signal.SIGKILL)
        assert await asyncio.wait_for(client.wait(), 5) == -signal.SIGKILL
        async with asyncio.timeout(5):
            while await asyncio.to_thread(_alive, descendant_pid):  # noqa: ASYNC110
                await asyncio.sleep(0.05)
    finally:
        if client.returncode is None:
            client.kill()
            await client.wait()
        if client.stderr is not None:
            await client.stderr.read()
