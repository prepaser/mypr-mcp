import ast
import asyncio
import json
import os
import subprocess
from pathlib import Path

import pytest
from conftest import execute, result_text
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

CLI = Path(__file__).resolve().parents[1] / ".venv" / "bin" / "mypr-mcp"


def server_parameters(workspace: Path) -> StdioServerParameters:
    return StdioServerParameters(
        command=str(CLI), args=["serve"], cwd=str(workspace), env=dict(os.environ)
    )


async def test_serve_uses_current_directory_as_workspace(workspace: Path):
    async with stdio_client(server_parameters(workspace)) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("init", {})
            assert not result.is_error, result
            result = await execute(session, "str(ws.workspace), __import__('os').getcwd()")
            assert result["state"] == "succeeded"
            assert ast.literal_eval(result_text(result)) == (str(workspace), str(workspace))
    assert (workspace / ".mypr" / "runtime.json").exists()


async def test_status_uses_current_directory(workspace: Path):
    async with stdio_client(server_parameters(workspace)) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()
            result = await session.call_tool("init", {})
            assert not result.is_error, result
            process = await asyncio.create_subprocess_exec(
                str(CLI),
                "status",
                cwd=str(workspace),
                env=dict(os.environ),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await process.communicate()
            assert process.returncode == 0, stderr.decode()
            state = json.loads(stdout)
            assert state["workspace"] == str(workspace)
            assert state["healthy"] is True


@pytest.mark.parametrize(
    ("flag", "value"),
    [
        ("--client-id", "agent"),
        ("--client-name", "agent"),
        ("--workspace", "/tmp/other-workspace"),
        ("--directory", "/tmp/other-project"),
        ("--project", "/tmp/other-project"),
    ],
)
def test_removed_launch_flags_are_rejected_without_starting_runtime(workspace, flag, value):
    result = subprocess.run(
        [str(CLI), "serve", flag, value],
        cwd=workspace,
        env=dict(os.environ),
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "unrecognized arguments" in result.stderr
    assert not (workspace / ".mypr").exists()
