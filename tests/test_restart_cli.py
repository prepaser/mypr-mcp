import asyncio
import os
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from mypr_mcp.instructions import COMMON_INSTRUCTIONS


async def test_mcp_handshake_survives_workspace_startup_failure(tmp_path):
    script = """
import asyncio
from pathlib import Path
from mypr_mcp import cli
async def unavailable(workspace):
    raise RuntimeError('Incompatible workspace protocol; run mypr-mcp restart')
cli.ensure = unavailable
asyncio.run(cli.serve(Path.cwd()))
"""
    params = StdioServerParameters(
        command=sys.executable,
        args=["-c", script],
        cwd=str(tmp_path),
        env=dict(os.environ),
    )
    async with stdio_client(params) as (reader, writer):
        async with ClientSession(reader, writer) as session:
            initialized = await asyncio.wait_for(session.initialize(), 5)
            assert initialized.instructions == COMMON_INSTRUCTIONS
            tools = await session.list_tools()
            assert {tool.name for tool in tools.tools} == {"init", "execute", "poll"}
            result = await session.call_tool("init", {})
            assert result.is_error
            assert "run mypr-mcp restart" in result.content[0].text
            assert len((await session.list_tools()).tools) == 3
