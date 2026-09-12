from __future__ import annotations

import asyncio

from mcp.server import MCPServer

server = MCPServer("test-echo", version="1")


@server.tool()
async def echo(text: str) -> str:
    return text


if __name__ == "__main__":
    asyncio.run(server.run_stdio_async())
