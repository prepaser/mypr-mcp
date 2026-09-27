import sys

from conftest import execute, mcp_session, result_text

from mypr_mcp.services import _session_dispatch


async def test_resource_template_cursor_and_server_fields_are_preserved():
    class Session:
        async def list_resource_templates(self, *, params):
            assert params.cursor == "next-page"
            return {
                "resourceTemplates": [{"uriTemplate": "item://{id}", "name": "item"}],
                "nextCursor": "last-page",
                "_meta": {"custom": True},
            }

    result = await _session_dispatch(
        Session(), "list_resource_templates", {"cursor": "next-page"}
    )
    assert result["nextCursor"] == "last-page"
    assert result["_meta"] == {"custom": True}


async def test_templates_can_be_discovered_and_read_inside_python(workspace):
    server = workspace / "resources.py"
    server.write_text(
        "import asyncio\n"
        "from mcp.server import MCPServer\n"
        "server = MCPServer('resources')\n"
        "@server.resource('note://{name}')\n"
        "def note(name: str) -> str:\n"
        "    return 'note:' + name\n"
        "@server.tool()\n"
        "def echo(text: str) -> str:\n"
        "    return text\n"
        "asyncio.run(server.run_stdio_async())\n"
    )
    async with mcp_session(workspace) as session:
        result = await execute(
            session,
            f"await ws.mcp.configure('resources', {{'command': {sys.executable!r}, "
            f"'args': [{str(server)!r}]}})\n"
            "ws.local['templates'] = await ws.mcp.list_resource_templates('resources')\n"
            "print(ws.local['templates']['resourceTemplates'][0]['uriTemplate'])\n"
            "print(await ws.mcp.read_resource('resources', 'note://example'))\n"
            "print(await ws.mcp.call_tool('resources', 'echo', {'text': 'still works'}))",
        )
        assert result["state"] == "succeeded", result
        assert "note://{name}" in result_text(result)
        assert "note:example" in result_text(result)
        assert "still works" in result_text(result)
