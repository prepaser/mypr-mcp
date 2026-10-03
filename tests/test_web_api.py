import pytest

import mypr_mcp.kernel_api as kernel_api
from mypr_mcp.kernel_api import RPCError, Workspace, execution_context
from mypr_mcp.web_api import WebAPI


@pytest.mark.asyncio
async def test_web_api_forwards_manager_methods(monkeypatch, tmp_path):
    calls = []

    async def rpc(op, **fields):
        calls.append((op, fields))
        return fields

    monkeypatch.setattr(kernel_api, "_rpc", rpc)
    ws = Workspace(tmp_path)
    with execution_context({"client_id": "calm-otter", "connection_id": "conn"}):
        await ws.web.providers()
        await ws.web.search("asyncio", provider="brave", limit=2, options={"freshness": "pw"})
        await ws.web.context("asyncio", provider="brave", max_tokens=1024)
        await ws.web.extract("https://example.test/page", provider="tavily")
        await ws.web.page("cursor-1", max_bytes=4096)

    assert calls == [
        ("web", {"method": "providers", "params": {}}),
        (
            "web",
            {
                "method": "search",
                "params": {
                    "query": "asyncio",
                    "provider": "brave",
                    "limit": 2,
                    "options": {"freshness": "pw"},
                    "max_bytes": 32768,
                },
            },
        ),
        (
            "web",
            {
                "method": "context",
                "params": {
                    "query": "asyncio",
                    "provider": "brave",
                    "limit": 10,
                    "max_tokens": 1024,
                    "options": None,
                    "max_bytes": 32768,
                },
            },
        ),
        (
            "web",
            {
                "method": "extract",
                "params": {
                    "urls": ["https://example.test/page"],
                    "provider": "tavily",
                    "options": None,
                    "max_bytes": 32768,
                },
            },
        ),
        ("web", {"method": "page", "params": {"cursor": "cursor-1", "max_bytes": 4096}}),
    ]


@pytest.mark.asyncio
async def test_web_api_requires_client_and_validates_bounds(tmp_path):
    web = Workspace(tmp_path).web
    with pytest.raises(RPCError, match="initialized client"):
        await web.providers()

    with execution_context({"client_id": "calm-otter"}):
        with pytest.raises(ValueError, match="provider must be one of"):
            await web.search("query", provider="google")
        with pytest.raises(ValueError, match="between 1 and 1024"):
            await web.search("query", limit=1025)
        with pytest.raises(ValueError, match="4096 and 1048576"):
            await web.search("query", max_bytes=1024)
        with pytest.raises(ValueError, match="1024 and 32768"):
            await web.context("query", max_tokens=1023)
        with pytest.raises(ValueError, match="between 1 and 20"):
            await web.extract([])
        with pytest.raises(TypeError, match="mapping"):
            await web.search("query", options=[])


def test_web_api_is_exposed_and_helpful(tmp_path):
    ws = Workspace(tmp_path)
    assert isinstance(ws.web, WebAPI)
    assert "ws.web.search" in ws.help("web")
    assert "max_tokens" in ws.help("web.context")


@pytest.mark.asyncio
async def test_web_api_reports_old_manager_capability_gap():
    async def rpc(*_args, **_kwargs):
        raise RPCError("ValueError: Unknown operation: web")

    with execution_context({"client_id": "calm-otter"}):
        with pytest.raises(RPCError, match="does not support web") as failure:
            await WebAPI(rpc, kernel_api._client_context).providers()
    assert failure.value.code == "capability_missing"
    assert failure.value.details["restart_required"] is True
