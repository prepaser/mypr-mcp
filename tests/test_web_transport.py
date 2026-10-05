from __future__ import annotations

import json

import httpx2
import pytest

from mypr_mcp.diagnostics import RPCError
from mypr_mcp.web_snapshots import WebSnapshots
from mypr_mcp.web_transport import MAX_RESPONSE_BYTES, WebTransport


def _config(provider: str) -> dict:
    return {"default_provider": provider, "providers": {provider: {"api_key_env": "WEB_KEY"}}}


@pytest.mark.parametrize(
    "provider,operation,params",
    [
        ("kagi", "search", {"query": "test", "options": {"lens": "not-an-object"}}),
        ("kagi", "search", {"query": "test", "options": {"extract": True}}),
        ("kagi", "search", {"query": "test", "options": {"extract": {"count": 0}}}),
        ("kagi", "search", {"query": "test", "options": {"lens": {"unknown": "value"}}}),
        ("kagi", "search", {"query": "test", "options": {"filters": {"after": "2026-02-30"}}}),
        ("brave", "context", {"query": "test", "options": {"maximum_number_of_tokens_per_url": 1}}),
        ("tavily", "search", {"query": "test", "options": {"include_answer": True}}),
        (
            "tavily",
            "extract",
            {"urls": ["https://example.test"], "options": {"extract_depth": "wrong"}},
        ),
        (
            "tavily",
            "extract",
            {"urls": ["https://example.test"], "options": {"timeout": float("nan")}},
        ),
        ("tavily", "extract", {"urls": ["https:///missing-host"]}),
    ],
)
def test_invalid_native_options_are_rejected_before_network(provider, operation, params):
    transport = WebTransport(_config(provider))
    with pytest.raises(RPCError):
        transport.validate_request(operation, provider, params)
    assert not transport._clients


async def test_native_lens_extraction_and_trace_are_preserved(monkeypatch):
    async def handler(request):
        body = json.loads(request.content)
        assert body["lens"] == {"sites_included": ["docs.python.org"]}
        assert body["extract"] == {"count": 2}
        assert body["limit"] == 30
        return httpx2.Response(
            200,
            json={
                "meta": {"trace": "kagi-trace"},
                "data": {
                    "search": [
                        {"title": "Docs", "url": "https://docs.python.org", "snippet": "# Docs"}
                    ]
                },
            },
        )

    monkeypatch.setenv("WEB_KEY", "secret")
    transport = WebTransport(_config("kagi"), transport=httpx2.MockTransport(handler))
    try:
        page = await transport.run(
            "search",
            "kagi",
            {
                "query": "test",
                "limit": 30,
                "options": {
                    "lens": {"sites_included": ["docs.python.org"]},
                    "extract": {"count": 2},
                },
            },
        )
        assert page["results"][0]["snippet"] == "# Docs"
        assert page["request_id"] == "kagi-trace"
    finally:
        await transport.close()


async def test_tavily_advanced_extract_and_brave_source_metadata(monkeypatch):
    async def handler(request):
        body = json.loads(request.content)
        if request.url.host == "api.tavily.com":
            assert body["extract_depth"] == "advanced" and body["chunks_per_source"] == 5
            return httpx2.Response(
                200,
                json={
                    "results": [{"url": "https://example.test", "raw_content": "body"}],
                    "failed_results": [],
                },
            )
        assert body["maximum_number_of_tokens"] == 32768
        assert body["maximum_number_of_urls"] == 50
        return httpx2.Response(
            200,
            json={
                "grounding": {
                    "generic": [{"url": "https://example.test", "snippets": ["one", "two"]}]
                },
                "sources": {
                    "https://example.test": {"title": "Example", "hostname": "example.test"}
                },
            },
        )

    monkeypatch.setenv("WEB_KEY", "secret")
    for provider in ("tavily", "brave"):
        transport = WebTransport(_config(provider), transport=httpx2.MockTransport(handler))
        try:
            if provider == "tavily":
                page = await transport.run(
                    "extract",
                    provider,
                    {
                        "urls": ["https://example.test"],
                        "options": {
                            "extract_depth": "advanced",
                            "query": "test",
                            "chunks_per_source": 5,
                            "format": "text",
                        },
                    },
                )
                assert page["results"][0]["format"] == "text"
            else:
                page = await transport.run(
                    "context", provider, {"query": "test", "limit": 50, "max_tokens": 32768}
                )
                assert page["results"][0]["title"] == "Example"
                assert page["results"][0]["content"] == "one\ntwo"
                assert page["results"][0]["metadata"]["source"]["hostname"] == "example.test"
        finally:
            await transport.close()


@pytest.mark.asyncio
async def test_kagi_search_builds_json_request_and_normalizes_results(monkeypatch):
    seen = []

    async def handler(request):
        seen.append(request)
        return httpx2.Response(
            200,
            json={
                "data": {"search": [{"title": "A", "url": "https://example.test", "snippet": "B"}]},
                "request_id": "r1",
            },
            request=request,
        )

    monkeypatch.setenv("WEB_KEY", "secret-token")
    transport = WebTransport(_config("kagi"), transport=httpx2.MockTransport(handler))
    result = await transport.run("search", None, {"query": "hello", "limit": 3, "options": {}})
    await transport.close()

    assert result["results"] == [{"title": "A", "url": "https://example.test", "snippet": "B"}]
    assert result["request_id"] == "r1"
    assert seen[0].url == "https://kagi.com/api/v1/search"
    assert seen[0].headers["Authorization"] == "Bearer secret-token"
    assert json.loads(seen[0].content) == {
        "query": "hello",
        "workflow": "search",
        "format": "json",
        "limit": 3,
    }


@pytest.mark.asyncio
async def test_search_bounds_title_and_reports_oversized_url(monkeypatch):
    oversized_url = "https://example.test/" + ("x" * 9000)

    async def handler(request):
        return httpx2.Response(
            200,
            json={
                "data": {
                    "search": [
                        {
                            "title": "t" * 9000,
                            "url": "https://example.test/title",
                            "snippet": "bounded",
                        },
                        {"title": "t" * 9000, "url": oversized_url, "snippet": "skipped"},
                        {"title": "kept", "url": "https://example.test/kept", "snippet": "ok"},
                    ]
                }
            },
            request=request,
        )

    monkeypatch.setenv("WEB_KEY", "secret-token")
    transport = WebTransport(_config("kagi"), transport=httpx2.MockTransport(handler))
    try:
        result = await transport.run("search", "kagi", {"query": "hello"})
    finally:
        await transport.close()

    assert len(result["results"]) == 2
    assert result["results"][0]["url"] == "https://example.test/title"
    assert len(result["results"][0]["title"].encode()) == 8 * 1024
    assert result["results"][0]["metadata"]["title_truncated"] is True
    assert result["results"][1] == {
        "title": "kept",
        "url": "https://example.test/kept",
        "snippet": "ok",
    }
    assert result["failed_results"][0]["url_truncated"] is True
    assert "exceeds" in result["failed_results"][0]["error"]


@pytest.mark.asyncio
async def test_context_and_extract_bound_provider_metadata(monkeypatch):
    oversized_url = "https://example.test/" + ("x" * 9000)

    async def context_handler(_request):
        return httpx2.Response(
            200,
            json={
                "grounding": {
                    "generic": [
                        {
                            "title": "t" * 9000,
                            "url": "https://example.test/context",
                            "snippets": ["context"],
                        },
                        {"title": "skip", "url": oversized_url, "snippets": ["skip"]},
                    ]
                },
                "sources": {"https://example.test/context": {}},
            },
        )

    async def extract_handler(_request):
        return httpx2.Response(
            200,
            json={
                "results": [{"url": oversized_url, "raw_content": "content"}],
                "failed_results": [{"url": "https://failed.test", "error": "오" * 9000}],
            },
        )

    monkeypatch.setenv("WEB_KEY", "secret-token")
    context_transport = WebTransport(
        _config("brave"), transport=httpx2.MockTransport(context_handler)
    )
    extract_transport = WebTransport(
        _config("tavily"), transport=httpx2.MockTransport(extract_handler)
    )
    try:
        context = await context_transport.run("context", "brave", {"query": "q"})
        extract = await extract_transport.run(
            "extract", "tavily", {"urls": ["https://request.test"]}
        )
    finally:
        await context_transport.close()
        await extract_transport.close()

    assert len(context["results"][0]["title"].encode()) == 8 * 1024
    assert context["results"][0]["metadata"]["title_truncated"] is True
    assert context["failed_results"][0]["url_truncated"] is True
    assert not extract["results"]
    assert extract["failed_results"][0]["url_truncated"] is True
    assert len(extract["failed_results"][1]["error"].encode()) <= 8 * 1024
    assert extract["failed_results"][1]["error_truncated"] is True


@pytest.mark.parametrize("provider,operation", [("brave", "context"), ("tavily", "search")])
async def test_nested_provider_metadata_fits_default_page(monkeypatch, provider, operation):
    url = "https://example.test"
    metadata = {f"field{i}": "x" * 4096 for i in range(32)}
    payload = (
        {
            "grounding": {"generic": [{"url": url, "snippets": ["content"]}]},
            "sources": {url: metadata},
        }
        if operation == "context"
        else {"results": [{"url": url, "content": "content", "source": metadata, "score": 0.5}]}
    )

    async def handler(_request):
        return httpx2.Response(200, json=payload)

    monkeypatch.setenv("WEB_KEY", "secret-token")
    transport = WebTransport(_config(provider), transport=httpx2.MockTransport(handler))
    try:
        result = await transport.run(operation, provider, {"query": "q"})
    finally:
        await transport.close()
    page = WebSnapshots().create("owner", result, 32768)
    assert page["results"][0]["url"] == url
    assert page["results"][0]["metadata"]["metadata_truncated"] is True
    assert "source" not in page["results"][0]["metadata"]
    if operation == "search":
        assert page["results"][0]["metadata"]["score"] == 0.5


@pytest.mark.asyncio
async def test_brave_context_maps_grounding_and_does_not_leak_key(monkeypatch):
    seen = []

    async def handler(request):
        seen.append(request)
        return httpx2.Response(
            200,
            json={
                "grounding": {
                    "generic": [{"title": "A", "url": "https://example.test", "snippets": ["B"]}],
                    "poi": {"title": "Place", "url": "https://place.test", "snippets": ["C"]},
                }
            },
            request=request,
        )

    monkeypatch.setenv("WEB_KEY", "secret-token")
    transport = WebTransport(_config("brave"), transport=httpx2.MockTransport(handler))
    result = await transport.run(
        "context", "brave", {"query": "hello", "limit": 2, "max_tokens": 1200}
    )
    await transport.close()

    assert len(result["results"]) == 2
    assert result["results"][1]["metadata"]["kind"] == "poi"
    assert seen[0].headers["X-Subscription-Token"] == "secret-token"
    assert json.loads(seen[0].content) == {
        "q": "hello",
        "count": 2,
        "maximum_number_of_urls": 2,
        "maximum_number_of_tokens": 1200,
    }
    assert "secret-token" not in json.dumps(result)


@pytest.mark.asyncio
async def test_tavily_extract_preserves_partial_failures(monkeypatch):
    async def handler(request):
        return httpx2.Response(
            200,
            json={
                "results": [{"url": "https://example.test", "raw_content": "# body"}],
                "failed_results": [{"url": "https://missing.test", "error": "not found"}],
                "usage": {"credits": 1},
            },
            request=request,
        )

    monkeypatch.setenv("WEB_KEY", "secret-token")
    transport = WebTransport(_config("tavily"), transport=httpx2.MockTransport(handler))
    result = await transport.run(
        "extract", "tavily", {"urls": ["https://example.test", "https://missing.test"]}
    )
    await transport.close()

    assert result["results"] == [
        {"url": "https://example.test", "content": "# body", "format": "markdown"}
    ]
    assert result["failed_results"] == [{"url": "https://missing.test", "error": "not found"}]
    assert result["usage"] == {"credits": 1}


async def test_empty_document_is_a_successful_extraction(monkeypatch):
    async def handler(request):
        return httpx2.Response(
            200,
            json={
                "results": [{"url": "https://example.test", "raw_content": ""}],
                "failed_results": [],
            },
        )

    monkeypatch.setenv("WEB_KEY", "secret")
    transport = WebTransport(_config("tavily"), transport=httpx2.MockTransport(handler))
    try:
        page = await transport.run("extract", "tavily", {"urls": "https://example.test"})
        assert page["results"][0]["content"] == ""
        assert not page.get("failed_results")
    finally:
        await transport.close()


def test_validation_rejects_unsupported_operations_before_requests(monkeypatch):
    monkeypatch.setenv("WEB_KEY", "secret-token")
    transport = WebTransport(_config("kagi"))
    with pytest.raises(RPCError) as failure:
        transport.validate_request("context", "kagi", {"query": "hello"})
    assert failure.value.code == "unsupported_operation"

    with pytest.raises(RPCError):
        transport.validate_request("extract", "kagi", {"urls": ["http://example.test"]})


@pytest.mark.asyncio
async def test_http_status_and_oversize_body_are_structured(monkeypatch):
    monkeypatch.setenv("WEB_KEY", "secret-token")

    async def error_handler(request):
        return httpx2.Response(429, text="secret-token provider details", request=request)

    transport = WebTransport(_config("brave"), transport=httpx2.MockTransport(error_handler))
    with pytest.raises(RPCError) as failure:
        await transport.run("search", "brave", {"query": "hello"})
    assert failure.value.code == "rate_limited"
    assert "secret-token" not in str(failure.value)
    await transport.close()

    async def large_handler(request):
        return httpx2.Response(200, content=b"x" * (MAX_RESPONSE_BYTES + 1), request=request)

    transport = WebTransport(_config("brave"), transport=httpx2.MockTransport(large_handler))
    with pytest.raises(RPCError) as failure:
        await transport.run("search", "brave", {"query": "hello"})
    assert failure.value.code == "response_too_large"
    await transport.close()
