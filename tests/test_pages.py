from __future__ import annotations

import pytest

from mypr_mcp.pages import PageLimitReached, Pages


class Dummy:
    def __init__(self):
        self.fs = self
        self.calls = []

    async def read(self, path, *, start_line=1, start_byte=None):
        self.calls.append((start_line, start_byte))
        if start_line == 1:
            return {
                "text": "a",
                "revision": "r1",
                "next_cursor": {"line": 2, "byte": 2},
                "has_more": True,
            }
        return {"text": "b", "revision": "r1", "next_cursor": None, "has_more": False}


@pytest.mark.asyncio
async def test_iter_adapts_filesystem_line_and_byte_cursor():
    dummy = Dummy()
    pages = Pages(dummy)
    result = [page async for page in pages.iter("fs.read", "file.txt")]
    assert [page["text"] for page in result] == ["a", "b"]
    assert dummy.calls == [(1, None), (2, 2)]


@pytest.mark.asyncio
async def test_iter_adapts_callable_filesystem_cursor():
    dummy = Dummy()
    pages = Pages(dummy)
    result = [page async for page in pages.iter(dummy.fs.read, "file.txt")]
    assert [page["text"] for page in result] == ["a", "b"]
    assert dummy.calls == [(1, None), (2, 2)]


@pytest.mark.asyncio
async def test_iter_detects_revision_change():
    class Source:
        async def read(self, cursor=None):
            return {
                "revision": "r1" if cursor is None else "r2",
                "next_cursor": "next" if cursor is None else None,
                "has_more": cursor is None,
            }

    with pytest.raises(RuntimeError, match="revision changed"):
        pages = Pages(type("W", (), {"source": Source()})())
        _ = [page async for page in pages.iter("source.read")]


@pytest.mark.asyncio
async def test_iter_detects_nonprogressing_cursor():
    class Source:
        async def read(self, cursor=None):
            return {"next_cursor": "same", "has_more": True}

    with pytest.raises(RuntimeError, match="non-progressing"):
        pages = Pages(type("W", (), {"source": Source()})())
        _ = [page async for page in pages.iter("source.read")]


@pytest.mark.asyncio
async def test_iter_adapts_mcp_camel_case_cursor():
    class MCP:
        async def list_resources(self, cursor=None):
            return {"items": [cursor], "nextCursor": None if cursor else "next"}

    pages = Pages(type("W", (), {"mcp": MCP()})())
    result = [page async for page in pages.iter("mcp.list_resources")]
    assert result[-1]["items"] == ["next"]


@pytest.mark.asyncio
async def test_iter_reports_limit_with_resumable_context():
    class Source:
        def __init__(self):
            self.calls = []

        async def read(self, query, *, cursor=None):
            self.calls.append((query, cursor))
            return {
                "items": [query, cursor],
                "next_cursor": "next" if cursor is None else None,
                "has_more": cursor is None,
            }

    pages = Pages(type("W", (), {"source": Source()})())
    seen = []
    with pytest.raises(PageLimitReached) as raised:
        async for page in pages.iter("source.read", "query", max_pages=1):
            seen.append(page)

    error = raised.value
    assert seen == [{"items": ["query", None], "next_cursor": "next", "has_more": True}]
    assert error.method == "source.read"
    assert error.pages_read == 1
    assert error.next_cursor == "next"
    assert error.next_kwargs == {"cursor": "next"}
    assert error.code == "page_limit_reached"
    assert error.details == {
        "method": "source.read",
        "pages_read": 1,
        "next_cursor": "next",
    }
    resumed = [
        page
        async for page in pages.iter(error.method, "query", max_pages=1, **error.next_kwargs)
    ]
    assert resumed == [{"items": ["query", "next"], "next_cursor": None, "has_more": False}]
    assert pages._workspace.source.calls == [("query", None), ("query", "next")]


@pytest.mark.asyncio
async def test_iter_default_limit_allows_natural_eof():
    class Source:
        async def read(self, cursor=None):
            return {"next_cursor": None, "has_more": False}

    pages = Pages(type("W", (), {"source": Source()})())
    assert [page async for page in pages.iter("source.read")] == [
        {"next_cursor": None, "has_more": False}
    ]


@pytest.mark.asyncio
async def test_iter_does_not_raise_on_explicit_break_at_limit():
    class Source:
        async def read(self, cursor=None):
            return {"next_cursor": "next", "has_more": True}

    pages = Pages(type("W", (), {"source": Source()})())
    result = []
    async for page in pages.iter("source.read", max_pages=1):
        result.append(page)
        break
    assert result == [{"next_cursor": "next", "has_more": True}]


@pytest.mark.asyncio
async def test_iter_validates_continuation_before_limit_error():
    class Source:
        async def read(self, *, cursor=None):
            return {"next_cursor": None, "has_more": True}

    pages = Pages(type("W", (), {"source": Source()})())
    with pytest.raises(RuntimeError, match="without a continuation cursor"):
        _ = [page async for page in pages.iter("source.read", max_pages=1)]


async def test_iter_reads_workspace_binary_and_revision_pages(tmp_path):
    from mypr_mcp.kernel_api import Workspace

    ws = Workspace(tmp_path)
    result = await ws.fs.write_bytes("data.bin", b"abcde")
    binary = [page async for page in ws.pages.iter("fs.read_bytes", "data.bin", max_bytes=2)]
    history = [page async for page in ws.pages.iter(
        "fs.read_revision", "data.bin", result["revision"], max_bytes=2
    )]
    assert len(binary) == len(history) == 3
    assert binary[-1]["next_cursor"] is None
    assert history[-1]["next_cursor"] is None
