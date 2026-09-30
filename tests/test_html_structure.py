import json

import pytest

from mypr_mcp import html_tools
from mypr_mcp.http_tools import HTTPTools


@pytest.mark.asyncio
async def test_html_structure_pages_without_dropping_headings(monkeypatch, tmp_path):
    headings = [{"level": 2, "text": f"Heading {index}"} for index in range(20)]

    async def fake_worker(*_args, **_kwargs):
        return {
            "title": "Document",
            "text": "Body",
            "links": [],
            "url": "https://example.test/page",
            "source_hash": "a" * 64,
            "warnings": [],
            "complete": True,
            "stop_reason": None,
            "structure": {
                "headings": headings,
                "metadata": {
                    "title": "Document",
                    "description": "Description",
                    "canonical": "https://example.test/page",
                    "language": "en",
                },
            },
            "structure_truncated": False,
        }

    monkeypatch.setattr(html_tools, "_run_worker", fake_worker)
    tools = HTTPTools(tmp_path, lambda: "structure-test")
    try:
        page = await tools.extract_html("<html />", include_structure=True, max_bytes=4096)
        seen = []
        metadata = {}
        while True:
            seen.extend(page["structure"]["headings"])
            metadata.update(page["structure"]["metadata"])
            assert len(json.dumps(page, ensure_ascii=False, separators=(",", ":")).encode()) <= 4096
            if page["next_cursor"] is None:
                break
            page = await tools.extract_html(cursor=page["next_cursor"], max_bytes=4096)
        assert seen == headings
        assert metadata["canonical"] == "https://example.test/page"
        assert metadata["language"] == "en"
        assert page["structure_truncated"] is False
    finally:
        await tools.aclose()
