from __future__ import annotations

import asyncio
import subprocess

import pytest

from mypr_mcp.filesystem import Filesystem
from mypr_mcp.pages import PageLimitReached, Pages
from mypr_mcp.search import Search


class SearchWorkspace:
    def __init__(self):
        self.fs = SearchFilesystem()


class SearchFilesystem:
    def __init__(self):
        self.calls = []

    async def search(
        self,
        pattern=None,
        *,
        paths=None,
        mode=None,
        max_bytes=32_768,
        max_matches=100,
        cursor=None,
        page_cursor=None,
    ):
        self.calls.append(
            {
                "pattern": pattern,
                "paths": paths,
                "mode": mode,
                "max_bytes": max_bytes,
                "max_matches": max_matches,
                "cursor": cursor,
                "page_cursor": page_cursor,
            }
        )
        marker = cursor if cursor is not None else page_cursor
        return {
            "matches": ["first" if marker is None else "second"],
            "next_cursor": "next" if marker is None else None,
            "has_more": marker is None,
        }


@pytest.mark.asyncio
async def test_search_pages_drop_query_arguments_on_continuation():
    workspace = SearchWorkspace()
    pages = Pages(workspace)

    result = [
        page
        async for page in pages.iter(
            "fs.search",
            "TODO",
            paths="src",
            max_bytes=123,
            max_matches=7,
            mode="matches",
        )
    ]

    assert [page["matches"] for page in result] == [["first"], ["second"]]
    assert workspace.fs.calls == [
        {
            "pattern": "TODO",
            "paths": "src",
            "mode": "matches",
            "max_bytes": 123,
            "max_matches": 7,
            "cursor": None,
            "page_cursor": None,
        },
        {
            "pattern": None,
            "paths": None,
            "mode": "matches",
            "max_bytes": 123,
            "max_matches": 7,
            "cursor": "next",
            "page_cursor": None,
        },
    ]


@pytest.mark.asyncio
async def test_search_page_limit_can_resume_with_original_query_arguments():
    workspace = SearchWorkspace()
    pages = Pages(workspace)

    with pytest.raises(PageLimitReached) as raised:
        _ = [
            page
            async for page in pages.iter(
                workspace.fs.search,
                "TODO",
                paths="src",
                max_bytes=123,
                max_matches=7,
                mode="matches",
                max_pages=1,
            )
        ]

    error = raised.value
    assert error.next_kwargs == {
        "cursor": "next",
        "max_bytes": 123,
        "max_matches": 7,
        "mode": "matches",
    }
    resumed = [
        page
        async for page in pages.iter(
            error.method,
            "TODO",
            paths="src",
            max_pages=1,
            **error.next_kwargs,
        )
    ]

    assert resumed == [{"matches": ["second"], "next_cursor": None, "has_more": False}]
    assert workspace.fs.calls[-1]["pattern"] is None
    assert workspace.fs.calls[-1]["paths"] is None


@pytest.mark.asyncio
async def test_search_pages_started_with_page_cursor_do_not_run_query_again():
    workspace = SearchWorkspace()
    pages = Pages(workspace)

    result = [
        page
        async for page in pages.iter(
            "fs.search",
            "TODO",
            paths="src",
            page_cursor="next",
        )
    ]

    assert result == [{"matches": ["second"], "next_cursor": None, "has_more": False}]
    assert workspace.fs.calls == [
        {
            "pattern": None,
            "paths": None,
            "mode": None,
            "max_bytes": 32_768,
            "max_matches": 100,
            "cursor": "next",
            "page_cursor": None,
        }
    ]


async def test_real_search_pages_resume_without_running_ripgrep_again(tmp_path):
    (tmp_path / "sample.py").write_text("TODO\nTODO\nTODO\n")

    class Shell:
        calls = 0

        async def run(self, command, *, cwd, **kwargs):
            self.calls += 1
            result = await asyncio.to_thread(
                subprocess.run, command, cwd=cwd, capture_output=True, text=True, timeout=10
            )
            return {
                "returncode": result.returncode,
                "stdout": result.stdout,
                "stderr": result.stderr,
            }

    shell = Shell()
    fs = Filesystem(tmp_path, searcher=Search(tmp_path, shell).search)
    workspace = type("W", (), {"fs": fs})()
    pages = Pages(workspace)
    result = []
    with pytest.raises(PageLimitReached) as raised:
        async for page in pages.iter(
            fs.search, "TODO", paths="sample.py", max_matches=1, max_pages=1
        ):
            result.append(page)
    error = raised.value
    result.extend([
        page async for page in pages.iter(
            error.method, "TODO", paths="sample.py", **error.next_kwargs
        )
    ])
    assert sum(len(page["matches"]) for page in result) == 3
    reread = await fs.search(page_cursor=result[0]["page_cursor"], max_matches=1)
    assert reread["matches"] == result[0]["matches"]
    with pytest.raises(ValueError, match="cursor accepts only page budgets"):
        await fs.search("TODO", page_cursor=result[0]["page_cursor"])
    assert shell.calls == 1


async def test_search_pages_reject_conflicting_cursor_aliases():
    with pytest.raises(ValueError, match="cursor and page_cursor"):
        _ = [page async for page in Pages(SearchWorkspace()).iter(
            "fs.search", cursor="first", page_cursor="second"
        )]
