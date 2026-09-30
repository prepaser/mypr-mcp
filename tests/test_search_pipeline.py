from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from mypr_mcp.search import Search


@pytest.mark.asyncio
async def test_search_cursor_contract_rejects_query_before_loading_snapshot(tmp_path, monkeypatch):
    search = Search(tmp_path, object())

    def unexpected_decode(_cursor):
        raise AssertionError("query validation must precede cursor decoding")

    monkeypatch.setattr(search.snapshots, "decode", unexpected_decode)
    with pytest.raises(ValueError, match="cursor accepts only page budgets"):
        await search.search("needle", cursor="cursor")


@pytest.mark.asyncio
async def test_search_pipeline_classifies_backend_warning(monkeypatch, tmp_path):
    class Runner:
        async def run(self, command, **kwargs):
            return {"returncode": 0, "stdout": "", "stderr": "notice"}

    monkeypatch.setattr(Search, "_build", lambda *args: ["fake"])
    result = await Search(tmp_path, Runner()).search("needle")

    assert result["complete"] is False
    assert result["stop_reason"] == "backend_warning"
    assert result["warnings"] == [{"code": "backend_diagnostic", "message": "notice"}]


@pytest.mark.asyncio
async def test_search_pipeline_timeout_releases_workspace_slot(monkeypatch, tmp_path):
    class Runner:
        runtime = SimpleNamespace(search_slots=asyncio.Semaphore(1))

        async def stream(self, command, **kwargs):
            raise TimeoutError

    monkeypatch.setattr(Search, "_build", lambda *args: ["fake"])
    runner = Runner()
    result = await Search(tmp_path, runner).search("needle", timeout=1)

    assert result["stop_reason"] == "timeout"
    assert runner.runtime.search_slots._value == 1
