from __future__ import annotations

import asyncio
import copy
import json
import time

import pytest

from mypr_mcp.diagnostics import RPCError
from mypr_mcp.web_service import WebService
from mypr_mcp.web_snapshots import WebSnapshots

CONFIG = {"providers": {"kagi": {"api_key_env": "MYPR_TEST_KAGI_KEY"}}, "max_concurrency": 1}


def result(content="short"):
    return {
        "provider": "kagi",
        "operation": "search",
        "query": "test",
        "fetched_at": "2026-10-03T00:00:00Z",
        "results": [{"title": "Title", "url": "https://example.test", "content": content}],
    }


class Transport:
    def __init__(self, config):
        self.calls = []
        self.entered = asyncio.Event()
        self.release = asyncio.Event()
        self.release.set()
        self.closed = False

    async def run(self, operation, provider, params):
        self.calls.append((operation, provider, params))
        self.entered.set()
        await self.release.wait()
        return result('한글 😀 \\"\n' * 2000)

    async def close(self):
        self.closed = True


def test_pages_preserve_unicode_and_failures_without_exceeding_budget():
    store = WebSnapshots()
    text = '한글 😀 \\"\n' * 3000
    value = result(text)
    value["results"].append({"url": "https://other.test", "snippet": "second"})
    value["failed_results"] = [{"url": "https://failed.test", "error": "failed" * 2500}]
    page = store.create("alice", value, 4096)
    parts = {}
    failures = []
    while True:
        assert len(json.dumps(page, ensure_ascii=True, separators=(",", ":")).encode()) <= 4096
        assert page["truncated"] is False
        for row in page["results"]:
            index = row["result_index"]
            field = row.get("text_field", "content" if "content" in row else "snippet")
            assert row.get("text_offset", 0) == len(parts.get(index, ""))
            parts[index] = parts.get(index, "") + row[field]
        failures.extend(row["error"] for row in page["failed_results"])
        if not page["has_more"]:
            break
        cursor = page["next_cursor"]
        page = store.page("alice", cursor, 4096)
        assert store.page("alice", cursor, 4096) == page
    assert parts == {0: text, 1: "second"}
    assert "".join(failures) == value["failed_results"][0]["error"]


def test_snapshot_cursor_ownership_expiry_and_eviction():
    store = WebSnapshots(max_count=1)
    first = store.create("alice", result("x" * 10000), 4096)
    cursor = first["next_cursor"]
    with pytest.raises(RPCError, match="invalid or expired"):
        store.page("bob", cursor, 4096)
    changed = cursor[:-1] + ("A" if cursor[-1] != "A" else "B")
    with pytest.raises(RPCError, match="invalid or expired"):
        store.page("alice", changed, 4096)
    second = store.create("alice", result("y" * 10000), 4096)
    with pytest.raises(RPCError, match="invalid or expired"):
        store.page("alice", cursor, 4096)
    store._snapshots[second["snapshot_id"]]["expires"] = time.monotonic() - 1
    with pytest.raises(RPCError, match="invalid or expired"):
        store.page("alice", second["next_cursor"], 4096)
    assert not store._snapshots and store._bytes == 0


def test_metadata_overflow_retains_paid_response_for_larger_page():
    store = WebSnapshots()
    value = result()
    value["query"] = "x" * 4096
    with pytest.raises(RPCError) as failure:
        store.create("alice", value, 4096)
    assert failure.value.code == "output_limit"
    page = store.page("alice", failure.value.details["page_cursor"], 8192)
    assert page["results"][0]["content"] == "short"
    assert page["query"] == value["query"]


def test_pages_split_both_snippet_and_raw_content():
    store = WebSnapshots()
    value = result("본문" * 4000)
    value["results"][0]["snippet"] = "발췌" * 1000
    page = store.create("alice", value, 4096)
    fields = {}
    while True:
        for row in page["results"]:
            field = row["text_field"]
            assert row["text_offset"] == len(fields.get(field, ""))
            fields[field] = fields.get(field, "") + row[field]
        if not page["has_more"]:
            break
        page = store.page("alice", page["next_cursor"], 4096)
    assert fields == {key: value["results"][0][key] for key in ("snippet", "content")}


async def test_paging_makes_one_request_and_logs_only_operation_metadata():
    events = []

    async def record(method, fields):
        events.append((method, fields))

    service = WebService(CONFIG, record, transport_factory=Transport)
    try:
        page = await service.dispatch(
            "search", "alice", {"query": "private query", "max_bytes": 4096}
        )
        while page["has_more"]:
            page = await service.dispatch(
                "page", "alice", {"cursor": page["next_cursor"], "max_bytes": 4096}
            )
        assert len(service.transport.calls) == 1
        assert len(events) == 1
        assert "private query" not in json.dumps(events)
        assert "results" not in events[0][1]
        assert events[0][1]["state"] == "succeeded"
        assert service.active_count == 0
    finally:
        await service.close()


async def test_config_reload_defers_even_force_and_waiting_requests_are_active():
    service = WebService(CONFIG, transport_factory=Transport)
    service.transport.release.clear()
    old = service.transport
    first = asyncio.create_task(service.dispatch("search", "alice", {"query": "first"}))
    second = None
    try:
        await asyncio.wait_for(old.entered.wait(), 2)
        second = asyncio.create_task(service.dispatch("search", "bob", {"query": "second"}))
        await asyncio.sleep(0)
        assert service.active_count == 2 and len(old.calls) == 1
        desired = copy.deepcopy(service.config)
        desired["max_concurrency"] = 2
        deferred = await service.apply_config(desired, force=True)
        assert deferred["deferred"] == ["web"]
        assert service.applied_config["max_concurrency"] == 1
        old.release.set()
        await asyncio.gather(first, second)
        applied = await service.apply_config(desired)
        assert applied["applied"] == ["web"] and old.closed
        assert service.applied_config["max_concurrency"] == 2
    finally:
        old.release.set()
        await asyncio.gather(*(task for task in (first, second) if task), return_exceptions=True)
        await service.close()


async def test_queue_timeout_does_not_send_another_paid_request():
    service = WebService(CONFIG, transport_factory=Transport)
    service.transport.release.clear()
    service.config["timeout_seconds"] = 0.05
    try:
        values = await asyncio.gather(
            *(service.dispatch("search", client, {"query": client}) for client in ("alice", "bob")),
            return_exceptions=True,
        )
        assert all(isinstance(value, RPCError) and value.code == "timeout" for value in values)
        assert len(service.transport.calls) == 1
        assert service.active_count == 0
    finally:
        await service.close()


async def test_reset_and_final_detach_cancel_requests_and_clear_pages():
    service = WebService(CONFIG, transport_factory=Transport)
    try:
        page = await service.dispatch("search", "alice", {"query": "first", "max_bytes": 4096})
        await service.drop_client("alice")
        with pytest.raises(RPCError, match="invalid or expired"):
            await service.dispatch("page", "alice", {"cursor": page["next_cursor"]})
        old = service.transport
        old.release.clear()
        old.entered.clear()
        request = asyncio.create_task(service.dispatch("search", "bob", {"query": "second"}))
        await asyncio.wait_for(old.entered.wait(), 2)
        await service.reset()
        assert request.cancelled()
        assert old.closed and service.active_count == 0
        assert not service.snapshots._snapshots
        assert service.transport is not old
    finally:
        await service.close()


async def test_default_selection_does_not_fall_back_for_unsupported_operations():
    service = WebService(CONFIG, transport_factory=Transport)
    try:
        with pytest.raises((RPCError, ValueError)):
            await service.dispatch("context", "alice", {"query": "test"})
        with pytest.raises((RPCError, ValueError)):
            await service.dispatch(
                "search", "alice", {"query": "test", "options": {"api_key": "secret"}}
            )
        assert not service.transport.calls
    finally:
        await service.close()


async def test_log_failure_does_not_discard_an_already_fetched_response():
    async def record(method, fields):
        raise OSError("history unavailable")

    service = WebService(CONFIG, record, transport_factory=Transport)
    try:
        page = await service.dispatch("search", "alice", {"query": "test"})
        assert page["results"]
        assert len(service.transport.calls) == 1
        assert service.status()["last_log_succeeded"] is False
        assert service.active_count == 0
    finally:
        await service.close()
