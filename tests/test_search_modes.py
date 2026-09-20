from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import pytest
from test_search import ShellRunner

from mypr_mcp.search import Search
from mypr_mcp.search_results import Results


async def test_files_counts_exists_and_literal_pattern_list(tmp_path):
    (tmp_path / "odd\nname.txt").write_text("foo foo\nbar\nfood\n")
    search = Search(tmp_path, ShellRunner())
    counts = await search.search(
        ["foo", "bar"], fixed=True, word=True, paths="odd\nname.txt", mode="counts"
    )
    assert counts["counts"] == [{"path": "odd\nname.txt", "count": 3}]
    assert counts["complete"]
    files = await search.search("foo", mode="files")
    assert files["files"] == ["./odd\nname.txt"]
    assert (await search.search("foo", mode="exists"))["exists"] is True
    assert (await search.search("missing", mode="exists"))["exists"] is False


async def test_multiline_utf8_ranges_and_every_submatch(tmp_path):
    (tmp_path / "text.txt").write_text("é foo foo\nnext\n")
    search = Search(tmp_path, ShellRunner())
    result = await search.search("foo", paths="text.txt")
    match = result["matches"][0]
    assert [part["range"]["start"]["byte"] for part in match["submatches"]] == [3, 7]
    result = await search.search("foo.*next", paths="text.txt", multiline=True, dotall=True)
    value = result["matches"][0]["range"]
    assert value["start"] == {"line": 1, "column": 4, "byte": 3}
    assert value["end"] == {"line": 2, "column": 5, "byte": 15}


async def test_all_file_and_count_pages_honor_record_budget(tmp_path):
    for index in range(4):
        (tmp_path / f"{index}.txt").write_text("hello\n")
    search = Search(tmp_path, ShellRunner())
    for mode in ("files", "counts"):
        first = await search.search("hello", mode=mode, max_matches=1)
        assert len(first[mode]) == 1
        assert first["has_more"] and first["complete"] and not first["scan_truncated"]
        page = await search.search(cursor=first["next_cursor"], max_matches=10)
        assert len(page[mode]) == 3


async def test_scan_limit_and_cursor_retry_are_separate_from_page_limit(tmp_path):
    (tmp_path / "text.txt").write_text("needle\n" * 20)
    search = Search(tmp_path, ShellRunner())
    result = await search.search("needle", scan_limit=3, max_matches=1)
    assert not result["complete"] and result["stop_reason"] == "scan_limit"
    assert result["has_more"]
    page = await search.search(cursor=result["page_cursor"], max_matches=20)
    assert len(page["matches"]) == 3
    with pytest.raises(ValueError, match="different search backend"):
        await search.search(backend="ast", cursor=result["next_cursor"])


async def test_legacy_cursor_and_non_utf8_path_snapshot(tmp_path):
    search = Search(tmp_path, ShellRunner())
    item = {"kind": "match", "path": "bad\udcff.txt", "line": 1, "column": 1, "text": "x"}
    ident = search.snapshots.create({}, [item], kind="matches")
    page = await search.search(cursor=search.snapshots.cursor(ident, 0, "matches"))
    assert page["matches"][0]["path"] == "bad\udcff.txt"
    assert page["backend"] == "rg"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"dotall": True},
        {"word": True, "line": True},
        {"context": 1, "before": 2},
        {"scan_bytes": 0},
        {"scan_limit": True},
        {"timeout": float("nan")},
        {"fixed": 1},
    ],
)
async def test_invalid_options_fail_before_launch(tmp_path, kwargs):
    class Runner:
        async def run(self, *args, **kw):
            raise AssertionError("unexpected launch")

    with pytest.raises((ValueError, TypeError)):
        await Search(tmp_path, Runner()).search("x", **kwargs)


async def test_workspace_slots_are_shared_and_wait_time_uses_deadline(tmp_path, monkeypatch):
    runtime = SimpleNamespace(search_slots=asyncio.Semaphore(2))
    active = peak = 0

    class Runner:
        async def stream(self, *args, **kwargs):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0.1)
                return {"returncode": 1}
            finally:
                active -= 1

    runner = Runner()
    runner.runtime = runtime
    monkeypatch.setattr(Search, "_build", lambda *args: ["fake"])
    results = await asyncio.gather(
        *[Search(tmp_path, runner).search("x", timeout=0.03) for _ in range(4)]
    )
    assert peak == 2
    assert sum(value["stop_reason"] == "timeout" for value in results) == 2


def test_parser_handles_split_records_without_promoting_partial_tail():
    collector = Results("rg", "matches", parse_match=Search._match_item)
    raw = (
        json.dumps(
            {
                "type": "match",
                "data": {
                    "path": {"text": "a.py"},
                    "lines": {"text": "x\n"},
                    "line_number": 1,
                    "absolute_offset": 0,
                    "submatches": [{"start": 0, "end": 1, "match": {"text": "x"}}],
                },
            }
        )
        + "\n"
    )
    for chunk in [raw[:30], raw[30:], '{"type":']:
        collector.feed(chunk)
    assert len(collector.finish()) == 1
    assert collector.invalid


async def test_document_conversion_failure_without_hits_is_not_a_negative_result(
    tmp_path, monkeypatch
):
    class Runner:
        async def run(self, *args, **kwargs):
            return {"returncode": 2, "stderr": "pdftotext: failed to read damaged document"}

    monkeypatch.setattr(Search, "_build", lambda *args: ["fake"])
    result = await Search(tmp_path, Runner()).search("x", backend="rga", mode="exists")
    assert result["exists"] is None
    assert not result["complete"]
    assert result["warnings"]


async def test_cursor_cannot_change_mode(tmp_path):
    (tmp_path / "a.txt").write_text("x\nx\n")
    search = Search(tmp_path, ShellRunner())
    first = await search.search("x", max_matches=1)
    with pytest.raises(ValueError, match="mode"):
        await search.search(cursor=first["next_cursor"], mode="files")


async def test_count_scan_limit_caps_occurrences_even_on_one_line(tmp_path):
    (tmp_path / "a.txt").write_text("x x x x x\n")
    result = await Search(tmp_path, ShellRunner()).search("x", mode="counts", scan_limit=2)
    assert result["counts"] == [{"path": "./a.txt", "count": 2}]
    assert result["stop_reason"] == "scan_limit"
    assert not result["complete"]


async def test_cancelled_document_search_removes_temporary_cache_and_releases_slot(
    tmp_path, monkeypatch
):
    started = asyncio.Event()

    class Runner:
        runtime = SimpleNamespace(search_slots=asyncio.Semaphore(2))

        async def stream(self, *args, **kwargs):
            started.set()
            await asyncio.Event().wait()

    runner = Runner()
    monkeypatch.setattr(Search, "_build", lambda *args: ["fake"])
    task = asyncio.create_task(Search(tmp_path, runner).search("x", backend="rga", cache=False))
    await started.wait()
    assert list((tmp_path / ".mypr" / "searches").glob(".rga-*"))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not list((tmp_path / ".mypr" / "searches").glob(".rga-*"))
    assert runner.runtime.search_slots._value == 2


async def test_cursor_rejects_changed_filters(tmp_path):
    (tmp_path / "a.txt").write_text("x\nx\n")
    search = Search(tmp_path, ShellRunner())
    first = await search.search("x", max_matches=1)
    with pytest.raises(ValueError, match="cursor accepts"):
        await search.search(cursor=first["next_cursor"], ignore_case=True)


@pytest.mark.parametrize("options", [{"context": 1}, {"before": 2}, {"after": 0}])
async def test_ast_rejects_unimplemented_context_options(tmp_path, options):
    with pytest.raises(ValueError, match="context options"):
        await Search(tmp_path, ShellRunner()).search(
            "print($A)", backend="ast", lang="python", **options
        )


def test_dense_multiline_spans_keep_byte_columns_at_newline_boundaries():
    raw = ("éx\n" * 1000).encode()
    submatches = [
        {"start": i, "end": i + 2, "match": {"text": "x\n"}} for i in range(2, len(raw), 4)
    ]
    record = {
        "type": "match",
        "data": {
            "path": {"text": "a.txt"},
            "lines": {"text": raw.decode()},
            "line_number": 4,
            "absolute_offset": 100,
            "submatches": submatches,
        },
    }
    match = Search._match_item(record)
    assert len(match["submatches"]) == 1000
    for row, span in enumerate(match["submatches"]):
        assert span["range"]["start"] == {"line": 4 + row, "column": 3, "byte": 102 + row * 4}
        assert span["range"]["end"] == {"line": 5 + row, "column": 1, "byte": 104 + row * 4}
