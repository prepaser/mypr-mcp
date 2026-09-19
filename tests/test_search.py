from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path

import pytest

from mypr_mcp.search import Search


class ShellRunner:
    async def run(self, command, *, cwd, check, max_bytes):
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()
        combined = stdout + stderr
        truncated = len(combined) > max_bytes
        kept = combined[:max_bytes]
        stdout_size = min(len(stdout), len(kept))
        return {
            "returncode": process.returncode,
            "stdout": kept[:stdout_size].decode("utf-8", "ignore"),
            "stderr": kept[stdout_size:].decode("utf-8", "ignore"),
            "truncated": truncated,
            "timed_out": False,
        }


async def _search(workspace: Path, **kwargs):
    return await Search(workspace, ShellRunner()).search(**kwargs)


@pytest.mark.asyncio
async def test_search_returns_match_metadata_context_and_fixed_patterns(tmp_path: Path):
    path = tmp_path / "odd [name].txt"
    path.write_text("before\nNeedle value\nafter\n", encoding="utf-8")

    result = await _search(
        tmp_path,
        pattern="needle value",
        paths="odd [name].txt",
        fixed=True,
        ignore_case=True,
        context=1,
    )

    assert [item["kind"] for item in result["matches"]] == ["context", "match", "context"]
    match = result["matches"][1]
    assert match["path"].endswith("odd [name].txt")
    assert match["line"] == 2
    assert match["column"] == 1
    assert match["text"] == "Needle value"
    assert result["truncated"] is False


@pytest.mark.asyncio
async def test_search_pages_count_matches_and_keep_trailing_context(tmp_path: Path):
    (tmp_path / "context.txt").write_text(
        "before\nneedle one\nbetween\nneedle two\nafter\n", encoding="utf-8"
    )

    search = Search(tmp_path, ShellRunner())
    first = await search.search("needle", context=1, max_matches=1, max_bytes=4096)

    assert [item["kind"] for item in first["matches"]] == ["context", "match", "context"]
    assert first["has_more"] is True

    second = await search.search(cursor=first["next_cursor"], max_bytes=4096)
    assert [item["kind"] for item in second["matches"]] == ["match", "context"]
    assert second["has_more"] is False


@pytest.mark.asyncio
async def test_file_listing_respects_ignore_and_hidden_options(tmp_path: Path):
    (tmp_path / ".gitignore").write_text("ignored.txt\n", encoding="utf-8")
    (tmp_path / "visible.txt").write_text("visible", encoding="utf-8")
    (tmp_path / "ignored.txt").write_text("ignored", encoding="utf-8")
    (tmp_path / ".hidden.txt").write_text("hidden", encoding="utf-8")

    normal = await _search(tmp_path)
    all_files = await _search(tmp_path, hidden=True, no_ignore=True)

    assert any(item.endswith("visible.txt") for item in normal["files"])
    assert not any(item.endswith("ignored.txt") for item in normal["files"])
    assert not any(item.endswith(".hidden.txt") for item in normal["files"])
    assert any(item.endswith("ignored.txt") for item in all_files["files"])
    assert any(item.endswith(".hidden.txt") for item in all_files["files"])


@pytest.mark.asyncio
async def test_search_no_match_is_empty_and_invalid_regex_is_actionable(tmp_path: Path):
    (tmp_path / "file.txt").write_text("hello\n", encoding="utf-8")

    result = await _search(tmp_path, pattern="missing")
    assert result["matches"] == []
    assert result["truncated"] is False

    with pytest.raises(RuntimeError, match="ripgrep failed:.*regex parse error"):
        await _search(tmp_path, pattern="[")


@pytest.mark.asyncio
async def test_search_cancels_at_match_and_byte_bounds(tmp_path: Path):
    (tmp_path / "many.txt").write_text("needle é\n" * 1000, encoding="utf-8")

    result = await _search(tmp_path, pattern="needle", max_matches=2, max_bytes=4096)
    assert sum(item["kind"] == "match" for item in result["matches"]) <= 2
    assert result["truncated"] is True

    result = await _search(tmp_path, pattern="needle", max_bytes=128)
    assert result["truncated"] is True
    assert len(json.dumps(result, ensure_ascii=False).encode("utf-8")) < 4096


def test_search_parsers_drop_clipped_file_tail_and_keep_json_text_separators():
    files, truncated = Search._parse_files("one.py\0partial", 10, 1024)
    assert files == ["one.py"]
    assert truncated is True

    record = {
        "type": "match",
        "data": {
            "path": {"text": "one.py"},
            "lines": {"text": "before\u2028needle\n"},
            "line_number": 1,
            "submatches": [{"start": 7}],
        },
    }
    matches, truncated = Search._parse_matches(json.dumps(record) + "\n", 10, 1024)
    assert matches[0]["text"] == "before\u2028needle"
    assert truncated is False

    encoded_path = base64.b64encode(b"bad\xff.py").decode()
    binary_record = {
        "type": "match",
        "data": {
            "path": {"bytes": encoded_path},
            "lines": {"text": "needle\n"},
            "line_number": 1,
            "submatches": [{"start": 0}],
        },
    }
    matches, _ = Search._parse_matches(json.dumps(binary_record) + "\n", 10, 1024)
    assert matches[0]["path"].endswith(".py")


@pytest.mark.asyncio
async def test_search_reports_missing_ripgrep(monkeypatch, tmp_path: Path):
    monkeypatch.setattr("mypr_mcp.search.shutil.which", lambda _: None)
    with pytest.raises(RuntimeError, match="ripgrep .*required"):
        await _search(tmp_path, pattern="anything")


@pytest.mark.asyncio
async def test_search_pages_from_persisted_snapshot_without_rerunning(tmp_path: Path):
    (tmp_path / "many.txt").write_text("needle\n" * 20, encoding="utf-8")
    runner = ShellRunner()
    search = Search(tmp_path, runner)

    first = await search.search(pattern="needle", max_matches=20, max_bytes=512)
    assert first["has_more"] is True
    first_matches = first["matches"]
    cursor = first["next_cursor"]
    assert cursor

    second = await search.search(cursor=cursor, max_bytes=512)
    assert second["matches"]
    all_matches = first_matches + second["matches"]
    assert (tmp_path / ".mypr" / "searches").is_dir()

    # A new Search object can continue the same persisted query after a
    # manager/kernel restart without invoking ripgrep again.
    restarted = Search(tmp_path, runner)
    final = second
    while final["next_cursor"]:
        final = await restarted.search(cursor=final["next_cursor"], max_bytes=512)
        all_matches.extend(final["matches"])
    assert final["has_more"] is False
    assert len(all_matches) == 20
