from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
from pathlib import Path

import pytest
from conftest import execute, mcp_session, result_text


async def value(session, code):
    result = await execute(session, code)
    assert result["state"] == "succeeded", result.get("error") or result_text(result)
    return json.loads(result_text(result))


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep required")
async def test_manager_query_snapshots_survive_kernel_reset(workspace: Path):
    await asyncio.to_thread(subprocess.run, ["git", "init", "-q", str(workspace)], check=True)
    await asyncio.to_thread(
        subprocess.run,
        ["git", "-C", str(workspace), "config", "user.email", "test@example.com"],
        check=True,
    )
    await asyncio.to_thread(
        subprocess.run, ["git", "-C", str(workspace), "config", "user.name", "Test"], check=True
    )
    (workspace / ".gitignore").write_text(".mypr/\n")
    (workspace / "data.txt").write_text("".join(f"match {i}\n" for i in range(30)))
    await asyncio.to_thread(subprocess.run, ["git", "-C", str(workspace), "add", "."], check=True)
    await asyncio.to_thread(
        subprocess.run, ["git", "-C", str(workspace), "commit", "-qm", "fixture"], check=True
    )
    async with mcp_session(workspace) as session:
        search = await value(
            session,
            "import json\n"
            'print(json.dumps(await ws.fs.search("match", paths="data.txt", max_matches=3)))',
        )
        assert len(search["matches"]) == 3
        assert search["has_more"]
        show = await value(
            session, 'print(json.dumps(await ws.git.show(path="data.txt", max_bytes=1024)))'
        )
        assert show["text"].startswith("match 0")
        (workspace / "data.txt").write_text("replacement\n")
        for i in range(5):
            (workspace / f"untracked-{i}.txt").write_text("new\n")
        status = await value(session, "print(json.dumps(await ws.git.status(max_entries=2)))")
        assert len(status["files"]) == 2
        assert status["has_more"]
        diff = await value(session, "print(json.dumps(await ws.git.diff(max_bytes=1024)))")
        assert "replacement" in diff["patch"]
        (workspace / "large.txt").write_text("a" * (5 * 1024 * 1024) + " needle-end\n")
        large = await value(
            session,
            'page = await ws.fs.search("needle-end", paths="large.txt", max_bytes=6*1024*1024)\n'
            'print(json.dumps({"length": len(page["matches"][0]["text"]), '
            '"tail": page["matches"][0]["text"][-11:], "truncated": page["truncated"]}))',
        )
        assert large["length"] > 5 * 1024 * 1024
        assert "needle-end" in large["tail"]
        assert not large["truncated"]
        reset = await execute(session, "await ws.reset()", wait_ms=15000)
        assert reset["state"] == "succeeded", reset
        after = await value(
            session,
            f"import json\nprint(json.dumps(await ws.fs.search(cursor={search['next_cursor']!r}, "
            "max_matches=3)))",
        )
        assert [m["text"].strip() for m in after["matches"]] == ["match 3", "match 4", "match 5"]
        next_status = await value(
            session,
            f"print(json.dumps(await ws.git.status(cursor={status['next_cursor']!r}, "
            "max_entries=2)))",
        )
        assert len(next_status["files"]) == 2
        assert next_status["files"] != status["files"]
