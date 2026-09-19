from __future__ import annotations

import json
import subprocess

from conftest import execute, mcp_session, result_text, stop_manager


def _git(workspace, *args: str) -> None:
    subprocess.run(
        ["git", *args],
        cwd=workspace,
        check=True,
        capture_output=True,
        text=True,
    )


async def test_search_and_git_snapshots_continue_after_manager_restart(workspace):
    _git(workspace, "init", "-q")
    _git(workspace, "config", "user.email", "test@example.com")
    _git(workspace, "config", "user.name", "Test")
    tracked = workspace / "tracked.txt"
    tracked.write_text("".join(f"base {index}\n" for index in range(80)), encoding="utf-8")
    _git(workspace, "add", "tracked.txt")
    _git(workspace, "commit", "-qm", "initial")
    tracked.write_text("".join(f"changed {index}\n" for index in range(80)), encoding="utf-8")
    searchable = workspace / "searchable.txt"
    searchable.write_text("".join(f"needle {index}\n" for index in range(80)), encoding="utf-8")

    async with mcp_session(workspace) as session:
        first = await execute(
            session,
            "import json\n"
            "search = await ws.fs.search('needle', paths='searchable.txt', max_bytes=256)\n"
            "diff = await ws.git.diff(max_bytes=1024)\n"
            "print(json.dumps({'search': search, 'diff': diff}))",
        )
        payload = json.loads(result_text(first))

    search_first = payload["search"]
    diff_first = payload["diff"]
    assert search_first["snapshot_id"]
    assert search_first["next_cursor"]
    assert search_first["matches"]
    assert diff_first["snapshot_id"]
    assert diff_first["next_cursor"]
    assert "base 0" in diff_first["patch"]

    await stop_manager(workspace)
    searchable.unlink()
    tracked.unlink()

    async with mcp_session(workspace) as restarted:
        continued = await execute(
            restarted,
            "import json\n"
            f"search = await ws.fs.search(cursor={search_first['next_cursor']!r}, max_bytes=256)\n"
            f"diff = await ws.git.diff(cursor={diff_first['next_cursor']!r}, max_bytes=1024)\n"
            "search_pages = [search]\n"
            "while search_pages[-1]['next_cursor']:\n"
            "    search_pages.append(\n"
            "        await ws.fs.search(cursor=search_pages[-1]['next_cursor'], max_bytes=256)\n"
            "    )\n"
            "diff_pages = [diff]\n"
            "while diff_pages[-1]['next_cursor']:\n"
            "    diff_pages.append(\n"
            "        await ws.git.diff(cursor=diff_pages[-1]['next_cursor'], max_bytes=1024)\n"
            "    )\n"
            "print(json.dumps({'search': search_pages, 'diff': diff_pages}))",
        )
        pages = json.loads(result_text(continued))

    search_pages = pages["search"]
    diff_pages = pages["diff"]
    assert all(page["snapshot_id"] == search_first["snapshot_id"] for page in search_pages)
    assert all(page["matches"] for page in search_pages)
    assert all(page["patch"] for page in diff_pages)
    assert search_pages[-1]["has_more"] is False
    assert diff_pages[-1]["has_more"] is False
    assert sum(len(page["matches"]) for page in search_pages) + len(search_first["matches"]) >= 80
    assert "changed 79" in "".join(page["patch"] for page in diff_pages)
