import asyncio
import json
import os
import shutil

import pytest

from mypr_mcp.filesystem import Filesystem
from mypr_mcp.git_api import Git
from mypr_mcp.json_utils import json_bytes
from mypr_mcp.kernel_api import Skills
from mypr_mcp.managed_commands import ManagedCommands
from mypr_mcp.search import Search
from mypr_mcp.services import Shells


class Runtime:
    def __init__(self, workspace, shells):
        self.workspace = workspace
        self.shells = shells
        self.search_slots = asyncio.Semaphore(2)

    def track_shell(self, *args, **kwargs):
        pass


@pytest.mark.parametrize("text", ["€Z", "\ufffdZ"])
@pytest.mark.parametrize("budget", [1, 2, 3])
async def test_managed_output_clips_utf8_without_inventing_replacements(tmp_path, text, budget):
    shells = Shells(tmp_path)
    command = ManagedCommands(Runtime(tmp_path, shells), "c", "conn", "exec")
    expected = text.encode()[:budget].decode("utf-8", "ignore")
    try:
        result = await command.run(["printf", text], max_bytes=budget)
        assert result["stdout"] == expected
        assert len(result["stdout"].encode()) <= budget
        assert result["truncated"] is True
        chunks = []

        async def collect(chunk):
            chunks.append(chunk)

        streamed = await command.stream(["printf", text], max_bytes=budget, on_stdout=collect)
        assert "".join(chunks) == expected
        assert len("".join(chunks).encode()) <= budget
        assert streamed["truncated"] is True
    finally:
        await shells.close()


async def test_non_utf8_paths_survive_git_search_and_json(tmp_path):
    if shutil.which("rg") is None:
        pytest.skip("rg is unavailable")
    name = os.fsdecode(b"bad-\xff.txt")
    (tmp_path / name).write_text("needle\n")
    process = await asyncio.create_subprocess_exec("git", "init", "-q", str(tmp_path))
    assert await process.wait() == 0
    shells = Shells(tmp_path)
    command = ManagedCommands(Runtime(tmp_path, shells), "client", "connection", "exec")
    try:
        status = await Git(tmp_path, command).status()
        assert status["files"][0]["path"] == name
        search = Search(tmp_path, command)
        files = await search.search(mode="files", glob="bad-*")
        matches = await search.search("needle", glob="bad-*")
        counts = await search.search("needle", mode="counts", glob="bad-*")
        assert files["files"] == [f"./{name}"]
        assert matches["matches"][0]["path"] == f"./{name}"
        assert counts["counts"][0]["path"] == f"./{name}"
        assert json.loads(json_bytes(matches)) == matches
        fs = Filesystem(tmp_path)
        source = await fs.read(matches["matches"][0]["path"])
        saved = await fs.write(name, "changed\n", expected_hash=source["revision"])
        history = await fs.history(name)
        assert saved["history_recorded"] is True
        assert history["items"]
        assert (tmp_path / name).read_text() == "changed\n"
    finally:
        await shells.close()


def test_shell_byte_cursor_preserves_invalid_bytes_and_utf8_boundaries():
    events = [{"stream": "stdout", "text": "é\udcff\udc80x"}]
    first, index, offset = Shells._page_events(
        events, 0, 0, stream=None, max_bytes=3
    )
    assert first[0]["text"] == "é\udcff"
    second, index, offset = Shells._page_events(
        events, index, offset, stream=None, max_bytes=2
    )
    assert second[0]["text"] == "\udc80x"
    assert (index, offset) == (1, 0)
    with pytest.raises(ValueError, match="cursor"):
        Shells._page_events(events, 0, 1, stream=None, max_bytes=3)


async def test_skill_listing_preserves_non_utf8_directory_name(tmp_path):
    name = os.fsdecode(b"bad-\xff")
    root = tmp_path / ".mypr/skills" / name
    root.mkdir(parents=True)
    (root / "SKILL.md").write_text("# Instructions\n")
    items = await Skills(tmp_path).list()
    assert items[0]["name"] == name
    assert json.loads(json_bytes(items)) == items
