import pytest

from mypr_mcp.filesystem import Filesystem


@pytest.mark.parametrize("text", ["", "abc\n", "🌏\nx\n", "one\r\ntwo\r\nend", "αβγδε\n🌏last\n"])
@pytest.mark.parametrize("limit", [4, 5, 7, 12])
async def test_cursor_roundtrip_without_missing_or_duplicate_bytes(tmp_path, text, limit):
    path = tmp_path / "text"
    path.write_bytes(text.encode())
    fs = Filesystem(tmp_path)
    kwargs = {}
    output = ""
    cursors = set()
    for _ in range(40):
        page = await fs.read("text", max_bytes=limit, **kwargs)
        output += page["text"]
        assert len(page["text"].encode()) <= limit
        if not page["truncated"]:
            break
        cursor = page["next_cursor"]
        key = (cursor["line"], cursor["byte"])
        assert key not in cursors
        cursors.add(key)
        kwargs = {"start_line": cursor["line"], "start_byte": cursor["byte"]}
    else:
        pytest.fail("read cursor did not finish")
    assert output == text


async def test_utf8_and_newline_across_read_chunk_boundary(tmp_path):
    text = "a" * 65534 + "🌏\nsecond\n"
    (tmp_path / "text").write_bytes(text.encode())
    fs = Filesystem(tmp_path)
    tail = await fs.read("text", start_line=1, start_byte=65534, max_bytes=32)
    assert tail["text"] == "🌏\nsecond\n"
    second = await fs.read("text", start_line=2, end_line=2)
    assert second["text"] == "second\n"
    assert second["end_line"] == 2


async def test_invalid_edit_flags_cannot_overwrite_existing_content(tmp_path):
    path = tmp_path / "text"
    path.write_text("before before")
    fs = Filesystem(tmp_path)
    with pytest.raises(TypeError, match="booleans"):
        await fs.write("text", "after", overwrite="false")
    with pytest.raises(TypeError, match="boolean"):
        await fs.patch("text", [{"old": "before", "new": "after", "count": "all"}], dry_run="false")
    with pytest.raises(ValueError, match="count"):
        await fs.patch("text", [{"old": "before", "new": "after", "count": -1.0}])
    assert path.read_text() == "before before"
