import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from mypr_mcp.journal import append_events, read_page


class CountedFile:
    def __init__(self, file, reads):
        self.file = file
        self.reads = reads

    def __getattr__(self, name):
        return getattr(self.file, name)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return self.file.__exit__(*args)

    def readline(self, *args):
        value = self.file.readline(*args)
        self.reads.append(len(value))
        return value


def count_reads(monkeypatch, path):
    reads = []
    original = Path.open

    def open_file(self, *args, **kwargs):
        file = original(self, *args, **kwargs)
        if self == path and args and args[0] == "rb":
            return CountedFile(file, reads)
        return file

    monkeypatch.setattr(Path, "open", open_file)
    return reads


def test_pages_read_only_requested_region(monkeypatch, tmp_path):
    path = tmp_path / "events.jsonl"
    events = [{"type": "stream", "text": "x" * 1024} for _ in range(1024)]
    append_events(path, events)
    reads = count_reads(monkeypatch, path)
    first, total = read_page(path, 0, 32768, 4)
    second, _ = read_page(path, len(first), 32768, 4)
    assert total == len(events)
    assert first + second == events[: len(first) + len(second)]
    assert 0 < sum(reads) < 3 * 32768
    assert path.stat().st_size > 1024 * 1024


def test_legacy_index_is_built_once_and_supports_arbitrary_cursors(monkeypatch, tmp_path):
    path = tmp_path / "legacy.jsonl"
    events = [{"text": f"{number}:안녕 👋" * 50} for number in range(100)]
    path.write_text("".join(json.dumps(event, ensure_ascii=False) + "\n" for event in events))
    reads = count_reads(monkeypatch, path)
    page, total = read_page(path, 90, 1024)
    assert page == events[90 : 90 + len(page)]
    assert total == 100
    assert sum(reads) >= path.stat().st_size
    reads.clear()
    assert read_page(path, 3, 1024)[0][0] == events[3]
    assert sum(reads) < path.stat().st_size // 10
    assert read_page(path, 100, 1024) == ([], 100)


def test_concurrent_legacy_reads_share_one_index_build(monkeypatch, tmp_path):
    path = tmp_path / "legacy.jsonl"
    path.write_text((json.dumps({"text": "x" * 1024}) + "\n") * 1024)
    reads = count_reads(monkeypatch, path)
    barrier = threading.Barrier(4)

    def page(cursor):
        barrier.wait(timeout=5)
        return read_page(path, cursor, 4096)

    with ThreadPoolExecutor(max_workers=4) as pool:
        pages = list(pool.map(page, [0, 128, 512, 800]))
    assert all(total == 1024 and items for items, total in pages)
    assert sum(reads) < path.stat().st_size + 4 * 8192


def test_append_and_stale_or_corrupt_index_rebuild(tmp_path):
    path = tmp_path / "events.jsonl"
    append_events(path, [{"text": "one"}])
    append_events(path, [{"text": "two"}])
    assert read_page(path, 1, 1024) == ([{"text": "two"}], 2)
    with path.open("a") as file:
        file.write(json.dumps({"text": "three"}) + "\n")
    assert read_page(path, 2, 1024) == ([{"text": "three"}], 3)
    path.with_suffix(".idx").write_bytes(b"broken")
    assert read_page(path, 0, 1024)[1] == 3


@pytest.mark.parametrize("cursor", [-1, True, 1.5, "1", 2])
def test_invalid_cursor(tmp_path, cursor):
    path = tmp_path / "events.jsonl"
    append_events(path, [{"text": "one"}])
    with pytest.raises(ValueError, match="cursor"):
        read_page(path, cursor, 1024)


def test_empty_journal(tmp_path):
    path = tmp_path / "missing.jsonl"
    assert read_page(path, 0, 1024) == ([], 0)
    path.touch()
    assert read_page(path, 0, 1024) == ([], 0)


@pytest.mark.parametrize("tail", [b'{"text":"cut', b'{"text":"\xf0\x9f'])
def test_incomplete_tail_preserves_prefix_and_cursor(tmp_path, tail):
    path = tmp_path / "events.jsonl"
    original = b'{"text":"saved"}\n' + tail
    path.write_bytes(original)
    page, total = read_page(path, 0, 4096)
    assert page[0] == {"text": "saved"}
    assert page[1]["code"] == "journal_truncated"
    assert page[1]["line"] == 2
    assert total == 2
    assert read_page(path, 1, 4096) == (page[1:], 2)
    assert read_page(path, 2, 4096) == ([], 2)
    assert path.read_bytes() == original


@pytest.mark.parametrize("damaged", [b"broken\n", b"[]\n", b"\xff\n"])
def test_corrupt_record_preserves_later_output_and_pagination(tmp_path, damaged):
    path = tmp_path / "events.jsonl"
    original = b'{"text":"before"}\n' + damaged + b'{"text":"after"}\n'
    path.write_bytes(original)
    events, cursor = [], 0
    while cursor < 3:
        page, total = read_page(path, cursor, 1)
        assert total == 3
        assert len(page) == 1
        events.extend(page)
        cursor += len(page)
    assert events[0] == {"text": "before"}
    assert events[1]["code"] == "journal_corrupt"
    assert events[2] == {"text": "after"}
    assert path.read_bytes() == original


def test_complete_json_without_newline_is_readable(tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_bytes(b'{"text":"complete"}')
    assert read_page(path, 0, 1024) == ([{"text": "complete"}], 1)
