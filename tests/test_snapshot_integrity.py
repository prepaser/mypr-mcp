from __future__ import annotations

import os
import subprocess
import sys

import pytest

from mypr_mcp import snapshots
from mypr_mcp.snapshots import SnapshotStore


def test_snapshot_fifo_is_rejected_without_waiting_for_a_writer(tmp_path):
    store = SnapshotStore(tmp_path, name="snapshots")
    ident = "a" * 32
    os.mkfifo(store.root / f"{ident}.json")
    code = (
        "from pathlib import Path\n"
        "from mypr_mcp.snapshots import SnapshotStore\n"
        "import sys\n"
        "SnapshotStore(Path(sys.argv[1]), name='snapshots').load('a' * 32)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code, str(tmp_path)], capture_output=True, text=True, timeout=3
    )
    assert result.returncode != 0
    assert "invalid persisted snapshot" in result.stderr


def test_snapshot_symlink_is_not_followed(tmp_path):
    store = SnapshotStore(tmp_path, name="snapshots")
    ident = store.create({}, ["valid"])
    target = store.root / f"{ident}.json"
    alternate = tmp_path / "alternate.json"
    target.rename(alternate)
    target.symlink_to(alternate)
    with pytest.raises(OSError):
        store.load(ident)


def test_snapshot_write_and_read_share_a_size_limit(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path, name="snapshots")
    monkeypatch.setattr(snapshots, "_MAX_SNAPSHOT_BYTES", 256)
    ident = store.create({}, ["normal"])
    assert store.load(ident)["items"] == ["normal"]
    with pytest.raises(ValueError, match="size limit"):
        store.create({}, ["é" * 100])
    assert list(store.root.iterdir()) == [store.root / f"{ident}.json"]

    (store.root / f"{ident}.json").write_bytes(b" " * 257)
    with pytest.raises(RuntimeError, match="size"):
        store.load(ident)


def test_snapshot_detects_a_file_change_during_read(tmp_path, monkeypatch):
    store = SnapshotStore(tmp_path, name="snapshots")
    ident = store.create({}, ["normal"])
    target = store.root / f"{ident}.json"
    fstat = snapshots.os.fstat
    calls = 0

    def mutate_after_read(fd):
        nonlocal calls
        calls += 1
        if calls == 2:
            with target.open("ab") as file:
                file.write(b" ")
        return fstat(fd)

    monkeypatch.setattr(snapshots.os, "fstat", mutate_after_read)
    with pytest.raises(RuntimeError, match="changed while reading"):
        store.load(ident)


def test_snapshot_decode_bounds_the_cursor_before_decoding(tmp_path):
    store = SnapshotStore(tmp_path, name="snapshots")
    with pytest.raises(ValueError, match="invalid snapshot cursor"):
        store.decode("a" * (snapshots._MAX_CURSOR_BYTES + 1))
