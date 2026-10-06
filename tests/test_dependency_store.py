from __future__ import annotations

import asyncio
import hashlib
import io
import json
import os
import tarfile
import threading
import time
import zipfile
from dataclasses import replace
from pathlib import Path

import pytest

from mypr_mcp.dependency_store import DependencyError, DependencyStore


@pytest.fixture(autouse=True)
def block_unexpected_artifact_resolution(monkeypatch):
    from mypr_mcp import dependency_store

    async def resolve(name: str, system: str | None = None):
        raise AssertionError(f"unexpected artifact resolution: {name} ({system})")

    monkeypatch.setattr(dependency_store, "resolve_artifact", resolve)


def _patch_resolver(monkeypatch, *artifacts):
    from mypr_mcp import dependency_store

    resolved = {artifact.name: artifact for artifact in artifacts}

    async def resolve(name: str, system: str | None = None):
        try:
            return resolved[name]
        except KeyError as exc:
            raise AssertionError(f"unexpected artifact resolution: {name} ({system})") from exc

    monkeypatch.setattr(dependency_store, "resolve_artifact", resolve)


def _fixture_artifact(name: str, *, version: str, url: str, sha256: str | None):
    from mypr_mcp import dependency_store

    return replace(
        dependency_store.CATALOG[name],
        version=version,
        url=url,
        sha256=sha256,
    )


def _archive(path: Path, kind: str, files: dict[str, bytes]) -> None:
    if kind == "zip":
        with zipfile.ZipFile(path, "w") as stream:
            for name, data in files.items():
                stream.writestr(name, data)
        return
    with tarfile.open(path, "w:gz") as stream:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            info.mode = 0o755
            stream.addfile(info, io.BytesIO(data))


def _fake_downloader(files: dict[str, bytes]):
    async def download(url: str, destination: Path, maximum: int) -> None:
        data = files[url]
        if len(data) > maximum:
            raise DependencyError("download exceeds limit")
        await asyncio.to_thread(destination.write_bytes, data)

    return download


@pytest.mark.asyncio
async def test_store_defaults_are_absolute_and_inspection_is_read_only(tmp_path, monkeypatch):
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "data"))
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "cache"))
    store = DependencyStore(platform_key="x86_64")
    assert store.data_root == (tmp_path / "data" / "mypr").resolve()
    assert store.cache_root == (tmp_path / "cache" / "mypr").resolve()
    state = await store.inspect("rg")
    assert state["status"] in {"installed", "missing", "unusable"}
    await store.list("binary")
    assert not (tmp_path / "data").exists()
    assert not (tmp_path / "cache").exists()
    await store.close()


@pytest.mark.asyncio
async def test_catalog_has_all_binary_and_model_names(tmp_path):
    store = DependencyStore(tmp_path / "data", tmp_path / "cache", platform_key="x86_64")
    assert store.names("binary") == ("rg", "ast-grep", "rga", "pandoc")
    assert "tessdata:eng" in store.names("model")
    assert "tessdata:kor" in store.names("model")
    assert len(store.names("model")) > 100
    assert (await store.inspect("does-not-exist"))["status"] == "unsupported"
    await store.close()


@pytest.mark.asyncio
async def test_binary_install_extracts_and_publishes_aliases(tmp_path, monkeypatch):
    archive = tmp_path / "rg.tar.gz"
    _archive(archive, "tar.gz", {"rg-15.2.0/rg": b"#!/bin/sh\necho rg 15.2.0\n"})
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    patched = _fixture_artifact("rg", version="15.2.0", url="fixture://rg", sha256=digest)
    _patch_resolver(monkeypatch, patched)

    async def download(_url: str, destination: Path, _maximum: int) -> None:
        data = await asyncio.to_thread(archive.read_bytes)
        await asyncio.to_thread(destination.write_bytes, data)

    store = DependencyStore(
        tmp_path / "data", tmp_path / "cache", downloader=download, platform_key="x86_64"
    )
    result = await store.install("rg")
    assert result["source"] == "shared"
    assert await asyncio.to_thread(Path(result["path"]).is_file)
    assert (store.bin_root / "rg").is_symlink()
    assert os.access(store.bin_root / "rg", os.X_OK)
    await store.close()


@pytest.mark.asyncio
async def test_model_install_verifies_hash_and_returns_model_dir(tmp_path, monkeypatch):
    data = b"fake traineddata"
    artifact = _fixture_artifact(
        "tessdata:eng",
        version="4.2.0",
        url="fixture://eng",
        sha256=hashlib.sha256(b"different traineddata").hexdigest(),
    )
    _patch_resolver(monkeypatch, artifact)

    # Keep this test independent of a network and the production digest by
    # exercising the bounded downloader and hash failure path directly.
    async def download(_url: str, destination: Path, _maximum: int) -> None:
        await asyncio.to_thread(destination.write_bytes, data)

    store = DependencyStore(
        tmp_path / "data", tmp_path / "cache", downloader=download, platform_key="x86_64"
    )
    with pytest.raises(DependencyError, match="SHA-256 mismatch"):
        await store.install("tessdata:eng")
    state = await store.inspect("tessdata:eng")
    assert state["status"] == "missing"
    assert Path(state["model_dir"]).name == "tessdata_fast"
    await store.close()


@pytest.mark.asyncio
async def test_concurrent_model_ensure_shares_one_download(tmp_path, monkeypatch):
    data = b"fake traineddata"
    digest = hashlib.sha256(data).hexdigest()
    artifact = _fixture_artifact(
        "tessdata:eng", version="4.2.0", url="fixture://eng", sha256=digest
    )
    _patch_resolver(monkeypatch, artifact)
    calls = 0

    async def download(_url: str, destination: Path, _maximum: int) -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.01)
        await asyncio.to_thread(destination.write_bytes, data)

    store = DependencyStore(
        tmp_path / "data", tmp_path / "cache", downloader=download, platform_key="x86_64"
    )
    results = await asyncio.gather(*(store.ensure("tessdata:eng") for _ in range(3)))
    assert calls == 1
    assert all(item["status"] == "installed" for item in results)
    assert results[0]["path"] == results[1]["path"]
    await store.close()


@pytest.mark.asyncio
async def test_model_git_digest_and_local_marker_detect_corruption(tmp_path, monkeypatch):
    data = b"model fixture"
    digest = hashlib.sha1(f"blob {len(data)}\0".encode() + data, usedforsecurity=False).hexdigest()
    artifact = replace(
        _fixture_artifact("tessdata:eng", version="4.2.0", url="fixture://eng", sha256=None),
        git_sha1=digest,
    )
    _patch_resolver(monkeypatch, replace(artifact, git_sha1="0" * 40))
    store = DependencyStore(
        tmp_path / "data",
        tmp_path / "cache",
        downloader=_fake_downloader({"fixture://eng": data}),
        platform_key="x86_64",
    )
    try:
        with pytest.raises(DependencyError, match="Git blob digest mismatch"):
            await store.ensure("tessdata:eng")
        assert (await store.inspect("tessdata:eng"))["status"] == "missing"
        _patch_resolver(monkeypatch, artifact)
        installed = await store.ensure("tessdata:eng")
        assert installed["version"] == "4.2.0"
        path = Path(installed["path"])
        await asyncio.to_thread(path.write_bytes, b"corrupt model")
        assert (await store.inspect("tessdata:eng"))["status"] == "unusable"
        repaired = await store.ensure("tessdata:eng")
        assert repaired["status"] == "installed"
        assert await asyncio.to_thread(path.read_bytes) == data
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_model_fifo_marker_is_rejected_without_blocking(tmp_path, monkeypatch):
    data = b"model fixture"
    artifact = _fixture_artifact(
        "tessdata:eng",
        version="4.2.0",
        url="fixture://eng",
        sha256=hashlib.sha256(data).hexdigest(),
    )
    _patch_resolver(monkeypatch, artifact)
    store = DependencyStore(tmp_path / "data", tmp_path / "cache", platform_key="x86_64")
    model_dir = store.model_root / "tessdata_fast-4.2.0-fixture"
    model_dir.mkdir(parents=True)
    (model_dir / "eng.traineddata").write_bytes(data)
    os.mkfifo(model_dir / ".eng.mypr-complete.json")
    try:
        state = await asyncio.wait_for(store.inspect("tessdata:eng"), 1)
        assert state["status"] == "unusable"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_corrupt_managed_binary_is_repaired_without_running_it(tmp_path, monkeypatch):
    archive = tmp_path / "rg.tar.gz"
    _archive(archive, "tar.gz", {"rg-15.2.0/rg": b"#!/bin/sh\necho rg 15.2.0\n"})
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    artifact = _fixture_artifact("rg", version="15.2.0", url="fixture://rg", sha256=digest)
    _patch_resolver(monkeypatch, artifact)

    async def download(_url: str, destination: Path, _maximum: int) -> None:
        await asyncio.to_thread(
            destination.write_bytes, await asyncio.to_thread(archive.read_bytes)
        )

    store = DependencyStore(
        tmp_path / "data", tmp_path / "cache", downloader=download, platform_key="x86_64"
    )
    monkeypatch.setattr(store, "_inspect_system", lambda _artifact: None)
    installed = await store.install("rg")
    executable = Path(installed["path"])
    await asyncio.to_thread(executable.write_bytes, b"#!/bin/sh\necho a poisoned version\n")
    state = await store.inspect("rg")
    assert state["status"] == "unusable"
    repaired = await store.ensure("rg")
    assert repaired["status"] == "installed"
    assert "rg 15.2.0" in await asyncio.to_thread(executable.read_text)
    await store.close()


def test_safe_extract_rejects_traversal(tmp_path):
    from mypr_mcp.dependency_store import _safe_extract

    archive = tmp_path / "bad.tar.gz"
    _archive(archive, "tar.gz", {"../escape": b"x"})
    with pytest.raises(DependencyError, match="unsafe path"):
        _safe_extract(archive, tmp_path / "out", "tar.gz")


def test_safe_extract_accepts_normal_zip_file_mode(tmp_path):
    from mypr_mcp.dependency_store import _find_executable, _safe_extract

    archive = tmp_path / "tool.zip"
    _archive(archive, "zip", {"tool/ast-grep": b"#!/bin/sh\necho 0.45.3\n"})
    target = tmp_path / "out"
    _safe_extract(archive, target, "zip")
    assert _find_executable(target, ("ast-grep",))["ast-grep"].is_file()


def test_publish_alias_updates_owned_link_and_preserves_unowned_entries(tmp_path):
    store = DependencyStore(tmp_path / "data", tmp_path / "cache", platform_key="x86_64")
    old = store._tools_root / "old" / "rg"
    new = store._tools_root / "new" / "rg"
    old.parent.mkdir(parents=True)
    new.parent.mkdir(parents=True)
    old.write_text("old")
    new.write_text("new")
    alias = store.bin_root / "rg"
    store.bin_root.mkdir(parents=True)
    alias.symlink_to(old)
    store._publish_alias(alias, new)
    assert alias.resolve() == new.resolve()

    regular = store.bin_root / "regular"
    regular.write_text("owned by user")
    store._publish_alias(regular, new)
    assert regular.read_text() == "owned by user"

    external = tmp_path / "external"
    external.write_text("external")
    linked = store.bin_root / "linked"
    linked.symlink_to(external)
    store._publish_alias(linked, new)
    assert linked.resolve() == external.resolve()


def test_safe_extract_ignores_unused_tar_links_but_rejects_executable_link(tmp_path):
    from mypr_mcp.dependency_store import _find_executable, _safe_extract

    archive = tmp_path / "links.tar.gz"
    with tarfile.open(archive, "w:gz") as stream:
        link = tarfile.TarInfo("tool/share/unused")
        link.type = tarfile.SYMTYPE
        link.linkname = "target"
        stream.addfile(link)
        executable = tarfile.TarInfo("tool/rg")
        executable.mode = 0o755
        executable.size = 3
        stream.addfile(executable, io.BytesIO(b"rg\n"))
    target = tmp_path / "out"
    _safe_extract(archive, target, "tar.gz", allowed_links=("rg",))
    assert _find_executable(target, ("rg",))["rg"].is_file()

    requested = tmp_path / "requested.tar.gz"
    with tarfile.open(requested, "w:gz") as stream:
        link = tarfile.TarInfo("tool/rg")
        link.type = tarfile.SYMTYPE
        link.linkname = "target"
        stream.addfile(link)
    with pytest.raises(DependencyError, match="requested executable"):
        _safe_extract(requested, tmp_path / "requested-out", "tar.gz", allowed_links=("rg",))


@pytest.mark.asyncio
async def test_valid_shared_tool_wins_over_incompatible_system_tool(tmp_path, monkeypatch):
    store = DependencyStore(tmp_path / "data", tmp_path / "cache", platform_key="x86_64")
    artifact = store._tools_root / "rg" / "15.2.0" / "x86_64"
    (artifact / "nested").mkdir(parents=True)
    executable = artifact / "nested" / "rg"
    await asyncio.to_thread(executable.write_text, "#!/bin/sh\necho rg 15.2.0\n")
    await asyncio.to_thread(executable.chmod, 0o755)
    import json

    metadata = {
        "name": "rg",
        "version": "15.2.0",
        "platform": "x86_64",
        "files": {
            "rg": {
                "path": "nested/rg",
                "sha256": hashlib.sha256(
                    await asyncio.to_thread(executable.read_bytes)
                ).hexdigest(),
            }
        },
    }
    await asyncio.to_thread((artifact / ".mypr-complete.json").write_text, json.dumps(metadata))
    monkeypatch.setattr(
        store,
        "_inspect_system",
        lambda _artifact: {
            "name": "rg",
            "kind": "binary",
            "scope": "global",
            "source": "system",
            "status": "unusable",
            "version": None,
            "path": "/usr/bin/rg",
        },
    )
    result = await store.inspect("rg")
    assert result["source"] == "shared"
    assert result["status"] == "installed"
    await store.close()


@pytest.mark.asyncio
async def test_system_binary_newer_than_minimum_is_reused(tmp_path, monkeypatch):
    from mypr_mcp import dependency_store

    store = DependencyStore(tmp_path / "data", tmp_path / "cache", platform_key="x86_64")
    monkeypatch.setattr(
        dependency_store.shutil,
        "which",
        lambda name: "/usr/bin/rg" if name == "rg" else None,
    )
    monkeypatch.setattr(dependency_store, "_run_version", lambda _path: "ripgrep 15.2.0")
    result = await store.inspect("rg")
    assert result["source"] == "system"
    assert result["status"] == "installed"
    assert result["version"] == "15.2.0"
    await store.close()


@pytest.mark.asyncio
async def test_malformed_managed_metadata_is_unusable(tmp_path, monkeypatch):
    store = DependencyStore(tmp_path / "data", tmp_path / "cache", platform_key="x86_64")
    target = store._tools_root / "rg" / "15.2.0" / "x86_64"
    target.mkdir(parents=True)
    executable = target / "rg"
    executable.write_text("#!/bin/sh\necho rg 15.2.0\n")
    executable.chmod(0o755)
    (target / ".mypr-complete.json").write_text("{malformed")
    monkeypatch.setattr(store, "_inspect_system", lambda _artifact: None)
    result = await store.inspect("rg")
    assert result["source"] == "shared"
    assert result["status"] == "unusable"
    assert "metadata" in result["reason"] or "digest" in result["reason"]
    await store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("with_metadata", [False, True])
@pytest.mark.parametrize("with_current", [False, True])
async def test_legacy_model_directory_is_preserved(tmp_path, with_metadata, with_current):
    store = DependencyStore(tmp_path / "data", tmp_path / "cache", platform_key="x86_64")
    legacy = store.model_root / "tessdata_fast-4.1.0-65727574dfcd"
    legacy.mkdir(parents=True)
    data = b"legacy model"
    model = legacy / "eng.traineddata"
    model.write_bytes(data)
    metadata = {
        "name": "tessdata:eng",
        "version": "4.1.0",
        "url": "https://example.invalid/eng.traineddata",
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    marker = legacy / ".eng.mypr-complete.json"
    if with_metadata:
        marker.write_text(json.dumps(metadata))
    current = store.model_root / "tessdata_fast"
    if with_current:
        current.mkdir()
    inspected = await store.inspect("tessdata:eng")
    assert inspected["status"] == "installed"
    assert Path(inspected["model_dir"]) == legacy
    assert not (current / "eng.traineddata").exists()
    result = await store.ensure("tessdata:eng")
    assert result["status"] == "installed"
    assert result["version"] == "4.1.0"
    assert Path(result["model_dir"]) == (current if with_current else legacy)
    if with_current:
        assert await asyncio.to_thread((current / "eng.traineddata").read_bytes) == data
    assert model.read_bytes() == data
    if with_metadata:
        assert json.loads(marker.read_text()) == metadata
    else:
        assert not marker.exists()
    await store.close()


@pytest.mark.asyncio
async def test_same_data_root_different_caches_share_install_lock(tmp_path, monkeypatch):
    archive = tmp_path / "rg.tar.gz"
    _archive(archive, "tar.gz", {"rg-15.2.0/rg": b"#!/bin/sh\necho rg 15.2.0\n"})
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    artifact = _fixture_artifact("rg", version="15.2.0", url="fixture://rg", sha256=digest)
    _patch_resolver(monkeypatch, artifact)
    calls = 0

    async def download(_url: str, destination: Path, _maximum: int) -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)
        data = await asyncio.to_thread(archive.read_bytes)
        await asyncio.to_thread(destination.write_bytes, data)

    data_root = tmp_path / "shared-data"
    stores = [
        DependencyStore(
            data_root, tmp_path / "cache-a", downloader=download, platform_key="x86_64"
        ),
        DependencyStore(
            data_root, tmp_path / "cache-b", downloader=download, platform_key="x86_64"
        ),
    ]
    for store in stores:
        monkeypatch.setattr(store, "_inspect_system", lambda _artifact: None)
    results = await asyncio.gather(*(store.install("rg") for store in stores))
    assert calls == 1
    assert all(result["status"] == "installed" for result in results)
    await asyncio.gather(*(store.close() for store in stores))


@pytest.mark.asyncio
async def test_cancellation_while_waiting_for_lock_does_not_leak_lock(tmp_path, monkeypatch):
    from mypr_mcp import dependency_store

    artifact = _fixture_artifact("rg", version="15.2.0", url="fixture://rg", sha256=None)
    _patch_resolver(monkeypatch, artifact)
    store = DependencyStore(tmp_path / "data", tmp_path / "cache", platform_key="x86_64")
    monkeypatch.setattr(store, "_inspect_system", lambda _artifact: None)
    store._locks_root.mkdir(parents=True)
    lock_path = store._locks_root / "rg.lock"
    held = await asyncio.to_thread(dependency_store._open_lock, lock_path, time.monotonic() + 2)
    task = asyncio.create_task(store.install("rg"))
    for _ in range(100):
        if store._inflight:
            break
        await asyncio.sleep(0.005)
    task.cancel()
    await asyncio.sleep(0)
    await asyncio.to_thread(dependency_store._close_lock, held)
    with pytest.raises(asyncio.CancelledError):
        await task
    probe = await asyncio.to_thread(dependency_store._open_lock, lock_path, time.monotonic() + 2)
    await asyncio.to_thread(dependency_store._close_lock, probe)
    await store.close()


@pytest.mark.asyncio
async def test_cancellation_of_last_waiter_releases_completed_install(tmp_path, monkeypatch):
    store = DependencyStore(tmp_path / "data", tmp_path / "cache", platform_key="x86_64")
    started = asyncio.Event()
    release = asyncio.Event()

    async def inspect(_name):
        return {"status": "missing"}

    async def install(_name):
        started.set()
        await release.wait()
        return {"status": "installed"}

    monkeypatch.setattr(store, "inspect", inspect)
    monkeypatch.setattr(store, "_install", install)
    request = asyncio.create_task(store.ensure("rg"))
    await asyncio.wait_for(started.wait(), 1)
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request

    release.set()
    for _ in range(100):
        await asyncio.sleep(0)
        if not store._inflight:
            break
    assert not store._inflight
    await store.close()


@pytest.mark.asyncio
async def test_cancellation_after_temp_directory_creation_cleans_directory(tmp_path, monkeypatch):
    from mypr_mcp import dependency_store

    archive = tmp_path / "rg.tar.gz"
    _archive(archive, "tar.gz", {"rg-15.2.0/rg": b"#!/bin/sh\necho rg 15.2.0\n"})
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    artifact = _fixture_artifact("rg", version="15.2.0", url="fixture://rg", sha256=digest)
    _patch_resolver(monkeypatch, artifact)

    async def download(_url: str, destination: Path, _maximum: int) -> None:
        await asyncio.to_thread(
            destination.write_bytes, await asyncio.to_thread(archive.read_bytes)
        )

    created = threading.Event()
    release = threading.Event()
    original_mkdtemp = dependency_store.tempfile.mkdtemp

    def blocked_mkdtemp(*args, **kwargs):
        path = original_mkdtemp(*args, **kwargs)
        created.set()
        release.wait(2)
        return path

    monkeypatch.setattr(dependency_store.tempfile, "mkdtemp", blocked_mkdtemp)
    store = DependencyStore(
        tmp_path / "data", tmp_path / "cache", downloader=download, platform_key="x86_64"
    )
    monkeypatch.setattr(store, "_inspect_system", lambda _artifact: None)
    task = asyncio.create_task(store.install("rg"))
    await asyncio.to_thread(created.wait, 2)
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    await store.close()
    assert not list(store._tools_root.glob(".rg-*"))


@pytest.mark.parametrize("value", ["", "relative/path"])
def test_empty_or_relative_xdg_roots_do_not_depend_on_workspace(tmp_path, monkeypatch, value):
    home = tmp_path / "home"
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: home))
    monkeypatch.setenv("XDG_DATA_HOME", value)
    monkeypatch.setenv("XDG_CACHE_HOME", value)
    store = DependencyStore(platform_key="x86_64")
    assert store.data_root == home / ".local" / "share" / "mypr"
    assert store.cache_root == home / ".cache" / "mypr"
    assert not home.exists()


@pytest.mark.asyncio
async def test_model_publication_handles_separate_cache_filesystem(tmp_path, monkeypatch):
    import errno

    data = b"model fixture"
    artifact = _fixture_artifact(
        "tessdata:eng",
        version="4.2.0",
        url="fixture://model",
        sha256=hashlib.sha256(data).hexdigest(),
    )
    _patch_resolver(monkeypatch, artifact)
    from mypr_mcp import dependency_store

    cache = tmp_path / "cache"
    original_replace = dependency_store.os.replace

    def replace_file(source, destination):
        if Path(source).is_relative_to(cache) and not Path(destination).is_relative_to(cache):
            raise OSError(errno.EXDEV, "cross-device rename")
        return original_replace(source, destination)

    monkeypatch.setattr(dependency_store.os, "replace", replace_file)
    store = DependencyStore(
        tmp_path / "data",
        cache,
        downloader=_fake_downloader({"fixture://model": data}),
    )
    try:
        result = await store.ensure("tessdata:eng")
        assert result["status"] == "installed"
        assert await asyncio.to_thread(Path(result["path"]).read_bytes) == data
        assert Path(result["model_dir"]).name == "tessdata_fast"
        marker = Path(result["model_dir"]) / ".eng.mypr-complete.json"
        assert json.loads(marker.read_text()) == {
            "name": "tessdata:eng",
            "version": "4.2.0",
            "url": "fixture://model",
            "sha256": hashlib.sha256(data).hexdigest(),
        }
        assert not list(cache.rglob("*.tmp"))
        assert not list(store.model_root.rglob(".*.tmp"))
    finally:
        await store.close()


@pytest.mark.parametrize("method", ["ensure", "install"])
async def test_close_rejects_install_waiting_for_admission(tmp_path, monkeypatch, method):
    store = DependencyStore(tmp_path / "data", tmp_path / "cache")
    inspected = asyncio.Event()

    async def inspect(_name):
        inspected.set()
        return {"status": "missing"}

    async def unexpected(_name):
        raise AssertionError("installation must not start after close")

    monkeypatch.setattr(store, "inspect", inspect)
    monkeypatch.setattr(store, "_install", unexpected)
    async with store._guard:
        request = asyncio.create_task(getattr(store, method)("rg"))
        await asyncio.wait_for(inspected.wait(), 2)
        closing = asyncio.create_task(store.close())
        await asyncio.sleep(0)
    try:
        with pytest.raises(RuntimeError, match="store is closed"):
            await request
        await closing
        assert not store._inflight
    finally:
        await asyncio.gather(request, closing, return_exceptions=True)
