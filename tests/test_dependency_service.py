from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

from mypr_mcp.bootstrap import prepare_core
from mypr_mcp.dependency_service import DependencyService
from mypr_mcp.diagnostics import RPCError
from mypr_mcp.python_dependencies import CORE_PACKAGES


class FakeStore:
    def __init__(self, tmp_path: Path, *, states: dict[str, str] | None = None):
        self.data_root = tmp_path / "data"
        self.bin_root = self.data_root / "bin"
        self.cache_root = tmp_path / "cache"
        self.states = states or {}
        self.inspect_calls: list[str] = []
        self.ensure_calls: list[str] = []
        self.closed = False

    def names(self, kind: str | None = None):
        if kind in (None, "binary"):
            return ("ast-grep", "rg")
        if kind == "model":
            return ("tessdata:eng",)
        raise ValueError(kind)

    async def inspect(self, name: str):
        self.inspect_calls.append(name)
        state = self.states.get(name, "missing")
        return {
            "name": name,
            "kind": "model" if name.startswith("tessdata:") else "binary",
            "scope": "global",
            "source": "shared" if state == "installed" else None,
            "status": state,
            "version": "1.0.0" if state == "installed" else None,
            "path": str(self.data_root / name),
        }

    async def ensure(self, name: str):
        self.ensure_calls.append(name)
        self.states[name] = "installed"
        return await self.inspect(name)

    async def close(self):
        self.closed = True


def _service(tmp_path: Path, store: FakeStore, **kwargs) -> DependencyService:
    return DependencyService(
        tmp_path,
        Path("/workspace/venv/bin/python"),
        {},
        kwargs.pop("install_packages", _missing_package_install),
        kwargs.pop("install_browser", _missing_browser_install),
        store=store,
        **kwargs,
    )


async def _missing_package_install(_names, _context):
    raise AssertionError("package installation was not expected")


async def _missing_browser_install(_name, _context):
    raise AssertionError("browser installation was not expected")


@pytest.mark.asyncio
async def test_list_is_read_only_and_cursor_is_bound_to_kind(tmp_path):
    store = FakeStore(tmp_path)
    service = _service(tmp_path, store)

    first = await service.list(kind="binary", limit=1)
    second = await service.list(kind="binary", limit=1, cursor=first["next_cursor"])

    assert [item["name"] for item in first["items"]] == ["ast-grep"]
    assert [item["name"] for item in second["items"]] == ["rg"]
    assert first["has_more"] is True
    assert second["has_more"] is False
    assert store.ensure_calls == []
    assert not store.data_root.exists()
    with pytest.raises(ValueError, match="invalid dependency cursor"):
        await service.list(kind="model", limit=1, cursor=first["next_cursor"])
    await service.close()


@pytest.mark.asyncio
async def test_automatic_missing_is_blocked_but_explicit_ensure_bypasses_policy(tmp_path):
    store = FakeStore(tmp_path)
    service = _service(tmp_path, store)
    service.apply_config({"auto_install": False})

    with pytest.raises(RPCError) as failure:
        await service.ensure(["rg"], automatic=True)
    assert failure.value.code == "dependency_missing"
    assert store.ensure_calls == []

    result = await service.ensure(["rg"], automatic=False)
    assert result["items"][0]["status"] == "installed"
    assert store.ensure_calls == ["rg"]
    await service.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("auto_install", [False, True])
async def test_core_preparation_keeps_optional_install_policy(tmp_path, monkeypatch, auto_install):
    installed = set()
    calls = []

    async def install(names, context):
        calls.append((tuple(names), context))
        installed.update(names)

    service = _service(tmp_path, FakeStore(tmp_path), install_packages=install)
    service.apply_config({"auto_install": auto_install})

    async def packages(names):
        return {
            name: {"name": name, "status": "installed" if name in installed else "missing"}
            for name in names
        }

    monkeypatch.setattr(service, "_packages", packages)
    try:
        for _ in range(2):
            result = await prepare_core(service)
            assert all(item["status"] == "installed" for item in result["items"])
        assert calls == [(CORE_PACKAGES, {"bootstrap": True})]
        assert installed == set(CORE_PACKAGES)
        assert service.config["auto_install"] is auto_install
        if not auto_install:
            with pytest.raises(RPCError) as failure:
                await service.ensure(["pillow"], automatic=True)
            assert failure.value.code == "dependency_missing"
            assert failure.value.details["name"] == "pillow"
            assert len(calls) == 1
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_overlapping_python_batches_share_each_pending_package(tmp_path, monkeypatch):
    store = FakeStore(tmp_path)
    service = _service(tmp_path, store)
    installed: set[str] = set()
    entered = asyncio.Event()
    release = asyncio.Event()
    calls: list[tuple[str, ...]] = []

    async def packages(names):
        return {
            name: {
                "name": name,
                "kind": "python",
                "scope": "workspace",
                "source": "workspace" if name in installed else None,
                "status": "installed" if name in installed else "missing",
                "version": "1.0.0" if name in installed else None,
                "path": str(service.python),
            }
            for name in names
        }

    async def install(names, _context):
        calls.append(tuple(names))
        entered.set()
        await release.wait()
        installed.update(names)

    monkeypatch.setattr(service, "_packages", packages)
    service._install_packages = install

    first = asyncio.create_task(service.ensure(["pillow"], automatic=True))
    await entered.wait()
    second = asyncio.create_task(service.ensure(["pillow", "pymupdf"], automatic=True))
    await asyncio.sleep(0)
    release.set()
    await asyncio.gather(first, second)

    assert calls == [("pillow",), ("pymupdf",)]
    await service.close()


@pytest.mark.asyncio
async def test_cancelling_one_waiter_does_not_cancel_shared_install(tmp_path, monkeypatch):
    store = FakeStore(tmp_path)
    service = _service(tmp_path, store)
    installed = False
    entered = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def packages(names):
        return {
            name: {
                "name": name,
                "kind": "python",
                "scope": "workspace",
                "source": "workspace" if installed else None,
                "status": "installed" if installed else "missing",
                "version": "1.0.0" if installed else None,
                "path": str(service.python),
            }
            for name in names
        }

    async def install(_names, _context):
        nonlocal calls, installed
        calls += 1
        entered.set()
        await release.wait()
        installed = True

    monkeypatch.setattr(service, "_packages", packages)
    service._install_packages = install
    first = asyncio.create_task(service.ensure(["pillow"], automatic=True))
    await entered.wait()
    second = asyncio.create_task(service.ensure(["pillow"], automatic=True))
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    release.set()
    result = await second

    assert calls == 1
    assert result["items"][0]["status"] == "installed"
    await service.close()


@pytest.mark.asyncio
async def test_close_cancels_install_jobs_and_closes_store(tmp_path, monkeypatch):
    store = FakeStore(tmp_path)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def install(_names, _context):
        entered.set()
        await release.wait()

    service = _service(tmp_path, store, install_packages=install)

    async def packages(names):
        return {
            name: {
                "name": name,
                "kind": "python",
                "scope": "workspace",
                "source": None,
                "status": "missing",
                "version": None,
                "path": str(service.python),
            }
            for name in names
        }

    monkeypatch.setattr(service, "_packages", packages)
    request = asyncio.create_task(service.ensure(["pillow"], automatic=True))
    await entered.wait()
    await service.close()

    with pytest.raises(asyncio.CancelledError):
        await request
    assert service.active_count == 0
    assert store.closed is True
    release.set()


@pytest.mark.asyncio
async def test_install_that_leaves_package_missing_reports_unusable(tmp_path, monkeypatch):
    store = FakeStore(tmp_path)
    service = _service(tmp_path, store, install_packages=lambda *_: _noop())

    async def packages(names):
        return {
            name: {
                "name": name,
                "kind": "python",
                "scope": "workspace",
                "source": None,
                "status": "missing",
                "version": None,
                "path": str(service.python),
            }
            for name in names
        }

    async def _noop():
        return None

    monkeypatch.setattr(service, "_packages", packages)
    with pytest.raises(RPCError) as failure:
        await service.ensure(["pillow"], automatic=True)
    assert failure.value.code == "dependency_unusable"
    await service.close()


@pytest.mark.asyncio
async def test_dependency_probe_counts_as_busy_and_close_reaps_process(tmp_path, monkeypatch):
    import mypr_mcp.dependency_service as dependency_service

    store = FakeStore(tmp_path)
    service = _service(tmp_path, store)
    service.python = Path(sys.executable)
    service._probe_slots = asyncio.Semaphore(1)
    launched = asyncio.Event()
    processes = []
    original = dependency_service.asyncio.create_subprocess_exec

    async def create_process(*args, **kwargs):
        process = await original(*args, **kwargs)
        processes.append(process)
        launched.set()
        return process

    monkeypatch.setattr(dependency_service.asyncio, "create_subprocess_exec", create_process)
    probe = asyncio.create_task(
        service._probe("import time; time.sleep(30); print('{}')", {})
    )
    await asyncio.wait_for(launched.wait(), 1)
    queued = asyncio.create_task(
        service._probe("import time; time.sleep(30); print('{}')", {})
    )
    await asyncio.sleep(0)
    assert service.active_count == 2

    await service.close()
    with pytest.raises(asyncio.CancelledError):
        await probe
    with pytest.raises(asyncio.CancelledError):
        await queued
    await asyncio.sleep(0)
    assert processes[0].returncode is not None
    assert service.active_count == 0
    assert not service._probe_processes


@pytest.mark.asyncio
async def test_cancelling_probe_during_launch_still_reaps_process(tmp_path, monkeypatch):
    import mypr_mcp.dependency_service as dependency_service

    store = FakeStore(tmp_path)
    service = _service(tmp_path, store)
    service.python = Path(sys.executable)
    launching = asyncio.Event()
    processes = []
    original = dependency_service.asyncio.create_subprocess_exec

    async def create_process(*args, **kwargs):
        launching.set()
        process = await original(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(dependency_service.asyncio, "create_subprocess_exec", create_process)
    probe = asyncio.create_task(
        service._probe("import time; time.sleep(30); print('{}')", {})
    )
    await asyncio.wait_for(launching.wait(), 1)
    probe.cancel()
    with pytest.raises(asyncio.CancelledError):
        await probe
    assert processes
    assert processes[0].returncode is not None
    assert service.active_count == 0
    await service.close()


@pytest.mark.asyncio
async def test_probe_output_limit_reaps_noisy_process(tmp_path, monkeypatch):
    import mypr_mcp.dependency_service as dependency_service

    store = FakeStore(tmp_path)
    service = _service(tmp_path, store)
    service.python = Path(sys.executable)
    processes = []
    original = dependency_service.asyncio.create_subprocess_exec

    async def create_process(*args, **kwargs):
        process = await original(*args, **kwargs)
        processes.append(process)
        return process

    monkeypatch.setattr(dependency_service.asyncio, "create_subprocess_exec", create_process)
    with pytest.raises(RPCError, match="output exceeded") as failure:
        await asyncio.wait_for(
            service._probe("print('x' * 200_000)", {}),
            3,
        )
    assert failure.value.code == "dependency_probe_failed"
    assert processes[0].returncode is not None
    assert service.active_count == 0
    assert not service._probe_processes
    await service.close()


@pytest.mark.asyncio
async def test_probe_is_rejected_after_close(tmp_path):
    service = _service(tmp_path, FakeStore(tmp_path))
    await service.close()

    with pytest.raises(RuntimeError, match="Dependency service is closed"):
        await service._probe("print('{}')", {})


@pytest.mark.parametrize("engine_status", ["installed", "missing"])
async def test_browser_requires_compatible_sdk_even_with_existing_engine(
    tmp_path, monkeypatch, engine_status
):
    service = _service(tmp_path, FakeStore(tmp_path))

    async def probe(_script, names):
        if isinstance(names, dict):
            return {name: {"status": "installed", "version": "1.0"} for name in names}
        return {name: {"status": engine_status, "version": "1.0"} for name in names}

    monkeypatch.setattr(service, "_probe", probe)
    try:
        with pytest.raises(RPCError) as failure:
            await service.ensure(["browser:chromium"], automatic=True)
        assert failure.value.code == "dependency_unusable"
        assert failure.value.details["name"] == "playwright"
        inventory = await service.list(kind="browser")
        assert all(item["status"] == "unusable" for item in inventory["items"])
    finally:
        await service.close()


async def test_browser_prepares_sdk_before_engine_and_respects_auto_install(tmp_path, monkeypatch):
    service = _service(tmp_path, FakeStore(tmp_path))
    installed = set()
    calls = []

    async def probe(_script, names):
        if isinstance(names, dict):
            return {
                name: {
                    "status": "installed" if name in installed else "missing",
                    "version": "1.58.0" if name in installed else None,
                }
                for name in names
            }
        return {
            name: {
                "status": "installed" if name in installed else "missing",
                "version": "1.58.0" if "playwright" in installed else None,
            }
            for name in names
        }

    async def install_packages(names, _context):
        calls.append(tuple(names))
        installed.update(names)

    async def install_browser(name, _context):
        assert "playwright" in installed
        calls.append(name)
        installed.add(name)

    monkeypatch.setattr(service, "_probe", probe)
    service._install_packages = install_packages
    service._install_browser = install_browser
    service.apply_config({"auto_install": False})
    try:
        with pytest.raises(RPCError) as failure:
            await service.ensure(["browser:chromium"], automatic=True)
        assert failure.value.code == "dependency_missing"
        assert calls == []
        result = await service.ensure(["browser:chromium"])
        assert calls == [("playwright",), "chromium"]
        assert [(item["name"], item["status"]) for item in result["items"]] == [
            ("browser:chromium", "installed")
        ]
    finally:
        await service.close()
