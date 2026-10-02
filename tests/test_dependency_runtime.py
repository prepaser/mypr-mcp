from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from mypr_mcp.runtime import Runtime


class FakeDependencyService:
    def __init__(self):
        self.list_calls = []
        self.ensure_calls = []

    async def list(self, **params):
        self.list_calls.append(params)
        return {"items": [], "has_more": False, "next_cursor": None}

    async def ensure(self, names, *, automatic=False, context=None):
        self.ensure_calls.append((names, automatic, context))
        return {"items": [{"name": name, "status": "installed"} for name in names]}


def _runtime(service):
    runtime = Runtime.__new__(Runtime)
    runtime.dependencies = service
    runtime._admission_lock = asyncio.Lock()
    runtime.resetting = False
    runtime.healthy = True
    runtime.generation = "generation-1"
    runtime._check_dispatch_admission = lambda _op: None
    runtime.workspace_available = lambda: True
    return runtime


@pytest.mark.asyncio
async def test_dependency_dispatch_lists_without_client_initialization():
    service = FakeDependencyService()
    runtime = _runtime(service)
    result = await runtime._dispatch_dependencies(
        "dependencies",
        {"method": "list", "params": {"kind": "python", "limit": 2}},
        client="",
        connection_id=None,
        connection=None,
        requested_client=None,
        generation=None,
    )
    assert result["items"] == []
    assert service.list_calls == [{"kind": "python", "limit": 2}]


@pytest.mark.asyncio
@pytest.mark.parametrize("client", ["", "anonymous"])
async def test_dependency_dispatch_requires_initialized_client_for_install(client):
    runtime = _runtime(FakeDependencyService())
    with pytest.raises(RuntimeError, match="initialized client"):
        await runtime._dispatch_dependencies(
            "dependencies",
            {"method": "ensure", "params": {"names": ["rg"]}},
            client=client,
            connection_id=None,
            connection=None,
            requested_client=None,
            generation=None,
        )


@pytest.mark.asyncio
async def test_dependency_dispatch_forwards_context_and_automatic_policy():
    service = FakeDependencyService()
    runtime = _runtime(service)
    result = await runtime._dispatch_dependencies(
        "dependencies",
        {
            "method": "ensure",
            "params": {"names": ["pillow"], "automatic": True},
            "exec_id": "exec-1",
        },
        client="alice",
        connection_id="connection-1",
        connection={"client_id": "alice"},
        requested_client=None,
        generation="generation-1",
    )
    assert result["items"][0]["name"] == "pillow"
    assert service.ensure_calls == [
        (
            ["pillow"],
            True,
            {
                "client_id": "alice",
                "connection_id": "connection-1",
                "exec_id": "exec-1",
                "generation": "generation-1",
            },
        )
    ]


@pytest.mark.asyncio
async def test_dependency_dispatch_reports_unavailable_service():
    runtime = _runtime(None)
    with pytest.raises(RuntimeError, match="dependencies are unavailable"):
        await runtime._dispatch_dependencies(
            "dependencies",
            {"method": "list", "params": {}},
            client="alice",
            connection_id="connection-1",
            connection=None,
            requested_client=None,
            generation=None,
        )


@pytest.mark.asyncio
async def test_dependency_service_construction_does_not_create_global_roots(tmp_path, monkeypatch):
    from mypr_mcp.dependency_service import DependencyService
    from mypr_mcp.dependency_store import DependencyStore

    global_data = tmp_path / "global-data"
    global_cache = tmp_path / "global-cache"
    monkeypatch.setenv("XDG_DATA_HOME", str(global_data))
    monkeypatch.setenv("XDG_CACHE_HOME", str(global_cache))
    store = DependencyStore()
    service = DependencyService(
        tmp_path / "workspace",
        Path("/workspace/venv/bin/python"),
        {},
        lambda *_: None,
        lambda *_: None,
        store=store,
    )
    assert service.status()["auto_install"] is True
    assert not global_data.exists()
    assert not global_cache.exists()
    await service.close()
