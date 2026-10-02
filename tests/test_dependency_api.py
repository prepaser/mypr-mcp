from __future__ import annotations

import pytest

from mypr_mcp.dependency_api import Dependencies
from mypr_mcp.diagnostics import RPCError


@pytest.mark.asyncio
async def test_dependency_api_forwards_list_and_explicit_ensure():
    calls = []

    async def rpc(op, **kwargs):
        calls.append((op, kwargs))
        return {"items": []}

    api = Dependencies(rpc)
    assert await api.list(kind="python", limit=3, cursor="next") == {"items": []}
    assert await api.ensure("pillow", "pymupdf") == {"items": []}
    assert calls == [
        (
            "dependencies",
            {"method": "list", "params": {"kind": "python", "limit": 3, "cursor": "next"}},
        ),
        (
            "dependencies",
            {"method": "ensure", "params": {"names": ["pillow", "pymupdf"], "automatic": False}},
        ),
    ]


@pytest.mark.asyncio
async def test_dependency_api_explains_old_manager_capability():
    async def rpc(*_args, **_kwargs):
        raise RPCError("Unknown operation: dependencies", code="unknown_operation")

    with pytest.raises(RPCError) as failure:
        await Dependencies(rpc).list()
    assert failure.value.code == "capability_missing"
    assert failure.value.operation == "dependencies"
    assert failure.value.details == {"capability": "dependencies", "restart_required": True}
    assert "restart it" in str(failure.value)


@pytest.mark.asyncio
async def test_dependency_api_preserves_non_capability_errors():
    original = RPCError("dependency is unusable", code="dependency_unusable")

    async def rpc(*_args, **_kwargs):
        raise original

    with pytest.raises(RPCError) as failure:
        await Dependencies(rpc).ensure("pillow")
    assert failure.value is original


@pytest.mark.asyncio
async def test_dependency_api_automatic_helper_is_available_to_runtime_adapters():
    calls = []

    async def rpc(op, **kwargs):
        calls.append((op, kwargs))
        return {"items": []}

    api = Dependencies(rpc)
    await api._automatic("rg", "ast-grep")
    assert calls == [
        (
            "dependencies",
            {"method": "ensure", "params": {"names": ["rg", "ast-grep"], "automatic": True}},
        )
    ]
