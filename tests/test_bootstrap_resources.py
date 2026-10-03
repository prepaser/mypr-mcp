import pytest

from mypr_mcp import bootstrap
from mypr_mcp.diagnostics import RPCError


async def test_bootstrap_prepares_only_core_packages():
    calls = []

    class Service:
        async def ensure(self, names, **kwargs):
            calls.append((tuple(names), kwargs))
            return {"items": [{"name": name, "status": "installed"} for name in names]}

    result = await bootstrap.prepare_core(Service())
    assert [item["name"] for item in result["items"]] == ["ipykernel", "tomlkit"]
    assert calls == [
        (("ipykernel", "tomlkit"), {"automatic": True, "context": {"bootstrap": True}})
    ]


async def test_bootstrap_missing_core_explains_manual_preparation():
    class Service:
        async def ensure(self, names, **kwargs):
            raise RPCError("ipykernel is missing", code="dependency_missing")

    with pytest.raises(RPCError, match="uvx mypr-mcp prepare") as failure:
        await bootstrap.prepare_core(Service())
    assert failure.value.code == "dependency_missing"
    assert failure.value.details["required"] == ["ipykernel", "tomlkit"]


async def test_bootstrap_explicit_preparation_bypasses_auto_install():
    class Service:
        async def ensure(self, names, **kwargs):
            assert kwargs["automatic"] is False
            return {"items": []}

    assert await bootstrap.prepare_core(Service(), automatic=False) == {"items": []}


async def test_bootstrap_propagates_install_failure():
    class Service:
        async def ensure(self, names, **kwargs):
            raise RPCError("install failed", code="dependency_install_failed")

    with pytest.raises(RPCError, match="install failed") as failure:
        await bootstrap.prepare_core(Service())
    assert failure.value.code == "dependency_install_failed"
