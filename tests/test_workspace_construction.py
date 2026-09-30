import asyncio

import pytest

import mypr_mcp.kernel_api as api


async def test_workspace_helpers_share_one_graph_and_factory_has_no_global_tasks(
    tmp_path, monkeypatch
):
    async def rpc(*args, **kwargs):
        pass

    monkeypatch.setattr(api, "_rpc", rpc)
    first = api.create_workspace(tmp_path)
    assert first.code.fs is first.fs
    assert first.modules.fs is first.fs
    assert first.fs._shell is first.shell
    assert first.shell._tasks is first.tasks
    assert first.net.tasks is first.tasks
    await first._close_resources()

    second = api.create_workspace(tmp_path)
    assert second.tasks is not first.tasks
    job = second.tasks.start(asyncio.sleep(0, result=42))
    assert await job == 42
    await job._reporter
    await second._close_resources()


@pytest.mark.parametrize("force", ["false", 0, None])
async def test_reset_rejects_non_boolean_before_contacting_manager(tmp_path, monkeypatch, force):
    async def rpc(*args, **kwargs):
        pytest.fail("invalid force must not contact the manager")

    monkeypatch.setattr(api, "_rpc", rpc)
    with pytest.raises(TypeError, match="force must be a boolean"):
        await api.Workspace(tmp_path).reset(force=force)


async def test_mcp_mutation_rejects_non_boolean_force_before_rpc(monkeypatch):
    async def rpc(*args, **kwargs):
        pytest.fail("invalid force must not contact the manager")

    monkeypatch.setattr(api, "_rpc", rpc)
    with pytest.raises(TypeError, match="force must be a boolean"):
        await api.MCP().configure("sample", {"command": "example"}, force="false")
