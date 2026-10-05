import pytest

from mypr_mcp.kernel_api import Workspace


async def test_workspace_retries_failed_resource_cleanup(tmp_path, monkeypatch):
    workspace = Workspace(tmp_path)
    attempts = 0

    async def close_browser():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient cleanup failure")

    monkeypatch.setattr(workspace.browser, "aclose", close_browser)
    with pytest.raises(ExceptionGroup, match="cleanup failed"):
        await workspace._close_resources()
    await workspace._close_resources()
    await workspace._close_resources()
    assert attempts == 2
