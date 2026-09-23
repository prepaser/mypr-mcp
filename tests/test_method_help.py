import inspect

import pytest

from mypr_mcp.kernel_api import Workspace


def test_method_help_uses_live_signature_and_exposes_undocumented_methods(tmp_path):
    ws = Workspace(tmp_path)
    help_text = ws.help("shell.run")
    assert str(inspect.signature(ws.shell.run)) in help_text
    assert "async ws.shell.run" in help_text
    assert "stdout" in help_text
    assert "mcp.read_resource" in ws.help("mcp")
    assert "ws.mcp.read_resource" in ws.help("ws.mcp.read_resource")

    async def replacement(command, *, custom=False):
        """An updated workspace helper."""

    ws.shell.run = replacement
    updated = ws.help("shell.run")
    assert "custom=False" in updated
    assert "An updated workspace helper" in updated


@pytest.mark.parametrize(
    "path", ["shell.__dict__", "shell.run.__globals__", "client.id", "fs.missing"]
)
def test_method_help_rejects_attributes_and_arbitrary_traversal(tmp_path, path):
    with pytest.raises(ValueError, match="unknown API method"):
        Workspace(tmp_path).help(path)
