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


def test_new_tool_signatures_are_discoverable_without_optional_dependencies(tmp_path):
    ws = Workspace(tmp_path)
    paths = (
        "mcp.list_resource_templates", "net.sockets", "system.process",
        "browser.observe", "browser.snapshot", "browser.find", "browser.diff",
        "code.document_symbols", "code.workspace_symbols", "code.calls",
        "fs.rewrite_ast", "fs.apply_rewrite", "fs.read_bytes", "fs.write_bytes",
        "fs.delete", "fs.move", "fs.copy", "fs.history", "fs.restore",
        "fs.replace", "fs.apply_replace", "http.extract_html", "http.read_html",
        "git.log", "git.blame", "git.commit_info", "docs.ocr", "docs.extract",
        "docs.backends", "code.rename", "code.actions", "code.prepare_action",
        "code.apply_edit", "code.workspace_diagnostics", "messages.clients",
        "storage.usage", "storage.gc", "storage.gc_apply", "pages.iter", "doctor",
    )
    for path in paths:
        if "." in path:
            owner, method = path.split(".")
            member = getattr(getattr(ws, owner), method)
        else:
            owner, method = ws, path
            member = getattr(ws, path)
        assert f"ws.{path}{inspect.signature(member)}" in ws.help(path)
