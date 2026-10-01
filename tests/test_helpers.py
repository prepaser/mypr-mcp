from __future__ import annotations

import json
import os
import stat
import sys
from pathlib import Path

import pytest

from mypr_mcp.config import ConfigError, MCPConfig, load_workspace_config
from mypr_mcp.diagnostics import RPCError, error_info, error_response
from mypr_mcp.package_worker import install_packages


def test_structured_errors_keep_legacy_message_and_bound_details():
    error = RPCError("failed", code="timeout", operation="probe", details={"value": "x" * 5000})
    info = error_info(error)
    assert info["code"] == "timeout"
    assert info["operation"] == "probe"
    assert info["truncated"] is True
    assert error_response(error)["error"] == "RPCError: failed"


def test_structured_error_limit_and_exception_location_are_bounded():
    error = ConfigError("bad", path="limits." + "x" * 1000, line=4, column=9)
    info = error_info(
        RPCError("broken", code="c" * 1000, error_type="t" * 1000, details={"count": 3}),
        limit=128,
    )
    assert len(json.dumps(info, ensure_ascii=False, separators=(",", ":")).encode()) <= 128
    located = error_info(error)
    assert located["details"]["line"] == 4
    assert located["details"]["column"] == 9
    assert located["details"]["path"].startswith("limits.")


def test_config_loader_validates_known_sections_and_keeps_unknown(tmp_path: Path):
    root = tmp_path / ".mypr"
    root.mkdir()
    (root / "config.toml").write_text(
        "[limits]\nresponse_bytes = 4096\n\n[future]\nvalue = 'kept'\n"
    )
    snapshot = load_workspace_config(tmp_path)
    assert snapshot.values["limits"]["response_bytes"] == 4096
    assert snapshot.values["future"]["value"] == "kept"
    (root / "config.toml").write_text("[limits]\nresponse_bytes = 1\n")
    with pytest.raises(ConfigError, match="limits.response_bytes"):
        load_workspace_config(tmp_path)
    (root / "config.toml").write_text("[limits]\nresponse_bytes = 1048577\n")
    with pytest.raises(ConfigError, match="limits.response_bytes"):
        load_workspace_config(tmp_path)


def test_config_loader_normalizes_lsp_like_lsp_manager(tmp_path: Path):
    root = tmp_path / ".mypr"
    root.mkdir()
    (root / "config.toml").write_text(
        "[lsp.servers.python]\n"
        "command = ['pylsp']\n"
        "languages = ['python', 'python']\n"
        "timeout = 10\n"
    )
    snapshot = load_workspace_config(tmp_path)
    assert snapshot.values["lsp"] == {
        "servers": {
            "python": {"command": ["pylsp"], "languages": ["python"], "timeout": 10.0}
        }
    }


def test_mcp_config_save_section_is_revision_checked_and_atomic(tmp_path: Path):
    root = tmp_path / ".mypr"
    root.mkdir()
    path = root / "config.toml"
    path.write_text("# retain\n[limits]\nresponse_bytes = 4096\n")
    store = MCPConfig(tmp_path)
    revision = store.revision
    new_revision = store.save_section("lsp", {"servers": {}}, revision)
    assert new_revision != revision
    assert "# retain" in path.read_text()
    with pytest.raises(RuntimeError, match="changed"):
        store.save_section("lsp", {"servers": {}}, revision)


def test_package_worker_serializes_install_and_preserves_old_manifest(tmp_path: Path):
    root = tmp_path / ".mypr"
    root.mkdir()
    manifest = root / "requirements.txt"
    manifest.write_text("old==1\n")
    uv = tmp_path / "uv"
    uv.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = --color ] && [ \"$3\" = pip ] && [ \"$4\" = freeze ]; "
        "then printf 'new==2\\n'; else exit 0; fi\n"
    )
    uv.chmod(uv.stat().st_mode | stat.S_IXUSR)
    result = install_packages(sys.executable, root, ["new"], uv=str(uv))
    assert result["bytes"] == len(b"new==2\n")
    assert manifest.read_text() == "new==2\n"


def test_package_worker_failed_install_keeps_manifest(tmp_path: Path):
    root = tmp_path / ".mypr"
    root.mkdir()
    manifest = root / "requirements.txt"
    manifest.write_text("old==1\n")
    uv = tmp_path / "uv"
    uv.write_text("#!/bin/sh\nexit 7\n")
    uv.chmod(uv.stat().st_mode | stat.S_IXUSR)
    with pytest.raises(RuntimeError, match="package install failed"):
        install_packages(sys.executable, root, ["new"], uv=str(uv))
    assert manifest.read_text() == "old==1\n"


def test_package_worker_oversized_freeze_keeps_manifest(tmp_path: Path):
    root = tmp_path / ".mypr"
    root.mkdir()
    manifest = root / "requirements.txt"
    manifest.write_text("old==1\n")
    uv = tmp_path / "uv.py"
    uv.write_text(
        f"#!{sys.executable}\n"
        "import sys\n"
        "if 'freeze' in sys.argv:\n"
        "    sys.stdout.write('x' * (16 * 1024 * 1024 + 1))\n"
    )
    uv.chmod(uv.stat().st_mode | stat.S_IXUSR)
    with pytest.raises(RuntimeError, match="freeze exceeds"):
        install_packages(sys.executable, root, ["new"], uv=str(uv))
    assert manifest.read_text() == "old==1\n"


def test_package_worker_accepts_freeze_at_exact_limit(tmp_path: Path):
    root = tmp_path / ".mypr"
    root.mkdir()
    manifest = root / "requirements.txt"
    uv = tmp_path / "uv.py"
    uv.write_text(
        f"#!{sys.executable}\n"
        "import sys\n"
        "if 'freeze' in sys.argv:\n"
        "    sys.stdout.write('x' * (16 * 1024 * 1024))\n"
    )
    uv.chmod(uv.stat().st_mode | stat.S_IXUSR)
    result = install_packages(sys.executable, root, ["new"], uv=str(uv))
    assert result["bytes"] == 16 * 1024 * 1024
    assert manifest.stat().st_size == 16 * 1024 * 1024


def test_package_worker_rejects_replaced_fifo_lock_without_blocking(tmp_path: Path):
    root = tmp_path / ".mypr"
    root.mkdir()
    lock = root / "packages.lock"
    os.mkfifo(lock)
    uv = tmp_path / "uv"
    uv.write_text("#!/bin/sh\nexit 0\n")
    uv.chmod(uv.stat().st_mode | stat.S_IXUSR)
    with pytest.raises(OSError, match="not a regular file"):
        install_packages(sys.executable, root, ["new"], uv=str(uv), lock_timeout=0.01)


def test_package_worker_reports_manifest_commit_with_unknown_durability(
    tmp_path: Path, monkeypatch
):
    root = tmp_path / ".mypr"
    root.mkdir()
    manifest = root / "requirements.txt"
    manifest.write_text("old==1\n")
    uv = tmp_path / "uv"
    uv.write_text(
        "#!/bin/sh\n"
        "if [ \"$1\" = --color ]; then printf 'new==2\\n'; fi\n"
    )
    uv.chmod(uv.stat().st_mode | stat.S_IXUSR)
    original_fsync = os.fsync
    calls = 0

    def fail_directory_fsync(fd):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("directory sync failed")
        return original_fsync(fd)

    monkeypatch.setattr("mypr_mcp.package_worker.os.fsync", fail_directory_fsync)
    result = install_packages(sys.executable, root, ["new"], uv=str(uv))
    assert result["durability"] == "unknown"
    assert result["warnings"] == [
        {
            "code": "manifest_durability_unknown",
            "text": "manifest was replaced but directory durability is unknown",
        }
    ]
    assert manifest.read_text() == "new==2\n"


def test_page_limit_error_keeps_resume_cursor_without_original_query_options():
    from mypr_mcp.pages import PageLimitReached

    error = PageLimitReached("fs.search", 3, next_cursor="saved-cursor",
                             next_kwargs={"cursor": "saved-cursor", "token": "private"})
    info = error_info(error, "cell.execute")
    assert info["code"] == "page_limit_reached"
    assert info["details"] == {
        "method": "fs.search", "pages_read": 3, "next_cursor": "saved-cursor"
    }
    assert "private" not in json.dumps(info)
