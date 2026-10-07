from __future__ import annotations

import asyncio
import json
import os
import shutil
import stat
import subprocess
import textwrap
from pathlib import Path

import pytest

from mypr_mcp import bootstrap
from mypr_mcp.bridge import ConnectionBridge
from mypr_mcp.diagnostics import RPCError
from mypr_mcp.json_utils import SOURCE_HASH_ENCODING, source_sha256
from mypr_mcp.runtime import Runtime


async def test_manual_prepare_rejects_a_live_manager(tmp_path, monkeypatch):
    monkeypatch.setattr("mypr_mcp.transport.manager_running", lambda _workspace: True)

    async def no_install(*_args, **_kwargs):
        pytest.fail("prepare must not install into a running kernel")

    with pytest.raises(RPCError, match="Stop the workspace manager") as failure:
        await bootstrap.prepare_workspace(tmp_path, command=no_install)
    assert failure.value.code == "manager_running"
    assert not (tmp_path / ".mypr" / "venv").exists()


def _fake_uv(path: Path, log: Path) -> None:
    real_uv = os.environ.get("MYPR_TEST_REAL_UV") or shutil.which("uv")
    if not real_uv:
        pytest.skip("uv is required for the isolated workspace preparation test")
    path.write_text(
        textwrap.dedent(
            f"""
            #!/usr/bin/env python3
            import json
            import subprocess
            import sys
            from pathlib import Path

            REAL_UV = {real_uv!r}
            LOG = Path({str(log)!r})

            def record(kind, target=None):
                with LOG.open("a", encoding="utf-8") as stream:
                    json.dump({{"kind": kind, "target": target}}, stream)
                    stream.write("\\n")

            args = sys.argv[1:]
            if len(args) >= 2 and args[0] == "venv":
                target = args[1]
                record("venv", target)
                raise SystemExit(subprocess.call([REAL_UV, *args]))

            if "pip" not in args:
                raise SystemExit("unsupported fake uv command")
            pip_index = args.index("pip")
            pip_args = args[pip_index + 1:]
            command = pip_args[0]
            target = pip_args[pip_args.index("--python") + 1]
            if command == "install":
                record("install", target)
                site = subprocess.check_output(
                    [target, "-c", "import site; print(site.getsitepackages()[0])"],
                    text=True,
                ).strip()
                site_path = Path(site)
                packages = {{
                    "ipykernel": ("ipykernel", "7.3.0"),
                    "tomlkit": ("tomlkit", "0.15.0"),
                }}
                for distribution, (module, version) in packages.items():
                    package_path = site_path / module
                    package_path.mkdir(parents=True, exist_ok=True)
                    (package_path / "__init__.py").write_text(
                        "__version__ = " + repr(version) + "\\n", encoding="utf-8"
                    )
                    info = site_path / (distribution + "-" + version + ".dist-info")
                    info.mkdir(exist_ok=True)
                    (info / "METADATA").write_text(
                        "Metadata-Version: 2.1\\nName: " + distribution
                        + "\\nVersion: " + version + "\\n", encoding="utf-8"
                    )
                raise SystemExit(0)
            if command == "freeze":
                record("freeze", target)
                print("ipykernel==7.3.0")
                print("tomlkit==0.15.0")
                raise SystemExit(0)
            raise SystemExit("unsupported fake uv pip command")
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


@pytest.mark.asyncio
async def test_manual_prepare_uses_only_workspace_uv_venv_and_reuses_packages(
    tmp_path, monkeypatch
):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / ".mypr").mkdir()
    (workspace / ".mypr" / "config.toml").write_text(
        "[dependencies]\nauto_install = false\n", encoding="utf-8"
    )
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    log = tmp_path / "uv.jsonl"
    _fake_uv(fake_bin / "uv", log)
    monkeypatch.setenv("PATH", str(fake_bin) + os.pathsep + os.environ["PATH"])

    async def direct_command(*args, env=None, **kwargs):
        process = await asyncio.create_subprocess_exec(
            *args,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        output, error = await process.communicate()
        if process.returncode:
            raise RuntimeError(
                f"command failed: {args[0]}: {(error or output).decode(errors='replace')}"
            )

    first = await asyncio.wait_for(
        bootstrap.prepare_workspace(workspace, command=direct_command), 20
    )
    second = await asyncio.wait_for(
        bootstrap.prepare_workspace(workspace, command=direct_command), 20
    )

    python = Path(first["python"])
    assert python.parent.parent == workspace / ".mypr" / "venv"
    assert [(item["name"], item["status"]) for item in first["items"]] == [
        ("ipykernel", "installed"),
        ("tomlkit", "installed"),
    ]
    assert [(item["name"], item["status"]) for item in second["items"]] == [
        ("ipykernel", "installed"),
        ("tomlkit", "installed"),
    ]
    version = (
        await asyncio.to_thread(
            subprocess.check_output,
            [
                str(python),
                "-I",
                "-c",
                "import importlib.metadata as m; print(m.version('tomlkit'))",
            ],
            text=True,
        )
    ).strip()
    assert version == "0.15.0"
    records = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert [item["kind"] for item in records] == ["venv", "install", "freeze"]
    assert all(item["target"] == str(python) for item in records[1:])


@pytest.mark.asyncio
async def test_bridge_retries_dependency_missing_without_changing_connection(tmp_path, monkeypatch):
    from mypr_mcp import cli

    calls = []
    prepared = False

    async def ensure(_workspace):
        calls.append(prepared)
        if not prepared:
            raise RPCError(
                "kernel dependencies are missing",
                code="dependency_missing",
                details={"prepare_command": "uvx mypr-mcp prepare"},
            )
        return tmp_path / "manager.sock"

    closed = asyncio.Event()

    class Attachment:
        async def wait_closed(self):
            await closed.wait()

    bridge = ConnectionBridge(tmp_path)

    async def attach(path):
        bridge.path = path
        bridge.attachment = Attachment()
        bridge._state = {
            "version": "1.4.0",
            "protocol_version": 1,
            "capabilities": [],
            "instructions": "",
        }
        bridge._ready.set()

    monkeypatch.setattr(cli, "ensure", ensure)
    monkeypatch.setattr(bridge, "_attach", attach)
    connection_id = bridge.connection_id

    with pytest.raises(RPCError, match="kernel dependencies"):
        await bridge.wait_ready()
    assert calls == [False]
    assert bridge.connection_id == connection_id

    prepared = True
    await bridge.wait_ready()
    assert calls == [False, True]
    assert bridge.connection_id == connection_id
    assert bridge._error is None
    await bridge.close()


def _runtime_for_dedup(history):
    runtime = Runtime.__new__(Runtime)
    runtime.history = history
    runtime.stopping = asyncio.Event()
    runtime.resetting = False
    runtime.restarting = None
    runtime.restart_pending = None
    runtime.healthy = True
    runtime._admission_lock = asyncio.Lock()
    runtime.generation = "generation"

    async def io(function, *args, **kwargs):
        return function(*args, **kwargs)

    runtime.io = io
    return runtime


@pytest.mark.asyncio
@pytest.mark.parametrize("record_kind", ["code", "digest"])
async def test_request_dedup_reuses_old_record_by_code_or_digest(record_kind):
    import hashlib

    record = {"id": f"old-{record_kind}"}
    if record_kind == "code":
        record["code"] = "1 + 1"
    else:
        record["code_sha256"] = hashlib.sha256(b"1 + 1").hexdigest()

    class History:
        def find_request(self, client, request_id):
            assert (client, request_id) == ("alice", "retry")
            return record

    runtime = _runtime_for_dedup(History())
    result = await runtime._admit_execution(
        "alice", "connection", {"code": "1 + 1", "request_id": "retry"}
    )
    assert result == {"duplicate": True, "id": record["id"]}


@pytest.mark.asyncio
async def test_request_dedup_uses_marked_surrogate_safe_digest():
    source = "value = '\ud800'"
    record = {
        "id": "old",
        "code_sha256": source_sha256(source),
        "code_sha256_encoding": SOURCE_HASH_ENCODING,
    }

    class History:
        def find_request(self, _client, _request_id):
            return record

    runtime = _runtime_for_dedup(History())
    result = await runtime._admit_execution(
        "alice", "connection", {"code": source, "request_id": "retry"}
    )
    assert result == {"duplicate": True, "id": "old"}
    with pytest.raises(ValueError, match="different code"):
        await runtime._admit_execution(
            "alice", "connection", {"code": r"value = '\ud800'", "request_id": "retry"}
        )


@pytest.mark.asyncio
async def test_request_dedup_rejects_unknown_source_hash_encoding():
    class History:
        def find_request(self, _client, _request_id):
            return {
                "id": "old",
                "code_sha256": "0" * 64,
                "code_sha256_encoding": "unknown",
            }

    runtime = _runtime_for_dedup(History())
    with pytest.raises(RPCError, match="hash encoding") as failure:
        await runtime._admit_execution(
            "alice", "connection", {"code": "same", "request_id": "retry"}
        )
    assert failure.value.code == "history_corrupt"


@pytest.mark.asyncio
async def test_request_dedup_rejects_different_code():
    class History:
        def find_request(self, _client, _request_id):
            return {"id": "old", "code_sha256": "0" * 64}

    runtime = _runtime_for_dedup(History())
    with pytest.raises(ValueError, match="different code"):
        await runtime._admit_execution(
            "alice", "connection", {"code": "different", "request_id": "retry"}
        )


@pytest.mark.asyncio
async def test_request_dedup_fails_closed_when_old_record_has_no_source_or_hash():
    class History:
        def find_request(self, _client, _request_id):
            return {"id": "old"}

    runtime = _runtime_for_dedup(History())
    with pytest.raises(RPCError, match="source is unavailable") as failure:
        await runtime._admit_execution(
            "alice", "connection", {"code": "same", "request_id": "retry"}
        )
    assert failure.value.code == "history_corrupt"
