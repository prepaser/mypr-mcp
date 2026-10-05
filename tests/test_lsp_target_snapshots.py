from __future__ import annotations

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from mypr_mcp.code_tools import MAX_DOCUMENT_BYTES, CodeTools, _LanguageServer
from mypr_mcp.lsp_edits import EditError


def _server(path: Path, edit: dict):
    document = SimpleNamespace(
        path=path,
        uri=path.as_uri(),
        version=1,
        text=path.read_text(encoding="utf-8"),
    )
    server = SimpleNamespace(
        name="fake",
        generation="generation",
        position_encoding="utf-16",
        documents={document.uri: document},
        capabilities={"codeActionProvider": {}},
        process=SimpleNamespace(returncode=None),
        _failure=None,
        _operation_lock=asyncio.Lock(),
    )
    server.status = lambda: {"name": "fake"}

    async def aclose():
        return None

    async def code_actions(*_args, **_kwargs):
        return document, [{"title": "Multi-file edit", "edit": edit}]

    server.aclose = aclose
    server.code_actions = code_actions
    return server


def _edit(origin: Path, other: Path, absent: Path) -> dict:
    return {
        "changes": {
            origin.as_uri(): [
                {
                    "range": {
                        "start": {"line": 0, "character": 0},
                        "end": {"line": 0, "character": 3},
                    },
                    "newText": "bar",
                }
            ],
            other.as_uri(): [
                {
                    "range": {
                        "start": {"line": 0, "character": 0},
                        "end": {"line": 0, "character": 3},
                    },
                    "newText": "two",
                }
            ],
        },
        "documentChanges": [{"kind": "create", "uri": absent.as_uri()}],
    }


@pytest.mark.asyncio
async def test_inline_action_rejects_changed_secondary_target(tmp_path: Path):
    origin = tmp_path / "origin.py"
    other = tmp_path / "other.py"
    absent = tmp_path / "created.py"
    origin.write_text("foo\n", encoding="utf-8")
    other.write_text("one\n", encoding="utf-8")
    code = CodeTools(tmp_path)
    code._servers["fake"] = _server(origin, _edit(origin, other, absent))
    try:
        listed = await code.actions("fake", origin, 1, 1)
        other.write_text("changed\n", encoding="utf-8")
        with pytest.raises(EditError, match="document changed"):
            await code.prepare_action(listed["actions"][0]["action_id"])
        assert origin.read_text(encoding="utf-8") == "foo\n"
        assert other.read_text(encoding="utf-8") == "changed\n"
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_inline_action_rejects_changed_open_target_without_disk_change(tmp_path: Path):
    origin = tmp_path / "origin.py"
    other = tmp_path / "other.py"
    origin.write_text("foo\n", encoding="utf-8")
    other.write_text("one\n", encoding="utf-8")
    code = CodeTools(tmp_path)
    server = _server(origin, _edit(origin, other, tmp_path / "created.py"))
    document = SimpleNamespace(path=other, uri=other.as_uri(), version=1, text="one\n")
    server.documents[document.uri] = document
    code._servers["fake"] = server
    try:
        listed = await code.actions("fake", origin, 1, 1)
        document.text = "changed\n"
        with pytest.raises(EditError, match="document changed"):
            await code.prepare_action(listed["actions"][0]["action_id"])
        assert other.read_text(encoding="utf-8") == "one\n"
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_inline_action_rejects_target_created_after_request(tmp_path: Path):
    origin = tmp_path / "origin.py"
    other = tmp_path / "other.py"
    absent = tmp_path / "created.py"
    origin.write_text("foo\n", encoding="utf-8")
    other.write_text("one\n", encoding="utf-8")
    code = CodeTools(tmp_path)
    code._servers["fake"] = _server(origin, _edit(origin, other, absent))
    try:
        listed = await code.actions("fake", origin, 1, 1)
        absent.write_text("created externally\n", encoding="utf-8")
        with pytest.raises(EditError, match="document changed"):
            await code.prepare_action(listed["actions"][0]["action_id"])
        assert origin.read_text(encoding="utf-8") == "foo\n"
        assert absent.read_text(encoding="utf-8") == "created externally\n"
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_actions_share_closed_target_snapshots(tmp_path: Path):
    origin = tmp_path / "origin.py"
    other = tmp_path / "other.py"
    absent = tmp_path / "created.py"
    origin.write_text("foo\n", encoding="utf-8")
    other.write_text("one\n", encoding="utf-8")
    code = CodeTools(tmp_path)
    server = _server(origin, _edit(origin, other, absent))
    document = next(iter(server.documents.values()))
    raw = {"title": "Multi-file edit", "edit": _edit(origin, other, absent)}

    async def code_actions(*_args, **_kwargs):
        return document, [raw, raw.copy()]

    server.code_actions = code_actions
    code._servers["fake"] = server
    reads: dict[Path, int] = {}
    original_read = code._read_edit_bytes

    async def counted_read(path: Path):
        reads[path] = reads.get(path, 0) + 1
        return await original_read(path)

    code._read_edit_bytes = counted_read
    try:
        listed = await code.actions("fake", origin, 1, 1)
        assert len(listed["actions"]) == 2
        assert reads == {other: 1, absent: 1}
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_actions_error_discards_only_new_ids(tmp_path: Path):
    origin = tmp_path / "origin.py"
    origin.write_text("foo\n", encoding="utf-8")
    code = CodeTools(tmp_path)
    server = _server(origin, {"changes": {}})
    document = next(iter(server.documents.values()))
    code._servers["fake"] = server
    try:
        first = await code.actions("fake", origin, 1, 1)
        old_id = first["actions"][0]["action_id"]

        async def failing_actions(*_args, **_kwargs):
            return document, [
                {"title": "valid", "edit": {"changes": {}}},
                {"title": "outside", "edit": {"changes": {"file:///tmp/outside.py": []}}},
            ]

        server.code_actions = failing_actions
        with pytest.raises(EditError):
            await code.actions("fake", origin, 1, 1)
        assert old_id in code._actions
        assert len(code._actions) == 1
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_prepared_plan_rejects_stale_nonedited_origin(tmp_path: Path):
    origin = tmp_path / "origin.py"
    other = tmp_path / "other.py"
    origin.write_text("foo\n", encoding="utf-8")
    other.write_text("one\n", encoding="utf-8")
    edit = {
        "changes": {
            other.as_uri(): [
                {
                    "range": {
                        "start": {"line": 0, "character": 0},
                        "end": {"line": 0, "character": 3},
                    },
                    "newText": "two",
                }
            ]
        }
    }
    code = CodeTools(tmp_path)
    code._servers["fake"] = _server(origin, edit)
    try:
        listed = await code.actions("fake", origin, 1, 1)
        prepared = await code.prepare_action(listed["actions"][0]["action_id"])
        origin.write_text("changed\n", encoding="utf-8")
        with pytest.raises(EditError, match="document changed"):
            await code.apply_edit(prepared["plan_id"])
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_prepared_plan_rejects_open_document_version_change(tmp_path: Path):
    origin = tmp_path / "origin.py"
    other = tmp_path / "other.py"
    origin.write_text("foo\n", encoding="utf-8")
    other.write_text("one\n", encoding="utf-8")
    code = CodeTools(tmp_path)
    server = _server(origin, _edit(origin, other, tmp_path / "created.py"))
    code._servers["fake"] = server
    try:
        listed = await code.actions("fake", origin, 1, 1)
        prepared = await code.prepare_action(listed["actions"][0]["action_id"])
        document = next(iter(server.documents.values()))
        document.version += 1
        with pytest.raises(EditError, match="document changed"):
            await code.apply_edit(prepared["plan_id"])
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_persisted_absent_target_guard_survives_reload(tmp_path: Path):
    origin = tmp_path / "origin.py"
    absent = tmp_path / "created.py"
    origin.write_text("foo\n", encoding="utf-8")
    edit = {"documentChanges": [{"kind": "create", "uri": absent.as_uri()}]}
    code = CodeTools(tmp_path)
    server = _server(origin, edit)
    code._servers["fake"] = server
    try:
        listed = await code.actions("fake", origin, 1, 1)
        prepared = await code.prepare_action(listed["actions"][0]["action_id"])
        plan = await code._plans.aget(prepared["plan_id"])
        assert plan.preconditions[absent] is None
        assert absent not in plan.documents
    finally:
        await code.aclose()

    absent.write_text("created elsewhere\n", encoding="utf-8")
    reloaded = CodeTools(tmp_path)
    reloaded._servers["fake"] = server
    try:
        with pytest.raises(EditError, match="document changed"):
            await reloaded.apply_edit(prepared["plan_id"])
    finally:
        await reloaded.aclose()


@pytest.mark.asyncio
async def test_lsp_edit_read_is_bounded(tmp_path: Path):
    path = tmp_path / "large.py"
    path.write_bytes(b"x" * (MAX_DOCUMENT_BYTES + 1))
    code = CodeTools(tmp_path)
    try:
        with pytest.raises(ValueError, match="document exceeds"):
            await code._read_edit_bytes(path)
    finally:
        await code.aclose()


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO is not available")
def test_lsp_source_read_rejects_nonregular_file(tmp_path: Path):
    path = tmp_path / "source.py"
    os.mkfifo(path)
    with pytest.raises(ValueError, match="regular source file"):
        _LanguageServer._read_file(path)


@pytest.mark.asyncio
async def test_workspace_edit_virtual_create_delete_create_update_applies(tmp_path: Path):
    origin = tmp_path / "origin.py"
    target = tmp_path / "generated.py"
    origin.write_text("origin\n", encoding="utf-8")
    code = CodeTools(tmp_path)
    server = _server(origin, {})
    code._servers["fake"] = server
    edit = {
        "documentChanges": [
            {"kind": "create", "uri": target.as_uri()},
            {"kind": "delete", "uri": target.as_uri()},
            {"kind": "create", "uri": target.as_uri()},
            {
                "textDocument": {"uri": target.as_uri()},
                "edits": [
                    {
                        "range": {
                            "start": {"line": 0, "character": 0},
                            "end": {"line": 0, "character": 0},
                        },
                        "newText": "final\n",
                    }
                ],
            },
        ]
    }
    try:
        plan = await code._workspace_edit_plan(server, edit, title="virtual sequence")
        result = await code.apply_edit(plan.ident)
        assert result["applied"] is True
        assert target.read_text(encoding="utf-8") == "final\n"
    finally:
        await code.aclose()


@pytest.mark.asyncio
async def test_workspace_edit_rename_regenerate_source_update_applies(tmp_path: Path):
    source = tmp_path / "source.py"
    destination = tmp_path / "destination.py"
    source.write_text("old\n", encoding="utf-8")
    code = CodeTools(tmp_path)
    server = _server(source, {})
    code._servers["fake"] = server
    edit = {
        "documentChanges": [
            {
                "kind": "rename",
                "oldUri": source.as_uri(),
                "newUri": destination.as_uri(),
            },
            {"kind": "create", "uri": source.as_uri()},
            {
                "textDocument": {"uri": source.as_uri()},
                "edits": [
                    {
                        "range": {
                            "start": {"line": 0, "character": 0},
                            "end": {"line": 0, "character": 0},
                        },
                        "newText": "new\n",
                    }
                ],
            },
        ]
    }
    try:
        plan = await code._workspace_edit_plan(server, edit, title="rename sequence")
        result = await code.apply_edit(plan.ident)
        assert result["applied"] is True
        assert source.read_text(encoding="utf-8") == "new\n"
        assert destination.read_text(encoding="utf-8") == "old\n"
    finally:
        await code.aclose()
