from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

from mypr_mcp.lsp_edits import EditError, EditPlan, EditPlanStore, PlannedOperation, sha256


def _operation(path: Path) -> PlannedOperation:
    return PlannedOperation("update", path, b"old\n", b"new\n", sha256(b"old\n"))


def test_lsp_plan_preconditions_and_documents_roundtrip(tmp_path: Path):
    edited = tmp_path / "edited.py"
    edited.write_text("old\n", encoding="utf-8")
    origin = tmp_path / "origin.py"
    origin.write_text("origin\n", encoding="utf-8")
    absent = tmp_path / "new.py"
    documents = {origin: (7, sha256(b"origin\n"))}
    preconditions = {origin: sha256(b"origin\n"), absent: None}

    store = EditPlanStore(tmp_path)
    plan = store.create(
        tmp_path,
        [_operation(edited)],
        "generation",
        "title",
        preconditions=preconditions,
        documents=documents,
    )
    loaded = EditPlanStore(tmp_path).get(plan.ident)

    assert loaded.preconditions == preconditions
    assert loaded.documents == documents
    assert origin not in {operation.path for operation in loaded.operations}


def test_lsp_plan_metadata_copies_input_mappings(tmp_path: Path):
    path = tmp_path / "sample.py"
    path.write_text("old\n", encoding="utf-8")
    preconditions = {path: sha256(b"old\n")}
    documents = {path: (1, sha256(b"old\n"))}

    plan = EditPlan(
        "id",
        tmp_path,
        [_operation(path)],
        "generation",
        "title",
        preconditions=preconditions,
        documents=documents,
    )
    preconditions[path] = None
    documents[path] = (2, sha256(b"changed\n"))

    assert plan.preconditions[path] == sha256(b"old\n")
    assert plan.documents[path] == (1, sha256(b"old\n"))


def test_lsp_plan_without_new_metadata_fields_remains_loadable(tmp_path: Path):
    path = tmp_path / "sample.py"
    path.write_text("old\n", encoding="utf-8")
    original = EditPlan(
        "id",
        tmp_path,
        [_operation(path)],
        "generation",
        "title",
    )
    payload = original.payload()
    payload.pop("preconditions")
    payload.pop("documents")

    loaded = EditPlan.from_payload(tmp_path, "id", payload)

    assert loaded.preconditions == {}
    assert loaded.documents == {}


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("preconditions", [{"path": Path("relative.py"), "revision": None}]),
        ("preconditions", [{"path": Path("/tmp/outside.py"), "revision": None}]),
        ("preconditions", [{"path": Path("sample.py"), "revision": "bad"}]),
        ("documents", [{"path": Path("sample.py"), "version": True, "digest": "a" * 64}]),
        ("documents", [{"path": Path("sample.py"), "version": 1, "digest": "A" * 64}]),
        ("documents", [{"path": Path("sample.py"), "version": -1, "digest": "a" * 64}]),
    ],
)
def test_lsp_plan_rejects_malformed_metadata(
    tmp_path: Path, field: str, value: list[dict[str, object]],
):
    path = tmp_path / "sample.py"
    path.write_text("old\n", encoding="utf-8")
    payload = EditPlan("id", tmp_path, [_operation(path)], "generation", "title").payload()
    payload[field] = [
        {**item, "path": path} if item.get("path") == Path("sample.py") else item
        for item in value
    ]

    with pytest.raises(EditError):
        EditPlan.from_payload(tmp_path, "id", payload)


def test_lsp_plan_rejects_symlink_metadata_path(tmp_path: Path):
    target = tmp_path / "target.py"
    target.write_text("target\n", encoding="utf-8")
    link = tmp_path / "link.py"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("symlinks are unavailable")
    path = tmp_path / "sample.py"
    path.write_text("old\n", encoding="utf-8")
    payload = EditPlan("id", tmp_path, [_operation(path)], "generation", "title").payload()
    payload["preconditions"] = [{"path": link, "revision": None}]

    with pytest.raises(EditError):
        EditPlan.from_payload(tmp_path, "id", payload)


def test_lsp_plan_rejects_invalid_revision_and_document_digest(tmp_path: Path):
    path = tmp_path / "sample.py"
    path.write_text("old\n", encoding="utf-8")
    payload = EditPlan("id", tmp_path, [_operation(path)], "generation", "title").payload()

    payload["preconditions"] = [{"path": path, "revision": "bad"}]
    with pytest.raises(EditError, match="SHA-256"):
        EditPlan.from_payload(tmp_path, "id", payload)

    payload["preconditions"] = []
    payload["documents"] = [{"path": path, "version": 1, "digest": "bad"}]
    with pytest.raises(EditError, match="SHA-256"):
        EditPlan.from_payload(tmp_path, "id", payload)


def test_lsp_plan_rejects_too_many_metadata_targets(tmp_path: Path):
    paths = {tmp_path / f"target-{index}.py": None for index in range(102)}
    path = tmp_path / "sample.py"
    path.write_text("old\n", encoding="utf-8")

    with pytest.raises(EditError, match="too many"):
        EditPlan(
            "id",
            tmp_path,
            [_operation(path)],
            "generation",
            "title",
            preconditions=paths,
        )


@pytest.mark.parametrize("field", ["path", "source"])
def test_lsp_plan_rejects_relative_operation_paths(tmp_path: Path, field):
    script = f"""
from pathlib import Path
from mypr_mcp.lsp_edits import EditError, EditPlan, PlannedOperation
root = Path.cwd()
operation = PlannedOperation("update", root / "sample.py", b"old", b"new", None)
payload = EditPlan("id", root, [operation], "generation", "title").payload()
payload["operations"][0][{field!r}] = Path("relative.py")
try:
    EditPlan.from_payload(root, "id", payload)
except EditError as exc:
    assert "absolute" in str(exc)
else:
    raise AssertionError("relative operation path accepted")
"""
    subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
        timeout=5,
        check=True,
    )
