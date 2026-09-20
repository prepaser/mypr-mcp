from __future__ import annotations

import hashlib
import json
import shutil
import zipfile
from io import BytesIO
from pathlib import Path

import pytest
from conftest import decode_result, execute, mcp_session, poll_until_done, result_text


async def _json_cell(session, code: str) -> dict:
    payload = await execute(session, "import json\n" + code)
    assert payload["state"] == "succeeded", payload.get("error") or result_text(payload)
    return json.loads(result_text(payload).strip())


def _pdf_bytes(text: str) -> bytes:
    escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
    bodies = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>",
        b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
        b"/Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >>",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>",
        (
            b"<< /Length "
            + str(len(b"BT /F1 12 Tf 72 720 Td (" + escaped.encode() + b") Tj ET\n")).encode()
            + b" >>\nstream\nBT /F1 12 Tf 72 720 Td ("
            + escaped.encode()
            + b") Tj ET\nendstream"
        ),
    ]
    output = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for number, body in enumerate(bodies, 1):
        offsets.append(len(output))
        output.extend(f"{number} 0 obj\n".encode())
        output.extend(body)
        output.extend(b"\nendobj\n")
    xref = len(output)
    output.extend(f"xref\n0 {len(bodies) + 1}\n".encode())
    output.extend(b"0000000000 65535 f \n")
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode())
    output.extend(
        f"trailer\n<< /Size {len(bodies) + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode()
    )
    return bytes(output)


def _docx_bytes(text: str) -> bytes:
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" '
        'ContentType="application/vnd.openxml-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/word/document.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
        "</Types>"
    )
    relationships = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="word/document.xml"/></Relationships>'
    )
    document = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">'
        f"<w:body><w:p><w:r><w:t>{text}</w:t></w:r></w:p></w:body></w:document>"
    )
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", relationships)
        archive.writestr("word/document.xml", document)
    return buffer.getvalue()


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep required")
async def test_rg_modes_spans_and_paging_are_available_through_mcp(workspace: Path):
    source = workspace / "src" / "search_target.py"
    source.parent.mkdir()
    source.write_text(
        "needle = 'one needle'\nother = 'NEEDLE'\n# TODO needle\n",
        encoding="utf-8",
    )
    async with mcp_session(workspace) as session:
        matches = await _json_cell(
            session,
            "print(json.dumps(await ws.fs.search(['needle', 'TODO'], paths='src', "
            "glob='*.py', ignore_case=True, mode='matches', max_matches=20)))",
        )
        assert matches["backend"] == "rg"
        assert matches["matches"]
        assert matches["complete"] is True
        assert matches["scan_truncated"] is False
        assert all(item["path"] == "src/search_target.py" for item in matches["matches"])
        assert all("text" in item and "line" in item for item in matches["matches"])
        assert all(item.get("submatches") for item in matches["matches"])
        assert all(
            item["range"]["start"]["byte"] < item["range"]["end"]["byte"]
            and item["submatches"][0]["range"]["start"]["byte"]
            < item["submatches"][0]["range"]["end"]["byte"]
            for item in matches["matches"]
        )

        files = await _json_cell(
            session,
            "print(json.dumps(await ws.fs.search('needle', paths='src', mode='files')))",
        )
        assert files["files"] == ["src/search_target.py"]
        assert files["matches"] == []

        counts = await _json_cell(
            session,
            "print(json.dumps(await ws.fs.search('needle', paths='src', mode='counts')))",
        )
        assert counts["counts"]
        assert counts["counts"][0]["path"] == "src/search_target.py"
        assert counts["counts"][0]["count"] >= 2

        exists = await _json_cell(
            session,
            "print(json.dumps(await ws.fs.search('missing', paths='src', mode='exists')))",
        )
        assert exists["exists"] is False
        assert exists["has_more"] is False


@pytest.mark.skipif(shutil.which("rg") is None, reason="ripgrep required")
async def test_search_does_not_block_another_mcp_cell(workspace: Path):
    (workspace / "large.txt").write_text("x" * (2 * 1024 * 1024) + "\nneedle\n", encoding="utf-8")
    async with mcp_session(workspace) as session:
        pending = decode_result(
            await session.call_tool(
                "execute",
                {
                    "code": "search_result = await ws.fs.search("
                    "'needle', paths='large.txt', timeout=30)",
                    "wait_ms": 0,
                },
            )
        )
        assert pending["state"] in {"queued", "running", "succeeded"}
        quick = await execute(session, "6 * 7")
        assert quick["state"] == "succeeded"
        assert result_text(quick).strip() == "42"
        finished = await poll_until_done(session, pending["exec_id"], deadline_seconds=10)
        assert finished["state"] == "succeeded", finished


@pytest.mark.skipif(shutil.which("ast-grep") is None, reason="ast-grep required")
async def test_ast_search_is_read_only_and_supports_pattern_and_rule(workspace: Path):
    source = workspace / "src" / "ast_target.py"
    source.parent.mkdir()
    original = (
        "import requests\n\n"
        "response = requests.get('https://example.test', timeout=3)\n"
        "other = requests.post('https://example.test')\n"
    )
    source.write_text(original, encoding="utf-8")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    async with mcp_session(workspace) as session:
        pattern = await _json_cell(
            session,
            "print(json.dumps(await ws.fs.search_ast("
            '"requests.get($URL, $$$ARGS)", lang="python", paths="src", mode="matches")))',
        )
        assert pattern["backend"] == "ast"
        assert pattern["matches"]
        assert any("requests.get" in item.get("text", "") for item in pattern["matches"])
        assert pattern["complete"] is True

        rule = await _json_cell(
            session,
            "print(json.dumps(await ws.fs.search_ast("
            'lang="python", rule={"kind": "call", "has": {"regex": "timeout"}}, '
            'paths="src", mode="files")))',
        )
        assert rule["files"] == ["src/ast_target.py"]
    assert hashlib.sha256(source.read_bytes()).hexdigest() == digest
    assert source.read_text(encoding="utf-8") == original


@pytest.mark.skipif(shutil.which("rga") is None, reason="ripgrep-all required")
async def test_rga_searches_zip_archives(workspace: Path):
    archive = workspace / "fixtures.zip"
    with zipfile.ZipFile(archive, "w") as output:
        output.writestr("nested/readme.txt", "archive-only needle\n")
    async with mcp_session(workspace) as session:
        result = await _json_cell(
            session,
            "print(json.dumps(await ws.fs.search_docs('archive-only needle', "
            "paths='fixtures.zip', mode='matches', cache=False)))",
        )
    assert result["backend"] == "rga"
    assert result["matches"]
    assert any("archive-only needle" in item.get("text", "") for item in result["matches"])
    assert result["complete"] is True
    assert not list((workspace / ".mypr" / "searches").glob(".rga-*"))


@pytest.mark.skipif(
    shutil.which("rga") is None or shutil.which("pdftotext") is None,
    reason="rga and pdftotext required",
)
async def test_rga_searches_generated_pdf(workspace: Path):
    (workspace / "fixture.pdf").write_bytes(_pdf_bytes("pdf-only needle"))
    async with mcp_session(workspace) as session:
        result = await _json_cell(
            session,
            "print(json.dumps(await ws.fs.search_docs('pdf-only needle', "
            "paths='fixture.pdf', mode='matches', cache=True)))",
        )
    assert result["backend"] == "rga"
    assert result["matches"]
    assert result["complete"] is True


@pytest.mark.skipif(
    shutil.which("rga") is None or shutil.which("pandoc") is None,
    reason="rga and the pandoc DOCX adapter are required",
)
async def test_rga_searches_generated_docx(workspace: Path):
    (workspace / "fixture.docx").write_bytes(_docx_bytes("docx-only needle"))
    async with mcp_session(workspace) as session:
        result = await _json_cell(
            session,
            "print(json.dumps(await ws.fs.search_docs('docx-only needle', "
            "paths='fixture.docx', mode='matches', cache=True)))",
        )
    assert result["backend"] == "rga"
    assert result["matches"]
    assert result["complete"] is True


@pytest.mark.skipif(shutil.which("rga") is None, reason="ripgrep-all required")
async def test_search_backends_reports_real_mcp_capabilities(workspace: Path):
    async with mcp_session(workspace) as session:
        result = await _json_cell(session, "print(json.dumps(await ws.fs.search_backends()))")
    assert result["workspace"] == str(workspace)
    assert set(result["backends"]) == {"rg", "rga", "ast"}
    assert result["backends"]["rg"]["available"] is True
    assert result["backends"]["rga"]["available"] is True
    if shutil.which("ast-grep") or shutil.which("sg"):
        assert result["backends"]["ast"]["available"] is True


@pytest.mark.skipif(
    shutil.which("rga") is None or shutil.which("pandoc") is not None,
    reason="requires rga without its pandoc converter",
)
async def test_rga_missing_converter_returns_partial_result(workspace: Path):
    (workspace / "fixture.docx").write_bytes(_docx_bytes("converter-only needle"))
    async with mcp_session(workspace) as session:
        result = await _json_cell(
            session,
            "print(json.dumps(await ws.fs.search_docs('converter-only needle', "
            "paths='fixture.docx', adapters=['pandoc'], mode='matches', cache=True)))",
        )
    assert result["backend"] == "rga"
    assert result["matches"] == []
    assert result["complete"] is False
    assert result["stop_reason"] in {"backend_error", "backend_warning"}
    assert result["warnings"]
