import asyncio
import io
import json
import os
import zipfile
from pathlib import Path

import pytest

from mypr_mcp.document_tools import DocumentExtractor
from mypr_mcp.filesystem import Filesystem
from mypr_mcp.media_tools import Documents


def _extractor(tmp_path):
    return DocumentExtractor(Filesystem(tmp_path))


def _python_with_document_deps(monkeypatch):
    from mypr_mcp import document_tools

    worker_python = os.environ.get("MYPR_DOCUMENT_TEST_PYTHON")
    if worker_python:
        monkeypatch.setattr(document_tools, "_WORKER_PYTHON", worker_python)


async def _collect_pages(docs, path, **kwargs):
    result = await docs.extract(path, **kwargs)
    items = list(result["items"])
    while result["next_cursor"]:
        result = await docs.extract(path, cursor=result["next_cursor"], **kwargs)
        items.extend(result["items"])
    return result, items


async def test_docx_long_unicode_text_is_chunked_and_paged(tmp_path, monkeypatch):
    docx = pytest.importorskip("docx")
    _python_with_document_deps(monkeypatch)
    path = tmp_path / "long.docx"
    text = ("한글  a  b 🐈\n" * 1300) + "끝"
    document = docx.Document()
    document.add_paragraph(text)
    table = document.add_table(rows=1, cols=1)
    table.cell(0, 0).text = "table value"
    nested = table.cell(0, 0).add_table(rows=1, cols=1)
    nested.cell(0, 0).text = "nested table value"
    document.save(path)

    docs = Documents(Filesystem(tmp_path))
    first = await docs.extract(path.name, max_bytes=16_384)
    pages = [first]
    while pages[-1]["next_cursor"]:
        pages.append(
            await docs.extract(
                path.name,
                cursor=pages[-1]["next_cursor"],
                max_bytes=16_384,
            )
        )
    items = [item for page in pages for item in page["items"]]
    paragraph = [item for item in items if item["location"] == {"paragraph": 1}]

    assert "".join(item["text"] for item in paragraph) == text
    assert [item["offset"] for item in paragraph] == list(range(0, len(text), 1024))
    assert paragraph[0]["part"] == 1
    assert all(len(json.dumps(page).encode()) <= 16_384 for page in pages)
    assert any(item["type"] == "table_cell" and item["text"] == "table value" for item in items)
    assert any(
        item["type"] == "table_cell" and item["text"] == "nested table value" for item in items
    )
    assert first["source"]["revision"]
    assert pages[-1]["next_cursor"] is None


async def test_pptx_and_xlsx_extract_text_and_keep_formula_whitespace(tmp_path, monkeypatch):
    pptx = pytest.importorskip("pptx")
    openpyxl = pytest.importorskip("openpyxl")
    _python_with_document_deps(monkeypatch)

    presentation = pptx.Presentation()
    slide = presentation.slides.add_slide(presentation.slide_layouts[6])
    shape = slide.shapes.add_textbox(100, 100, 500, 100)
    shape.text = "Slide text"
    group = slide.shapes.add_group_shape()
    nested_shape = group.shapes.add_textbox(100, 250, 500, 100)
    nested_shape.text = "Grouped text"
    deck_path = tmp_path / "notes.pptx"
    presentation.save(deck_path)
    deck = await _extractor(tmp_path).extract(deck_path.name)
    assert any(item["text"] == "Slide text" for item in deck["items"])
    assert any(item["text"] == "Grouped text" for item in deck["items"])
    assert deck["items"][0]["location"]["slide"] == 1

    workbook = openpyxl.Workbook()
    workbook.active["A1"] = '=IF(A1="a  b", 1, 0)'
    workbook.active["A2"] = "x  y"
    sheet_path = tmp_path / "values.xlsx"
    workbook.save(sheet_path)
    sheet = await _extractor(tmp_path).extract(sheet_path.name)
    values = {item["location"]["cell"]: item["text"] for item in sheet["items"]}
    assert values["A1"] == '=IF(A1="a  b", 1, 0)'
    assert values["A2"] == "x  y"


async def test_ocr_exif_orientation_and_pdf_page_continuation(tmp_path, monkeypatch):
    pillow = pytest.importorskip("PIL.Image")
    pymupdf = pytest.importorskip("pymupdf")
    from PIL import ImageDraw

    _python_with_document_deps(monkeypatch)
    image_path = tmp_path / "oriented.jpg"
    image = pillow.new("RGB", (400, 160), "white")
    ImageDraw.Draw(image).text((20, 35), "MYPR OCR", fill="black")
    exif = pillow.Exif()
    exif[274] = 6
    image.save(image_path, exif=exif)

    docs = _extractor(tmp_path)
    image_result = await docs.ocr(image_path.name)
    assert image_result["coordinate_space"] == "exif_normalized_image_pixels"
    assert (image_result["pages"][0]["width"], image_result["pages"][0]["height"]) == (
        160,
        400,
    )
    assert all("confidence" in item and len(item["bbox"]) == 4 for item in image_result["items"])

    pdf_path = tmp_path / "two-pages.pdf"
    pdf = pymupdf.open()
    for text in ("First Page", "Second Page"):
        page = pdf.new_page(width=320, height=240)
        page.insert_text((30, 60), text, fontsize=20)
    pdf.save(pdf_path)
    pdf.close()
    first = await docs.ocr(pdf_path.name, max_pages=1)
    assert first["next_page"] == 2
    assert first["truncation_reason"] == "page_limit"
    assert "First" in " ".join(item["text"] for item in first["items"])
    second = await docs.ocr(pdf_path.name, start_page=first["next_page"], max_pages=1)
    assert second["pages"][0]["page"] == 2
    assert second["complete"]
    assert "Second" in " ".join(item["text"] for item in second["items"])


async def test_backends_reports_tesseract_and_installed_languages(tmp_path, monkeypatch):
    pytest.importorskip("PIL.Image")
    _python_with_document_deps(monkeypatch)
    result = await _extractor(tmp_path).backends()
    assert result["tesseract"]["available"] is True
    assert "eng" in result["tesseract"]["languages"]
    assert "python-docx" in result["packages"]


def test_missing_dependency_has_workspace_install_hint(monkeypatch):
    import builtins

    from mypr_mcp import document_worker

    original_import = builtins.__import__

    def missing(name, *args, **kwargs):
        if name == "not_installed_for_test":
            raise ImportError(name)
        return original_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", missing)
    with pytest.raises(document_worker._Failure, match="ws.packages.add"):
        document_worker._dependency("not_installed_for_test", "python-docx")


def test_office_zip_expansion_limit_is_checked_before_parser_runs():
    from mypr_mcp import document_worker

    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zipped:
        zipped.writestr("word/document.xml", b"a" * (512 * 1024))
    with pytest.raises(document_worker._Failure, match="compression ratio limit"):
        document_worker._archive(archive.getvalue())


async def test_xlsx_sparse_dimension_has_a_scan_limit(tmp_path, monkeypatch):
    openpyxl = pytest.importorskip("openpyxl")
    from mypr_mcp import document_worker

    path = tmp_path / "sparse.xlsx"
    workbook = openpyxl.Workbook()
    workbook.active["XFD1048576"] = "last"
    workbook.save(path)
    monkeypatch.setattr(document_worker, "_MAX_SCANNED_CELLS", 100)
    request = {
        "path": str(path),
        "display": path.name,
        "max_input_bytes": 64 * 1024 * 1024,
        "cached_values": False,
    }
    result = document_worker._extract(request)
    assert result["complete"] is False
    assert result["truncation_reason"] == "cell_scan_limit"


async def _wait_for_pid(path: Path) -> int:
    for _ in range(200):
        try:
            return int(await asyncio.to_thread(path.read_text))
        except FileNotFoundError:
            pass
        await asyncio.sleep(0.01)
    pytest.fail("document worker did not start")


async def _assert_reaped(pid: int) -> None:
    for _ in range(100):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return
        await asyncio.sleep(0.02)
    pytest.fail(f"document worker {pid} is still alive")


async def test_worker_output_cap_drains_and_kills_process_group(tmp_path, monkeypatch):
    from mypr_mcp import document_tools

    pid_path = tmp_path / "worker.pid"
    worker = tmp_path / "flood.py"
    worker.write_text(
        "import os\n"
        f"open({str(pid_path)!r}, 'w').write(str(os.getpid()))\n"
        "block=b'x'*65536\n"
        "while True: os.write(1, block)\n"
    )
    monkeypatch.setattr(document_tools, "_WORKER", worker)
    monkeypatch.setattr(document_tools, "_MAX_WORKER_OUTPUT", 4096)
    with pytest.raises(document_tools.DocumentToolError, match="response exceeded"):
        await asyncio.wait_for(document_tools._run_worker("test", None, None, {}), timeout=5)
    await _assert_reaped(int(pid_path.read_text()))


async def test_cancelled_worker_removes_temp_tree_and_reaps_child(tmp_path, monkeypatch):
    from mypr_mcp import document_tools

    pid_path = tmp_path / "worker.pid"
    temp_path = tmp_path / "worker.tmp"
    worker = tmp_path / "stall.py"
    worker.write_text(
        "import os, time\n"
        f"open({str(pid_path)!r}, 'w').write(str(os.getpid()))\n"
        f"open({str(temp_path)!r}, 'w').write(os.environ['TMPDIR'])\n"
        "time.sleep(30)\n"
    )
    monkeypatch.setattr(document_tools, "_WORKER", worker)
    task = asyncio.create_task(document_tools._run_worker("test", None, None, {}))
    pid = await _wait_for_pid(pid_path)
    temporary = Path(temp_path.read_text())
    task.cancel()
    await asyncio.sleep(0.02)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=10)
    await _assert_reaped(pid)
    assert not await asyncio.to_thread(temporary.exists)


def test_failed_snapshot_install_preserves_existing_cursors(tmp_path, monkeypatch):
    from mypr_mcp import document_tools

    store = document_tools._ResultStore(tmp_path)
    first = store.create({"kind": "extract", "source": {"path": "x"}, "items": ["old"]})
    second = store.create({"kind": "extract", "source": {"path": "x"}, "items": ["middle"]})
    monkeypatch.setattr(document_tools, "_MAX_RESULTS", 2)

    def fail_replace(*_args):
        raise OSError("simulated replace failure")

    monkeypatch.setattr(document_tools.os, "replace", fail_replace)
    with pytest.raises(OSError, match="simulated"):
        store.create({"kind": "extract", "source": {"path": "x"}, "items": ["new"]})
    assert store.load(first)["items"] == ["old"]
    assert store.load(second)["items"] == ["middle"]


def test_result_store_retains_new_snapshot_before_pruning(tmp_path, monkeypatch):
    from mypr_mcp import document_tools

    store = document_tools._ResultStore(tmp_path)
    monkeypatch.setattr(document_tools, "_MAX_RESULTS", 2)
    first = store.create({"kind": "extract", "source": {"path": "x"}, "items": ["old"]})
    second = store.create({"kind": "extract", "source": {"path": "x"}, "items": ["middle"]})
    third = store.create({"kind": "extract", "source": {"path": "x"}, "items": ["new"]})
    with pytest.raises(ValueError, match="expired"):
        store.load(first)
    assert store.load(second)["items"] == ["middle"]
    assert store.load(third)["items"] == ["new"]
