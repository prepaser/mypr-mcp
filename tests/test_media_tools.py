import asyncio
import hashlib
import io
import os
from pathlib import Path

import pytest

from mypr_mcp.filesystem import Filesystem
from mypr_mcp.media_tools import Documents


async def _worker_pid(pid_path: Path) -> int:
    for _ in range(100):
        try:
            return int(await asyncio.to_thread(pid_path.read_text))
        except FileNotFoundError:
            pass
        await asyncio.sleep(0.01)
    pytest.fail("media worker did not start")


def _assert_process_reaped(pid: int) -> None:
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


async def test_image_info_and_transform_are_bounded_and_keep_source(tmp_path):
    pillow_image = pytest.importorskip("PIL.Image")
    image_path = tmp_path / "screen.png"
    original = pillow_image.new("RGB", (20, 10), "navy")
    original.save(image_path)
    source = image_path.read_bytes()

    fs = Filesystem(tmp_path)
    info = await fs.image_info(image_path.name)
    assert info == {
        "path": image_path.name,
        "revision": hashlib.sha256(source).hexdigest(),
        "size_bytes": len(source),
        "format": "PNG",
        "mode": "RGB",
        "width": 20,
        "height": 10,
    }

    transformed = await fs.image(
        image_path.name, crop=(2, 1, 18, 9), resize=(4, 4)
    )
    assert image_path.read_bytes() == source
    rendered = pillow_image.open(io.BytesIO(transformed.data))
    assert rendered.size == (4, 2)
    bundle, metadata = transformed._repr_mimebundle_()
    assert "image/png" in bundle
    assert "screen.png" in bundle["text/plain"]
    assert "crop [2, 1, 18, 9]px" in bundle["text/plain"]
    assert "fit [4, 4]px" in bundle["text/plain"]
    assert metadata["image/png"]["mypr"]["revision"] == info["revision"]
    with pytest.raises(FileNotFoundError):
        await fs.image("missing.png", resize=(4, 4))


async def test_pdf_read_cursor_reconstructs_pages_without_mixing_revisions(tmp_path):
    pymupdf = pytest.importorskip("pymupdf")
    pdf_path = tmp_path / "report.pdf"
    document = pymupdf.open()
    for number in range(1, 4):
        page = document.new_page(width=300, height=400)
        content = f"Page {number}: " + ("continuation text " * 8)
        page.insert_textbox(pymupdf.Rect(30, 40, 270, 300), content, fontsize=12)
    document.save(pdf_path)
    document.close()
    with pymupdf.open(pdf_path) as saved:
        expected = [page.get_text("text", sort=True) for page in saved]

    docs = Documents(Filesystem(tmp_path))
    pieces = []
    cursor = None
    while True:
        result = await docs.read(
            pdf_path.name,
            max_pages=2,
            max_chars=17,
            **({"cursor": cursor} if cursor else {}),
        )
        pieces.extend(page["text"] for page in result["pages"])
        cursor = result["next_cursor"]
        if cursor is None:
            break
    assert "".join(pieces) == "".join(expected)

    first = await docs.read(pdf_path.name, max_chars=5)
    assert first["pages"][0]["truncated"] is True
    assert first["next_cursor"]
    pdf_path.write_bytes(pdf_path.read_bytes() + b" ")
    with pytest.raises(ValueError, match="changed"):
        await docs.read(pdf_path.name, cursor=first["next_cursor"])


async def test_pdf_info_and_render_expose_page_provenance(tmp_path):
    pymupdf = pytest.importorskip("pymupdf")
    pdf_path = tmp_path / "one-page.pdf"
    document = pymupdf.open()
    page = document.new_page(width=300, height=400)
    page.insert_text((30, 60), "PDF media test", fontsize=16)
    document.save(pdf_path)
    document.close()

    docs = Documents(Filesystem(tmp_path))
    info = await docs.info(pdf_path.name, page=1)
    assert info["path"] == pdf_path.name
    assert len(info["revision"]) == 64
    assert info["page_count"] == 1
    assert info["bounds"] == [0.0, 0.0, 300.0, 400.0]

    image = await docs.render_page(pdf_path.name, 1, dpi=72, clip=(20, 20, 200, 150))
    assert image.format == "png"
    bundle, metadata = image._repr_mimebundle_()
    assert "image/png" in bundle
    assert "one-page.pdf" in bundle["text/plain"]
    assert "page 1" in bundle["text/plain"]
    assert "clip [20.0, 20.0, 200.0, 150.0]pt" in bundle["text/plain"]
    assert metadata["image/png"]["mypr"]["revision"] == info["revision"]


async def test_media_worker_output_is_capped_and_process_reaped(tmp_path, monkeypatch):
    from mypr_mcp import media_tools

    pid_path = tmp_path / "worker.pid"
    tmp_pid_path = tmp_path / "worker.pid.tmp"
    worker = tmp_path / "flood.py"
    worker.write_text(
        "import os, sys\n"
        f"with open({str(tmp_pid_path)!r}, 'w') as pid_file: pid_file.write(str(os.getpid()))\n"
        f"os.replace({str(tmp_pid_path)!r}, {str(pid_path)!r})\n"
        "block = b'x' * 65536\n"
        "while True: os.write(1, block)\n"
    )
    monkeypatch.setattr(media_tools, "_WORKER", worker)

    with pytest.raises(media_tools.MediaToolError, match="response exceeded its size limit"):
        await media_tools._call("test", tmp_path / "unused", "unused", {})
    _assert_process_reaped(int(pid_path.read_text()))


async def test_cancelled_media_worker_is_reaped(tmp_path, monkeypatch):
    from mypr_mcp import media_tools

    pid_path = tmp_path / "worker.pid"
    tmp_pid_path = tmp_path / "worker.pid.tmp"
    worker = tmp_path / "stall.py"
    worker.write_text(
        "import os, time\n"
        f"with open({str(tmp_pid_path)!r}, 'w') as pid_file: pid_file.write(str(os.getpid()))\n"
        f"os.replace({str(tmp_pid_path)!r}, {str(pid_path)!r})\n"
        "time.sleep(30)\n"
    )
    monkeypatch.setattr(media_tools, "_WORKER", worker)
    task = asyncio.create_task(
        media_tools._call("test", tmp_path / "unused", "unused", {})
    )
    pid = await _worker_pid(pid_path)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    _assert_process_reaped(pid)
