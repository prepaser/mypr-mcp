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


async def test_jpeg_exif_orientation_defines_info_and_crop_coordinates(tmp_path):
    pillow_image = pytest.importorskip("PIL.Image")
    image_path = tmp_path / "oriented.jpg"
    source = pillow_image.new("RGB", (120, 80))
    for box, color in (
        ((0, 0, 60, 40), "red"),
        ((60, 0, 120, 40), "green"),
        ((0, 40, 60, 80), "blue"),
        ((60, 40, 120, 80), "yellow"),
    ):
        source.paste(color, box)
    exif = pillow_image.Exif()
    exif[274] = 6
    source.save(image_path, quality=100, subsampling=0, exif=exif)

    fs = Filesystem(tmp_path)
    info = await fs.image_info(image_path.name)
    transformed = await fs.image(image_path.name, crop=(0, 0, 40, 40))
    rendered = pillow_image.open(io.BytesIO(transformed.data))

    assert (info["width"], info["height"]) == (80, 120)
    assert rendered.size == (40, 40)
    red, green, blue = rendered.getpixel((20, 20))
    assert blue > 180 and red < 80 and green < 80
    bundle, metadata = transformed._repr_mimebundle_()
    image_metadata = metadata["image/jpeg"]["mypr"]
    assert image_metadata["original_width"] == info["width"]
    assert image_metadata["original_height"] == info["height"]
    assert image_metadata["crop"] == [0, 0, 40, 40]
    assert "crop [0, 0, 40, 40]px" in bundle["text/plain"]


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
    monkeypatch.setattr(media_tools, "_MAX_WORKER_OUTPUT", 4096)

    with pytest.raises(media_tools.MediaToolError, match="response exceeded its size limit"):
        await asyncio.wait_for(
            media_tools._call("test", tmp_path / "unused", "unused", {}), timeout=2
        )
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
    kill_called = asyncio.Event()
    process_type = asyncio.subprocess.Process
    original_terminate = process_type.terminate

    def terminate(process):
        kill_called.set()
        return original_terminate(process)

    monkeypatch.setattr(process_type, "terminate", terminate)
    task.cancel()
    await kill_called.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    _assert_process_reaped(pid)


async def test_cancelled_media_worker_launch_is_cleaned_up(tmp_path, monkeypatch):
    from mypr_mcp import media_tools

    started = asyncio.Event()
    release = asyncio.Event()

    class Stdin:
        def is_closing(self):
            return True

    class Process:
        pid = 999_999
        returncode = None
        stdin = Stdin()

        def __init__(self):
            self.killed = False

        def kill(self):
            self.killed = True
            self.returncode = -9

        async def wait(self):
            return self.returncode

    process = Process()

    async def launch(*args, **kwargs):
        del args, kwargs
        started.set()
        await release.wait()
        return process

    monkeypatch.setattr(media_tools.asyncio, "create_subprocess_exec", launch)
    task = asyncio.create_task(media_tools._call("test", tmp_path / "unused", "unused", {}))
    await started.wait()
    task.cancel()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert process.killed
