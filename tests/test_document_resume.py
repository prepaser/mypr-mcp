import base64
import hashlib
import json
import zlib

import pytest

from mypr_mcp import document_tools, document_worker
from mypr_mcp.filesystem import Filesystem
from mypr_mcp.media_tools import Documents


def _tsv(words: list[str]) -> bytes:
    rows = [
        "level\tpage_num\tblock_num\tpar_num\tline_num\tword_num\tleft\ttop\twidth\theight\tconf\ttext"
    ]
    rows.extend(
        f"5\t1\t1\t1\t1\t{index}\t{index}\t0\t10\t10\t90\t{word}"
        for index, word in enumerate(words)
    )
    return ("\n".join(rows) + "\n").encode()


def test_ocr_resume_decompression_is_bounded():
    bomb = base64.b64encode(zlib.compress(b"x" * (8 * 1024 * 1024 + 1), 9)).decode()
    with pytest.raises(document_tools.DocumentToolError, match="8 MiB"):
        document_tools._decode_resume_tsv(bomb)


@pytest.mark.asyncio
async def test_ocr_resume_reuses_cached_page_and_binds_source(tmp_path, monkeypatch):
    path = tmp_path / "scan.png"
    path.write_bytes(b"source")
    raw_tsv = _tsv(["one", "two"])
    encoded_tsv = base64.b64encode(zlib.compress(raw_tsv, 6)).decode()
    revision = hashlib.sha256(path.read_bytes()).hexdigest()

    async def fake_worker(*_args, **_kwargs):
        return {
            "source": {
                "path": "scan.png",
                "revision": revision,
                "size_bytes": 6,
                "format": "png",
            },
            "coordinate_space": "image_pixels",
            "pages": [{"page": 1}],
            "items": [],
            "complete": False,
            "truncated": True,
            "truncation_reason": "result_size_limit",
            "next_page": None,
            "warnings": [],
            "resume": {
                "page": 1,
                "width": 10,
                "height": 10,
                "dpi": 200,
                "word_offset": 0,
                "tsv": encoded_tsv,
            },
        }

    monkeypatch.setattr(document_tools, "_run_worker", fake_worker)
    docs = Documents(Filesystem(tmp_path))
    first = await docs.ocr(path.name)
    assert first["resume_cursor"]
    resumed = await docs.ocr(path.name, resume_cursor=first["resume_cursor"])
    assert [item["text"] for item in resumed["items"]] == ["one", "two"]
    assert resumed["complete"] is True

    path.write_bytes(b"changed")
    resumed_again = await docs.ocr(path.name, resume_cursor=first["resume_cursor"])
    assert [item["text"] for item in resumed_again["items"]] == ["one", "two"]


@pytest.mark.asyncio
async def test_ocr_resume_traverses_dense_cached_page(tmp_path, monkeypatch):
    path = tmp_path / "scan.png"
    path.write_bytes(b"source")
    words = [f"word-{index:05d}-{'x' * 80}" for index in range(5000)]
    raw_tsv = _tsv(words)
    encoded_tsv = base64.b64encode(zlib.compress(raw_tsv, 6)).decode()
    revision = hashlib.sha256(path.read_bytes()).hexdigest()

    async def fake_worker(*_args, **_kwargs):
        return {
            "source": {
                "path": "scan.png",
                "revision": revision,
                "size_bytes": 6,
                "format": "png",
            },
            "coordinate_space": "image_pixels",
            "pages": [{"page": 1}],
            "items": [],
            "complete": False,
            "truncated": True,
            "truncation_reason": "result_size_limit",
            "next_page": None,
            "warnings": [],
            "resume": {
                "page": 1,
                "width": 10,
                "height": 10,
                "dpi": 200,
                "word_offset": 0,
                "tsv": encoded_tsv,
            },
        }

    monkeypatch.setattr(document_tools, "_run_worker", fake_worker)
    docs = Documents(Filesystem(tmp_path))
    page = await docs.ocr(path.name, max_bytes=16_384)
    texts = []
    calls = 0
    while page.get("resume_cursor"):
        page = await docs.ocr(
            path.name, resume_cursor=page["resume_cursor"], max_bytes=16_384
        )
        texts.extend(item["text"] for item in page["items"])
        calls += 1
        assert calls < 100
    assert texts == words
    assert page["complete"] is True


@pytest.mark.asyncio
async def test_ocr_rejects_mixed_page_and_resume_cursors(tmp_path):
    path = tmp_path / "scan.png"
    path.write_bytes(b"source")
    docs = Documents(Filesystem(tmp_path))
    with pytest.raises(ValueError, match="cannot be combined"):
        await docs.ocr(path.name, cursor="x", resume_cursor="y")


@pytest.mark.asyncio
async def test_ocr_rejects_tampered_resume_position(tmp_path, monkeypatch):
    path = tmp_path / "scan.png"
    path.write_bytes(b"source")
    raw_tsv = _tsv(["one", "two"])
    encoded_tsv = base64.b64encode(zlib.compress(raw_tsv, 6)).decode()
    revision = hashlib.sha256(path.read_bytes()).hexdigest()

    async def fake_worker(*_args, **_kwargs):
        return {
            "source": {"path": "scan.png", "revision": revision, "size_bytes": 6, "format": "png"},
            "coordinate_space": "image_pixels",
            "pages": [{"page": 1}],
            "items": [],
            "complete": False,
            "truncated": True,
            "truncation_reason": "result_size_limit",
            "next_page": None,
            "warnings": [],
            "resume": {"page": 1, "word_offset": 0, "tsv": encoded_tsv},
        }

    monkeypatch.setattr(document_tools, "_run_worker", fake_worker)
    docs = Documents(Filesystem(tmp_path))
    first = await docs.ocr(path.name)
    payload = json.loads(
        base64.urlsafe_b64decode(first["resume_cursor"] + "=" * (-len(first["resume_cursor"]) % 4))
    )
    payload["offset"] = 1
    tampered = base64.urlsafe_b64encode(json.dumps(payload).encode()).decode().rstrip("=")
    with pytest.raises(ValueError, match="invalid OCR resume cursor"):
        await docs.ocr(path.name, resume_cursor=tampered)


@pytest.mark.asyncio
async def test_ocr_resume_rechecks_source_before_unprocessed_page(tmp_path, monkeypatch):
    path = tmp_path / "scan.png"
    path.write_bytes(b"source")
    raw_tsv = _tsv(["one"])
    encoded_tsv = base64.b64encode(zlib.compress(raw_tsv, 6)).decode()
    revision = hashlib.sha256(path.read_bytes()).hexdigest()
    called = False

    async def fake_worker(*_args, **_kwargs):
        nonlocal called
        called = True
        return {
            "source": {"path": "scan.png", "revision": revision, "size_bytes": 6, "format": "png"},
            "coordinate_space": "image_pixels",
            "pages": [{"page": 1}],
            "items": [],
            "complete": True,
            "truncated": False,
            "truncation_reason": None,
            "next_page": None,
            "warnings": [],
            "resume": {
                "page": 1,
                "width": 10,
                "height": 10,
                "dpi": 200,
                "word_offset": 0,
                "next_page": 2,
                "remaining_pages": 1,
                "tsv": encoded_tsv,
            },
        }

    monkeypatch.setattr(document_tools, "_run_worker", fake_worker)
    docs = Documents(Filesystem(tmp_path))
    first = await docs.ocr(path.name)
    cached = await docs.ocr(path.name, resume_cursor=first["resume_cursor"])
    assert [item["text"] for item in cached["items"]] == ["one"]
    assert cached["resume_cursor"]
    assert cached["next_page"] == 2
    path.write_bytes(b"changed")
    with pytest.raises(ValueError, match="source changed"):
        await docs.ocr(path.name, resume_cursor=cached["resume_cursor"])
    assert called is True


@pytest.mark.asyncio
async def test_ocr_resume_returns_cached_words_before_continuing_page(tmp_path, monkeypatch):
    path = tmp_path / "scan.png"
    path.write_bytes(b"source")
    raw_tsv = _tsv(["one", "two"])
    encoded_tsv = base64.b64encode(zlib.compress(raw_tsv, 6)).decode()
    revision = hashlib.sha256(path.read_bytes()).hexdigest()
    calls = []

    async def fake_worker(_operation, _path, _display, request):
        calls.append(request)
        if len(calls) == 1:
            return {
                "source": {
                    "path": "scan.png",
                    "revision": revision,
                    "size_bytes": 6,
                    "format": "png",
                },
                "coordinate_space": "image_pixels",
                "pages": [{"page": 1}],
                "items": [],
                "complete": False,
                "truncated": True,
                "truncation_reason": "result_size_limit",
                "next_page": None,
                "warnings": [],
                "resume": {
                    "page": 1,
                    "width": 10,
                    "height": 10,
                    "dpi": 200,
                    "word_offset": 0,
                    "next_page": 2,
                    "remaining_pages": 1,
                    "tsv": encoded_tsv,
                },
            }
        return {
            "source": {
                "path": "scan.png",
                "revision": revision,
                "size_bytes": 6,
                "format": "png",
            },
            "coordinate_space": "image_pixels",
            "pages": [{"page": 2}],
            "items": [{"type": "word", "page": 2, "text": "three"}],
            "complete": True,
            "truncated": False,
            "next_page": None,
            "truncation_reason": None,
            "warnings": [],
        }

    monkeypatch.setattr(document_tools, "_run_worker", fake_worker)
    docs = Documents(Filesystem(tmp_path))
    first = await docs.ocr(path.name, max_pages=1)
    cached = await docs.ocr(path.name, resume_cursor=first["resume_cursor"])
    assert [item["text"] for item in cached["items"]] == ["one", "two"]
    assert cached["complete"] is False
    assert cached["truncation_reason"] == "page_limit"
    assert cached["next_page"] == 2

    second = await docs.ocr(path.name, resume_cursor=cached["resume_cursor"])
    assert [item["text"] for item in second["items"]] == ["three"]
    assert calls[1]["start_page"] == 2
    assert calls[1]["max_pages"] == 1


def test_worker_ocr_keeps_page_continuation_after_page_budget(tmp_path, monkeypatch):
    pymupdf = pytest.importorskip("pymupdf")
    path = tmp_path / "three-pages.pdf"
    document = pymupdf.open()
    for _ in range(3):
        document.new_page(width=320, height=240)
    document.save(path)
    document.close()

    monkeypatch.setattr(document_worker.shutil, "which", lambda _: "/usr/bin/tesseract")
    monkeypatch.setattr(document_worker, "_MAX_OUTPUT", 100)
    monkeypatch.setattr(document_worker, "_tesseract", lambda *_args: _tsv(["one"]))
    result = document_worker._ocr(
        {
            "path": str(path),
            "display": path.name,
            "language": "eng",
            "start_page": 1,
            "max_pages": 1,
            "dpi": 200,
            "max_input_bytes": 64 * 1024 * 1024,
        }
    )
    assert result["complete"] is False
    assert result["truncated"] is True
    assert result["resume"]["next_page"] == 2
    assert result["resume"]["remaining_pages"] == 1
    assert result["resume"]["page_limit"] == 1
    assert result["resume"]["page_count"] == 3


def test_worker_ocr_image_resume_keeps_page_metadata(tmp_path, monkeypatch):
    image = pytest.importorskip("PIL.Image")
    path = tmp_path / "scan.png"
    image.new("RGB", (1, 1), "white").save(path)

    monkeypatch.setattr(document_worker.shutil, "which", lambda _: "/usr/bin/tesseract")
    monkeypatch.setattr(document_worker, "_MAX_OUTPUT", 100)
    monkeypatch.setattr(document_worker, "_tesseract", lambda *_args: _tsv(["one"]))
    result = document_worker._ocr(
        {
            "path": str(path),
            "display": path.name,
            "language": "eng",
            "start_page": 1,
            "max_pages": 5,
            "dpi": 200,
            "max_input_bytes": 64 * 1024 * 1024,
        }
    )
    assert result["resume"]["next_page"] is None
    assert result["resume"]["remaining_pages"] == 0
    assert result["resume"]["page_limit"] == 5
    assert result["resume"]["page_count"] == 1
