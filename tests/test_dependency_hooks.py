from __future__ import annotations

import asyncio

import pytest

from mypr_mcp.document_tools import DocumentExtractor
from mypr_mcp.filesystem import Filesystem
from mypr_mcp.http_tools import HTTPTools
from mypr_mcp.search import Search


class _Runner:
    def __init__(self, stdout: str = ""):
        self.stdout = stdout
        self.commands = []
        self.environments = []

    async def run(  # noqa: ASYNC109
        self, command, *, cwd, check=False, timeout=None, max_bytes=0, env=None  # noqa: ASYNC109
    ):
        self.commands.append(command)
        self.environments.append(env)
        return {
            "returncode": 0 if self.stdout else 1,
            "stdout": self.stdout,
            "stderr": "",
            "truncated": False,
            "timed_out": False,
        }


@pytest.mark.asyncio
async def test_filesystem_and_search_use_automatic_dependency_callback(tmp_path, monkeypatch):
    calls = []

    async def ensure(*names, automatic=False):
        calls.append((names, automatic))
        return {"items": [{"name": name, "path": f"/managed/{name}"} for name in names]}

    async def inspect_image(*args, **kwargs):
        return {"path": "image.png"}

    monkeypatch.setattr("mypr_mcp.media_tools.inspect_image", inspect_image)
    (tmp_path / "image.png").write_bytes(b"png")
    fs = Filesystem(tmp_path, ensure_dependencies=ensure)
    await fs.image_info("image.png")
    runner = _Runner()
    await Search(tmp_path, runner, ensure_dependencies=ensure).search("needle")

    assert calls == [(('pillow',), True), (('rg',), True)]
    assert runner.commands[0][0] == "/managed/rg"


@pytest.mark.asyncio
async def test_search_cursor_does_not_prepare_dependencies(tmp_path):
    runner = _Runner(
        '{"type":"match","data":{"path":{"text":"a.txt"},'
        '"lines":{"text":"needle\\n"},"line_number":1,"absolute_offset":0,'
        '"submatches":[{"match":{"text":"needle"},"start":0,"end":6}}}}\n'
    )
    first = await Search(tmp_path, runner).search("needle", max_matches=1)
    calls = []

    async def fail(*names, automatic=False):
        calls.append(names)
        raise AssertionError("cursor must not install dependencies")

    if first["next_cursor"]:
        await Search(tmp_path, runner, ensure_dependencies=fail).search(
            cursor=first["next_cursor"]
        )
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "adapters,needs_pandoc",
    [
        (None, False),
        (["pandoc"], True),
        ("pandoc,poppler", True),
        ("+pandoc", True),
        (["-poppler", "pandoc"], False),
    ],
)
async def test_document_search_prepares_only_requested_converters(tmp_path, adapters, needs_pandoc):
    calls = []

    async def ensure(*names, automatic=False):
        calls.append((names, automatic))
        assert "pandoc" not in names or needs_pandoc
        return {"items": [{"name": name, "path": f"/managed/{name}"} for name in names]}

    runner = _Runner()
    fs = Filesystem(tmp_path, runner, ensure_dependencies=ensure)
    await fs.search_docs("needle", paths="fixture.txt", adapters=adapters)

    expected = ("rg", "rga", "pandoc") if needs_pandoc else ("rg", "rga")
    assert calls == [(expected, True)]
    assert runner.commands[0][0] == "/managed/rga"


@pytest.mark.asyncio
async def test_html_selector_requests_cssselect_only_when_needed(tmp_path, monkeypatch):
    calls = []

    async def ensure(*names, automatic=False):
        calls.append((names, automatic))
        return {}

    async def worker(*args, **kwargs):
        return {"text": "ok", "links": [], "source_hash": "a" * 64, "complete": True}

    monkeypatch.setattr("mypr_mcp.html_tools._run_worker", worker)
    tools = HTTPTools(tmp_path, ensure_dependencies=ensure)
    await tools.extract_html("<p>ok</p>")
    await tools.extract_html("<p>ok</p>", selector="p")
    assert calls == [(('trafilatura',), True), (('trafilatura', 'cssselect'), True)]


@pytest.mark.asyncio
async def test_document_extract_prepares_package_but_cached_page_does_not(tmp_path, monkeypatch):
    calls = []

    async def ensure(*names, automatic=False):
        calls.append((names, automatic))
        return {}

    async def worker(*args, **kwargs):
        return {
            "source": {"path": "sample.docx", "revision": "a" * 64},
            "items": [{"type": "paragraph", "text": "x" * 100} for _ in range(1000)],
            "complete": True,
            "truncated": False,
        }

    monkeypatch.setattr("mypr_mcp.document_tools._run_worker", worker)
    (tmp_path / "sample.docx").write_bytes(b"docx")
    docs = DocumentExtractor(Filesystem(tmp_path, ensure_dependencies=ensure))
    first = await docs.extract("sample.docx")
    assert calls == [(('python-docx',), True)]
    assert first["next_cursor"]
    await docs.extract("sample.docx", cursor=first["next_cursor"])
    assert calls == [(('python-docx',), True)]


@pytest.mark.asyncio
async def test_raw_image_does_not_prepare_pillow(tmp_path, monkeypatch):
    calls = []

    async def ensure(*names, automatic=False):
        calls.append(names)
        return {}

    path = tmp_path / "image.png"
    path.write_bytes(b"png")
    monkeypatch.setattr("mypr_mcp.filesystem._read_image", lambda *args: (b"png", "png"))
    image = await Filesystem(tmp_path, ensure_dependencies=ensure).image(path.name)
    assert image.data == b"png"
    assert calls == []


@pytest.mark.asyncio
async def test_ocr_prepares_all_requested_models_and_checks_tesseract_version(
    tmp_path, monkeypatch
):
    calls = []

    async def ensure(*names, automatic=False):
        calls.append(names)
        return {
            "items": [
                {"name": name, "model_dir": "/managed/models"}
                for name in names
                if name.startswith("tessdata:")
            ]
        }

    monkeypatch.setattr("mypr_mcp.document_tools.shutil.which", lambda name: "/usr/bin/tesseract")
    monkeypatch.setattr(
        "mypr_mcp.document_tools._tesseract_languages",
        lambda executable: asyncio.sleep(0, result={"eng"}),
    )
    monkeypatch.setattr(
        "mypr_mcp.document_tools._tesseract_version",
        lambda executable: asyncio.sleep(0, result=(5, 3, 0)),
    )
    fs = Filesystem(tmp_path, ensure_dependencies=ensure)
    extractor = DocumentExtractor(fs)
    prepared = await extractor._ocr_dependencies("image.png", "eng+kor")
    assert calls == [("pillow", "tessdata:eng", "tessdata:kor")]
    assert prepared["model_dir"] == "/managed/models"


@pytest.mark.asyncio
async def test_invalid_input_fails_before_dependency_preparation(tmp_path):
    calls = []

    async def ensure(*names, automatic=False):
        calls.append(names)
        return {}

    with pytest.raises(FileNotFoundError):
        await Filesystem(tmp_path, ensure_dependencies=ensure).image_info("missing.png")
    assert calls == []


@pytest.mark.asyncio
async def test_old_tesseract_fails_before_model_install(tmp_path, monkeypatch):
    calls = []

    async def ensure(*names, automatic=False):
        calls.append(names)
        return {}

    monkeypatch.setattr("mypr_mcp.document_tools.shutil.which", lambda name: "/usr/bin/tesseract")
    monkeypatch.setattr(
        "mypr_mcp.document_tools._tesseract_languages",
        lambda executable: asyncio.sleep(0, result={"eng"}),
    )
    monkeypatch.setattr(
        "mypr_mcp.document_tools._tesseract_version",
        lambda executable: asyncio.sleep(0, result=(3, 5, 0)),
    )
    extractor = DocumentExtractor(Filesystem(tmp_path, ensure_dependencies=ensure))
    with pytest.raises(RuntimeError, match="Tesseract 4"):
        await extractor._ocr_dependencies("image.png", "eng+kor")
    assert calls == []


@pytest.mark.asyncio
async def test_backends_reports_managed_models_without_installing(tmp_path, monkeypatch):
    calls = []

    async def ensure(*names, automatic=False):
        calls.append(names)
        return {}

    async def worker(*args, **kwargs):
        return {"packages": {}, "tesseract": {"available": False}}

    monkeypatch.setattr("mypr_mcp.document_tools._run_worker", worker)
    monkeypatch.setattr(
        "mypr_mcp.document_tools._managed_model_report",
        lambda: {"source": "shared", "languages": ["eng"], "installed_count": 1},
    )
    result = await DocumentExtractor(Filesystem(tmp_path, ensure_dependencies=ensure)).backends()
    assert result["managed_models"]["languages"] == ["eng"]
    assert calls == []
