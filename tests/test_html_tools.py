from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import os
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from urllib.parse import urlsplit

import pytest
import pytest_asyncio

from mypr_mcp import html_tools
from mypr_mcp.html_tools import MAX_INPUT_BYTES, HTMLToolError
from mypr_mcp.http_tools import HTTPTools


def _worker_python() -> str:
    override = os.environ.get("MYPR_HTML_TEST_PYTHON")
    if override:
        return override
    if importlib.util.find_spec("trafilatura") and importlib.util.find_spec("cssselect"):
        return sys.executable
    return ""


@pytest_asyncio.fixture
async def http_tools(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[HTTPTools]:
    worker_python = _worker_python()
    if worker_python:
        monkeypatch.setattr(html_tools, "_WORKER_PYTHON", worker_python)
    tools = HTTPTools(tmp_path, lambda: "html-test-client")
    yield tools
    await tools.aclose()


async def _serve(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        raw = await reader.readuntil(b"\r\n\r\n")
        lines = raw[:-4].decode().split("\r\n")
        _, target, _ = lines[0].split(" ", 2)
        headers = dict(line.split(": ", 1) for line in lines[1:] if ": " in line)
        path = urlsplit(target).path
        status, reason = 200, "OK"
        response_headers = {"Content-Type": "text/html; charset=utf-8"}
        if path == "/set-cookie":
            response_headers["Set-Cookie"] = "session=kept; Path=/"
            body = b"<title>cookie</title>"
        elif path == "/article" and headers.get("Cookie") == "session=kept":
            content = " ".join(f"cookie survived paragraph {index}." for index in range(80))
            body = (
                "<html><title>Cookie article</title><article><p>"
                + content
                + "</p></article></html>"
            ).encode()
        elif path == "/article":
            status, reason, body = 401, "Unauthorized", b"missing cookie"
        else:
            status, reason, body = 200, "OK", b"<title>ok</title><p>ok</p>"
        writer.write(
            f"HTTP/1.1 {status} {reason}\r\n".encode()
            + b"\r\n".join(
                f"{key}: {value}".encode() for key, value in response_headers.items()
            )
            + f"\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
            + body
        )
        await writer.drain()
    except (asyncio.IncompleteReadError, ConnectionError, BrokenPipeError):
        pass
    finally:
        writer.close()
        await writer.wait_closed()


@pytest_asyncio.fixture
async def html_server() -> AsyncIterator[str]:
    server = await asyncio.start_server(_serve, "127.0.0.1", 0)
    address = server.sockets[0].getsockname()
    yield f"http://{address[0]}:{address[1]}"
    server.close()
    await server.wait_closed()


def _require_extractor() -> None:
    if not _worker_python():
        pytest.skip("HTML extraction tests need optional trafilatura and cssselect packages")


async def test_extract_html_pages_main_text_and_resolves_base_links(
    http_tools: HTTPTools,
):
    _require_extractor()
    source = (
        '<html><head><title>Example title</title><base href="https://example.org/docs/"></head>'
        '<body><nav>navigation noise</nav><article id="main"><h1>Story</h1>'
        f"<p>{'useful sentence. ' * 700}</p><a href='chapter/2'>Next chapter</a>"
        "<script>script secret</script><style>.hidden { display: none }</style></article>"
        "<aside>unselected noise</aside></body></html>"
    )
    first = await http_tools.extract_html(
        source, url="https://origin.example/page", selector="#main", max_bytes=4096
    )
    assert first["title"] == "Example title"
    assert first["source_hash"] == hashlib.sha256(source.encode()).hexdigest()
    assert "useful sentence." in first["text"]
    assert "script secret" not in first["text"]
    assert first["truncated"] is True
    assert first["complete"] is True
    assert first["links"] == []
    assert len(json.dumps(first, ensure_ascii=False, separators=(",", ":")).encode()) <= 4096

    pages = [first]
    cursor = first["next_cursor"]
    while cursor:
        page = await http_tools.extract_html(cursor=cursor, max_bytes=4096)
        pages.append(page)
        assert len(json.dumps(page, ensure_ascii=False, separators=(",", ":")).encode()) <= 4096
        cursor = page["next_cursor"]
    text = "".join(page["text"] for page in pages)
    links = [link for page in pages for link in page["links"]]
    assert text.count("useful sentence.") >= 600
    assert "navigation noise" not in text
    assert "unselected noise" not in text
    assert links == [{"url": "https://example.org/docs/chapter/2", "text": "Next chapter"}]
    assert pages[-1]["has_more"] is False
    assert await http_tools.extract_html(cursor=first["page_cursor"], max_bytes=4096) == first


async def test_html_cursor_is_client_scoped(http_tools: HTTPTools):
    _require_extractor()
    paragraphs = "".join(
        f"<p>Paragraph {index}: text about extraction and paging.</p>" for index in range(500)
    )
    long_html = (
        "<html><head><title>Paging</title></head><body><article>"
        f"{paragraphs}</article></body></html>"
    )
    page = await http_tools.extract_html(long_html, max_bytes=4096)
    assert page["next_cursor"]
    http_tools._identity = lambda: "another-client"
    with pytest.raises(ValueError, match="expired or belongs to another client"):
        await http_tools.extract_html(cursor=page["next_cursor"], max_bytes=4096)


async def test_short_pages_fall_back_to_clean_visible_text(http_tools: HTTPTools):
    _require_extractor()
    result = await http_tools.extract_html("<p>Hello.</p>")
    assert result["title"] == ""
    assert result["text"] == "Hello."
    assert result["warnings"] == [
        "main-text extraction was empty; returned cleaned visible text"
    ]


@pytest.mark.parametrize("charset", ["", '<meta charset="utf-8">', '<meta charset="windows-1252">'])
@pytest.mark.parametrize("selector", [None, "#main"])
async def test_extraction_preserves_unicode(
    http_tools: HTTPTools, charset: str, selector: str | None
):
    _require_extractor()
    paragraph = "한국어 문서의 원문을 정확하게 읽어야 합니다. " * 20
    source = (
        f"<html><head>{charset}<title>한글 제목</title></head>"
        f'<body><article id="main"><p>{paragraph}</p></article></body></html>'
    )
    result = await http_tools.extract_html(source, selector=selector)
    assert result["title"] == "한글 제목"
    assert result["text"].strip() == paragraph.strip()
    assert result["complete"] is True


@pytest.mark.parametrize("tag", ["script", "style"])
async def test_removing_hidden_elements_preserves_visible_tail(
    http_tools: HTTPTools, tag: str
):
    _require_extractor()
    result = await http_tools.extract_html(
        f"<p>START visible content. <{tag}>hidden</{tag}> END text must survive.</p>"
    )
    assert result["text"] == "START visible content. END text must survive."
    assert result["complete"] is True


async def test_selector_excludes_text_outside_selected_element(http_tools: HTTPTools):
    _require_extractor()
    result = await http_tools.extract_html(
        '<html><body><span id="wanted">Selected text.</span> Outside text.</body></html>',
        selector="#wanted",
    )
    assert result["text"] == "Selected text."
    assert result["complete"] is True


async def test_read_html_reuses_named_clients_and_cookies(
    http_tools: HTTPTools, html_server: str
):
    _require_extractor()
    response = await http_tools.get(f"{html_server}/set-cookie", name="browser-session")
    assert response.status_code == 200
    result = await http_tools.read_html(f"{html_server}/article", name="browser-session")
    assert result["title"] == "Cookie article"
    assert "cookie survived" in result["text"]
    assert result["url"] == f"{html_server}/article"


async def test_missing_optional_dependency_has_install_hint(
    http_tools: HTTPTools, monkeypatch: pytest.MonkeyPatch
):
    if importlib.util.find_spec("trafilatura"):
        pytest.skip("trafilatura is installed in this test interpreter")
    monkeypatch.setattr(html_tools, "_WORKER_PYTHON", sys.executable)
    with pytest.raises(ImportError, match=r"ws.packages.add\('trafilatura'"):
        await http_tools.extract_html("<html><body><p>hello</p></body></html>")


async def test_input_limit_is_checked_before_worker_launch(http_tools: HTTPTools):
    with pytest.raises(ValueError, match="input limit"):
        await http_tools.extract_html("x" * (MAX_INPUT_BYTES + 1))


async def test_worker_accepts_maximum_unicode_header(tmp_path, monkeypatch):
    worker = tmp_path / "header_worker.py"
    worker.write_text(
        "import sys\n"
        "header = sys.stdin.buffer.readline(64 * 1024 + 2)\n"
        "assert len(header) == 64 * 1024 + 1 and header.endswith(b'\\n')\n"
        "sys.stdin.buffer.read()\n"
        "sys.stdout.write('{\"ok\":true,\"result\":{}}')\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(html_tools, "_WORKER", worker)
    url = "😀" * 4096
    selector = "😀" * 4096
    initial = html_tools.json_bytes(
        {"url": url, "selector": selector, "include_structure": False}, separators=(",", ":")
    )
    url += "x" * (html_tools._MAX_HEADER_BYTES - len(initial))
    result = await html_tools._run_worker("<p>header</p>", url=url, selector=selector)
    assert result == {}


async def test_cancellation_terminates_guarded_worker_tree(
    http_tools: HTTPTools, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    pid_file = tmp_path / "worker.pid"
    worker = tmp_path / "stalled_worker.py"
    child_code = (
        "import os, time\n"
        f"open({str(pid_file)!r}, 'w').write(str(os.getpid()))\n"
        "time.sleep(60)\n"
    )
    worker.write_text(
        "import subprocess, sys, time\n"
        f"child_code = {child_code!r}\n"
        "subprocess.Popen([sys.executable, '-I', '-c', child_code], start_new_session=True)\n"
        "time.sleep(60)\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(html_tools, "_WORKER", worker)
    task = asyncio.create_task(http_tools.extract_html("<p>cancel</p>"))
    for _ in range(100):
        if pid_file.exists():
            break
        await asyncio.sleep(0.02)
    assert pid_file.exists()
    child_pid = int(pid_file.read_text())
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    for _ in range(100):
        try:
            stat = await asyncio.to_thread(Path(f"/proc/{child_pid}/stat").read_text)
        except ProcessLookupError:
            break
        except FileNotFoundError:
            break
        if stat.split(") ", 1)[1].split()[0] in {"Z", "X"}:
            break
        await asyncio.sleep(0.02)
    else:
        pytest.fail(f"detached worker descendant {child_pid} remained alive after cancellation")


async def test_timeout_terminates_guarded_worker_tree(
    http_tools: HTTPTools, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    worker = tmp_path / "stalled_worker.py"
    worker.write_text("import time\ntime.sleep(60)\n", encoding="utf-8")
    monkeypatch.setattr(html_tools, "_WORKER", worker)
    monkeypatch.setattr(html_tools, "_TIMEOUT", 0.1)
    with pytest.raises(HTMLToolError, match="20-second time limit"):
        await http_tools.extract_html("<p>timeout</p>")


async def test_output_limit_reaps_detached_worker_descendants(
    http_tools: HTTPTools, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    pid_file = tmp_path / "detached.pid"
    worker = tmp_path / "flooding_worker.py"
    child_code = (
        "import os, time\n"
        f"pid_file = {str(pid_file)!r}\n"
        "open(pid_file, 'w').write(str(os.getpid()))\n"
        "time.sleep(60)\n"
    )
    worker_code = (
        "import os, subprocess, sys, time\n"
        f"pid_file = {str(pid_file)!r}\n"
        f"child_code = {child_code!r}\n"
        "subprocess.Popen([sys.executable, '-I', '-c', child_code], start_new_session=True)\n"
        "while not os.path.exists(pid_file): time.sleep(0.01)\n"
        "time.sleep(0.2)\n"
        "sys.stdout.buffer.write(b'x' * 262144)\n"
        "sys.stdout.flush()\n"
        "time.sleep(60)\n"
    )
    worker.write_text(worker_code, encoding="utf-8")
    monkeypatch.setattr(html_tools, "_WORKER", worker)
    monkeypatch.setattr(html_tools, "_MAX_WORKER_OUTPUT", 128 * 1024)
    with pytest.raises(HTMLToolError, match="response exceeded its size limit"):
        await http_tools.extract_html("<p>overflow</p>")
    child_pid = int(pid_file.read_text())
    for _ in range(100):
        try:
            stat = await asyncio.to_thread(Path(f"/proc/{child_pid}/stat").read_text)
            state = stat.split(") ", 1)[1].split()[0]
        except FileNotFoundError:
            break
        if state in {"Z", "X"}:
            break
        await asyncio.sleep(0.02)
    else:
        pytest.fail(f"detached worker descendant {child_pid} remained alive")
