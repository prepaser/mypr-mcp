"""Isolated HTML parsing worker used by html_tools."""

from __future__ import annotations

import hashlib
import json
import sys
from copy import deepcopy
from urllib.parse import urljoin, urlsplit

_MAX_INPUT_BYTES = 16 * 1024 * 1024
_MAX_TEXT_BYTES = 8 * 1024 * 1024
_MAX_LINK_BYTES = 1024 * 1024
_MAX_LINKS = 10_000


class _Failure(Exception):
    def __init__(self, kind: str, message: str) -> None:
        self.kind = kind
        super().__init__(message)


def _limits() -> None:
    if not sys.platform.startswith("linux"):
        return
    try:
        import resource

        for name, desired in (
            ("RLIMIT_CORE", 0),
            ("RLIMIT_CPU", 15),
            ("RLIMIT_FSIZE", 0),
            ("RLIMIT_NOFILE", 32),
            ("RLIMIT_AS", 768 * 1024 * 1024),
        ):
            limit = getattr(resource, name, None)
            if limit is None:
                continue
            _, hard = resource.getrlimit(limit)
            value = min(desired, hard) if hard != resource.RLIM_INFINITY else desired
            resource.setrlimit(limit, (value, value))
    except (ImportError, OSError, ValueError):
        pass


def _truncate(value: str, limit: int) -> tuple[str, bool]:
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value, False
    return encoded[:limit].decode("utf-8", errors="ignore"), True


def _clean(value: str) -> str:
    return "".join(char for char in value if char in "\n\r\t" or ord(char) >= 32)


def _links(root, base_url: str | None) -> tuple[list[dict[str, str]], bool]:
    links: list[dict[str, str]] = []
    size = 0
    truncated = False
    for anchor in root.iter("a"):
        href = anchor.get("href")
        if not href:
            continue
        try:
            target = urljoin(base_url or "", href.strip())
            if urlsplit(target).scheme.lower() not in {"", "http", "https"}:
                continue
        except ValueError:
            continue
        target, target_cut = _truncate(_clean(target), 512)
        label, label_cut = _truncate(_clean(" ".join(anchor.itertext()).strip()), 128)
        item = {"url": target, "text": label}
        item_size = len(json.dumps(item, ensure_ascii=False, separators=(",", ":")).encode())
        if len(links) >= _MAX_LINKS or size + item_size > _MAX_LINK_BYTES:
            truncated = True
            break
        links.append(item)
        size += item_size
        truncated |= target_cut or label_cut
    return links, truncated


def _extract(request: dict) -> dict:
    html = request.get("html")
    if not isinstance(html, str):
        raise _Failure("ValueError", "html must be a string")
    try:
        encoded = html.encode("utf-8")
    except UnicodeEncodeError:
        raise _Failure("ValueError", "html contains invalid Unicode") from None
    if len(encoded) > _MAX_INPUT_BYTES:
        raise _Failure("ValueError", f"HTML exceeds the {_MAX_INPUT_BYTES} byte input limit")

    url = request.get("url")
    selector = request.get("selector")
    if url is not None and (not isinstance(url, str) or len(url) > 8192):
        raise _Failure("ValueError", "url must be a string no longer than 8192 characters")
    if url is not None:
        try:
            urlsplit(url)
        except ValueError:
            raise _Failure("ValueError", "url is invalid") from None
    if selector is not None and (not isinstance(selector, str) or len(selector) > 4096):
        raise _Failure("ValueError", "selector must be a string no longer than 4096 characters")

    try:
        import trafilatura
        from lxml import etree
        from lxml import html as lxml_html
    except ImportError as exc:
        dependency = "cssselect" if "cssselect" in str(exc) else "trafilatura"
        raise _Failure(
            "MissingDependency",
            f"HTML extraction requires {dependency}. Install it with "
            "`await ws.packages.add('trafilatura', 'cssselect')` and await its task.",
        ) from None

    try:
        parser = lxml_html.HTMLParser(encoding="utf-8")
        root = lxml_html.fromstring(encoded, parser=parser)
    except (etree.ParserError, ValueError) as exc:
        raise _Failure("ValueError", f"Could not parse HTML: {exc}") from None
    if root is None:
        raise _Failure("ValueError", "Could not parse HTML")

    link_base = url
    base_nodes = root.xpath("(//base[@href])[1]")
    if base_nodes:
        try:
            candidate_base = urljoin(url or "", base_nodes[0].get("href", ""))
            if urlsplit(candidate_base).scheme.lower() in {"http", "https"}:
                link_base = candidate_base
        except ValueError:
            pass

    selected = root
    warnings: list[str] = []
    if selector:
        try:
            from lxml.cssselect import CSSSelector

            matches = CSSSelector(selector)(root)
        except ImportError:
            raise _Failure(
                "MissingDependency",
                "CSS selectors require cssselect. Install it with "
                "`await ws.packages.add('trafilatura', 'cssselect')` and await its task.",
            ) from None
        except (etree.XPathError, ValueError) as exc:
            raise _Failure("ValueError", f"Invalid CSS selector: {exc}") from None
        if matches:
            if len(matches) > 1000:
                raise _Failure("ValueError", "selector matched more than 1000 elements")
            selected_html = bytearray(b"<div>")
            for item in matches:
                fragment = lxml_html.tostring(item, encoding="utf-8", with_tail=False)
                if len(selected_html) + len(fragment) > _MAX_INPUT_BYTES:
                    raise _Failure("ValueError", "selector results exceed the HTML input limit")
                selected_html.extend(fragment)
            selected_html.extend(b"</div>")
            selected = lxml_html.fromstring(bytes(selected_html), parser=parser)
        else:
            selected = None
            warnings.append("selector matched no elements")

    title_nodes = root.xpath("(//title)[1]")
    title = " ".join(title_nodes[0].itertext()).strip() if title_nodes else ""
    if selected is None:
        text = ""
        extracted_title = ""
    else:
        cleaned = deepcopy(selected)
        for node in cleaned.xpath(
            ".//script | .//style | .//noscript | .//nav | .//header | .//footer | .//aside"
        ):
            parent = node.getparent()
            if parent is not None:
                node.drop_tree()
        try:
            raw_result = trafilatura.extract(
                cleaned,
                url=url,
                output_format="json",
                with_metadata=True,
                include_comments=False,
                include_links=False,
            )
            document = json.loads(raw_result) if raw_result else {}
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise _Failure("ValueError", f"HTML extraction failed: {exc}") from None
        if not isinstance(document, dict):
            document = {}
        text = document.get("text") if isinstance(document.get("text"), str) else ""
        extracted_title = document.get("title") if isinstance(document.get("title"), str) else ""
        if not text.strip():
            text = trafilatura.html2txt(cleaned) or ""
            if text.strip():
                warnings.append("main-text extraction was empty; returned cleaned visible text")

    text = _clean(text)
    text, text_truncated = _truncate(text, _MAX_TEXT_BYTES)
    if text_truncated:
        warnings.append("main text exceeded the 8 MiB extraction limit")
    links, links_truncated = _links(selected, link_base) if selected is not None else ([], False)
    if links_truncated:
        warnings.append("links exceeded the extraction limit")

    return {
        "title": _truncate(title or extracted_title, 1024)[0],
        "text": text,
        "links": links,
        "url": url,
        "source_hash": hashlib.sha256(encoded).hexdigest(),
        "warnings": warnings,
        "complete": not text_truncated and not links_truncated,
        "stop_reason": "output_limit" if text_truncated or links_truncated else None,
    }


def main() -> None:
    _limits()
    try:
        header = sys.stdin.buffer.readline(16 * 1024)
        if not header.endswith(b"\n"):
            raise _Failure("ValueError", "Invalid HTML worker request header")
        request = json.loads(header)
        if not isinstance(request, dict):
            raise _Failure("ValueError", "Invalid HTML worker request")
        raw_html = sys.stdin.buffer.read(_MAX_INPUT_BYTES + 1)
        if len(raw_html) > _MAX_INPUT_BYTES:
            raise _Failure("ValueError", f"HTML exceeds the {_MAX_INPUT_BYTES} byte input limit")
        try:
            request["html"] = raw_html.decode("utf-8")
        except UnicodeDecodeError:
            raise _Failure("ValueError", "html must be UTF-8 text") from None
        result = _extract(request)
        response = {"ok": True, "result": result}
    except _Failure as exc:
        response = {"ok": False, "kind": exc.kind, "error": str(exc)}
    except Exception as exc:
        response = {"ok": False, "kind": "HTMLToolError", "error": str(exc)[:2048]}
    sys.stdout.write(json.dumps(response, ensure_ascii=False, separators=(",", ":")))


if __name__ == "__main__":
    main()
