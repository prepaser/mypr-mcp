"""Dependency descriptors and explicit latest-release resolution.

Importing this module is side-effect free. Descriptors identify official
upstreams, while :func:`resolve_artifact` contacts GitHub only when an
installation needs a concrete release asset.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from types import MappingProxyType
from typing import Final
from urllib.parse import quote, urlsplit


@dataclass(frozen=True, slots=True)
class Artifact:
    name: str
    kind: str
    version: str
    url: str
    sha256: str | None
    archive: str | None = None
    executables: tuple[str, ...] = ()
    max_bytes: int = 64 * 1024 * 1024
    source: str = "official"
    repository: str | None = None
    asset_patterns: tuple[str, ...] = ()
    git_sha1: str | None = None


class CatalogResolutionError(ValueError):
    """The upstream did not provide a usable current artifact."""


_X86_64 = "x86_64"
_AARCH64 = "aarch64"
_GITHUB_API = "https://api.github.com"
_MAX_METADATA_BYTES = 4 * 1024 * 1024
_MAX_CHECKSUM_BYTES = 2 * 1024 * 1024
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$", re.IGNORECASE)


def _binary_descriptor(
    name: str,
    repository: str,
    patterns: tuple[str, ...],
    archive: str,
    executables: tuple[str, ...],
    max_bytes: int,
) -> Artifact:
    return Artifact(
        name,
        "binary",
        "",
        "",
        None,
        archive,
        executables,
        max_bytes,
        "official",
        repository,
        patterns,
    )


_BINARY_ASSETS: Final[dict[str, dict[str, Artifact]]] = {
    "rg": {
        _X86_64: _binary_descriptor(
            "rg",
            "BurntSushi/ripgrep",
            (r"^ripgrep-.+-x86_64-unknown-linux-musl\.tar\.gz$",),
            "tar.gz",
            ("rg",),
            16 * 1024 * 1024,
        ),
        _AARCH64: _binary_descriptor(
            "rg",
            "BurntSushi/ripgrep",
            (r"^ripgrep-.+-aarch64-unknown-linux-musl\.tar\.gz$",),
            "tar.gz",
            ("rg",),
            16 * 1024 * 1024,
        ),
    },
    "ast-grep": {
        _X86_64: _binary_descriptor(
            "ast-grep",
            "ast-grep/ast-grep",
            (r"^app-x86_64-unknown-linux-gnu\.zip$",),
            "zip",
            ("ast-grep", "sg"),
            32 * 1024 * 1024,
        ),
        _AARCH64: _binary_descriptor(
            "ast-grep",
            "ast-grep/ast-grep",
            (r"^app-aarch64-unknown-linux-gnu\.zip$",),
            "zip",
            ("ast-grep", "sg"),
            32 * 1024 * 1024,
        ),
    },
    "rga": {
        _X86_64: _binary_descriptor(
            "rga",
            "phiresky/ripgrep-all",
            (r"^ripgrep_all-v.+-x86_64-unknown-linux-musl\.tar\.gz$",),
            "tar.gz",
            ("rga", "rga-preproc"),
            32 * 1024 * 1024,
        ),
        _AARCH64: _binary_descriptor(
            "rga",
            "phiresky/ripgrep-all",
            (r"^ripgrep_all-v.+-aarch64-unknown-linux-gnu\.tar\.gz$",),
            "tar.gz",
            ("rga", "rga-preproc"),
            32 * 1024 * 1024,
        ),
    },
    "pandoc": {
        _X86_64: _binary_descriptor(
            "pandoc",
            "jgm/pandoc",
            (r"^pandoc-.+-linux-amd64\.tar\.gz$",),
            "tar.gz",
            ("pandoc",),
            96 * 1024 * 1024,
        ),
        _AARCH64: _binary_descriptor(
            "pandoc",
            "jgm/pandoc",
            (r"^pandoc-.+-linux-arm64\.tar\.gz$",),
            "tar.gz",
            ("pandoc",),
            96 * 1024 * 1024,
        ),
    },
}


_MODEL_NAMES: Final[tuple[str, ...]] = (
    "afr",
    "amh",
    "ara",
    "asm",
    "aze",
    "aze_cyrl",
    "bel",
    "ben",
    "bod",
    "bos",
    "bre",
    "bul",
    "cat",
    "ceb",
    "ces",
    "chi_sim",
    "chi_sim_vert",
    "chi_tra",
    "chi_tra_vert",
    "chr",
    "cos",
    "cym",
    "dan",
    "deu",
    "div",
    "dzo",
    "ell",
    "eng",
    "enm",
    "epo",
    "equ",
    "est",
    "eus",
    "fao",
    "fas",
    "fil",
    "fin",
    "fra",
    "frk",
    "frm",
    "fry",
    "gla",
    "gle",
    "glg",
    "grc",
    "guj",
    "hat",
    "heb",
    "hin",
    "hrv",
    "hun",
    "hye",
    "iku",
    "ind",
    "isl",
    "ita",
    "ita_old",
    "jav",
    "jpn",
    "jpn_vert",
    "kan",
    "kat",
    "kat_old",
    "kaz",
    "khm",
    "kir",
    "kmr",
    "kor",
    "kor_vert",
    "lao",
    "lat",
    "lav",
    "lit",
    "ltz",
    "mal",
    "mar",
    "mkd",
    "mlt",
    "mon",
    "mri",
    "msa",
    "mya",
    "nep",
    "nld",
    "nor",
    "oci",
    "ori",
    "osd",
    "pan",
    "pol",
    "por",
    "pus",
    "que",
    "ron",
    "rus",
    "san",
    "sin",
    "slk",
    "slv",
    "snd",
    "spa",
    "spa_old",
    "sqi",
    "srp",
    "srp_latn",
    "sun",
    "swa",
    "swe",
    "syr",
    "tam",
    "tat",
    "tel",
    "tgk",
    "tha",
    "tir",
    "ton",
    "tur",
    "uig",
    "ukr",
    "urd",
    "uzb",
    "uzb_cyrl",
    "vie",
    "yid",
    "yor",
)


def _models() -> dict[str, Artifact]:
    return {
        f"tessdata:{name}": Artifact(
            f"tessdata:{name}",
            "model",
            "",
            "",
            None,
            None,
            (),
            32 * 1024 * 1024,
            "official",
            "tesseract-ocr/tessdata_fast",
            (rf"^{re.escape(name)}\.traineddata$",),
        )
        for name in _MODEL_NAMES
    }


CATALOG: Final[MappingProxyType[str, Artifact]] = MappingProxyType(
    {name: next(iter(assets.values())) for name, assets in _BINARY_ASSETS.items()} | _models()
)
BINARY_NAMES: Final[tuple[str, ...]] = tuple(_BINARY_ASSETS)
MODEL_NAMES: Final[tuple[str, ...]] = tuple(f"tessdata:{name}" for name in _MODEL_NAMES)


def artifact_for(name: str, *, system: str | None = None) -> Artifact | None:
    """Return a side-effect-free descriptor for a logical dependency."""

    if name.startswith("tessdata:"):
        return CATALOG.get(name)
    if system is None:
        return CATALOG.get(name)
    return _BINARY_ASSETS.get(name, {}).get(system)


Fetch = Callable[[str, int], Awaitable[bytes]]


def _github_headers() -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "mypr-mcp",
    }
    token = os.environ.get("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


async def _read_url(url: str, maximum: int) -> bytes:
    parsed = urlsplit(url)
    if parsed.scheme != "https":
        raise CatalogResolutionError("dependency metadata requires HTTPS")
    headers = (
        _github_headers()
        if parsed.netloc.lower() == "api.github.com"
        else {"User-Agent": "mypr-mcp"}
    )
    received = 0
    chunks: list[bytes] = []
    try:
        import httpx2

        async with httpx2.AsyncClient(
            follow_redirects=True, timeout=30.0, headers=headers
        ) as client:
            async with client.stream("GET", url) as response:
                if response.url.scheme != "https":
                    raise CatalogResolutionError("dependency metadata redirect is not HTTPS")
                response.raise_for_status()
                length = response.headers.get("content-length")
                if length is not None and int(length) > maximum:
                    raise CatalogResolutionError("dependency metadata exceeds its size limit")
                async for chunk in response.aiter_bytes(1024 * 1024):
                    received += len(chunk)
                    if received > maximum:
                        raise CatalogResolutionError("dependency metadata exceeds its size limit")
                    chunks.append(chunk)
    except CatalogResolutionError:
        raise
    except Exception as exc:
        raise CatalogResolutionError(f"failed to fetch dependency metadata: {exc}") from exc
    return b"".join(chunks)


async def _fetch(url: str, maximum: int, fetcher: Fetch | None) -> bytes:
    if fetcher is not None:
        data = await fetcher(url, maximum)
        if len(data) > maximum:
            raise CatalogResolutionError("dependency metadata exceeds its size limit")
        return data
    return await _read_url(url, maximum)


async def _fetch_json(url: str, fetcher: Fetch | None) -> dict[str, object]:
    try:
        value = json.loads(await _fetch(url, _MAX_METADATA_BYTES, fetcher))
    except (CatalogResolutionError, json.JSONDecodeError, UnicodeDecodeError) as exc:
        if isinstance(exc, CatalogResolutionError):
            raise
        raise CatalogResolutionError("GitHub returned invalid dependency metadata") from exc
    if not isinstance(value, dict):
        raise CatalogResolutionError("GitHub dependency metadata is not an object")
    return value


def _release_url(repository: str) -> str:
    return f"{_GITHUB_API}/repos/{repository}/releases/latest"


def _release_tag(payload: dict[str, object]) -> str:
    tag = payload.get("tag_name")
    if not isinstance(tag, str) or len(tag) > 128 or re.fullmatch(r"v?\d+(?:\.\d+)+", tag) is None:
        raise CatalogResolutionError("GitHub release has no usable tag")
    return tag


def _release_assets(payload: dict[str, object]) -> list[dict[str, object]]:
    assets = payload.get("assets")
    if not isinstance(assets, list):
        raise CatalogResolutionError("GitHub release has no asset list")
    return [
        asset for asset in assets if isinstance(asset, dict) and isinstance(asset.get("name"), str)
    ]


def _asset_for(assets: list[dict[str, object]], patterns: tuple[str, ...]) -> dict[str, object]:
    matches = [
        asset
        for asset in assets
        if any(re.search(pattern, str(asset["name"]), re.IGNORECASE) for pattern in patterns)
    ]
    if len(matches) != 1:
        names = ", ".join(str(asset["name"]) for asset in matches[:4])
        detail = f" ({names})" if names else ""
        raise CatalogResolutionError(f"could not select one release asset{detail}")
    return matches[0]


def _download_url(repository: str, tag: str, name: str) -> str:
    return (
        f"https://github.com/{repository}/releases/download/"
        f"{quote(tag, safe='')}/{quote(name, safe='')}"
    )


def _sha256_from_asset(asset: dict[str, object]) -> str | None:
    digest = asset.get("digest")
    if not isinstance(digest, str) or not digest.lower().startswith("sha256:"):
        return None
    value = digest.partition(":")[2]
    return value.lower() if _HEX64_RE.fullmatch(value) else None


def _checksum_asset(assets: list[dict[str, object]], selected: str) -> dict[str, object] | None:
    candidates = []
    for asset in assets:
        name = str(asset["name"])
        lowered = name.lower()
        if name == selected or ("sha256" not in lowered and "checksum" not in lowered):
            continue
        if any(lowered.endswith(suffix) for suffix in (".sig", ".asc", ".pem")):
            continue
        candidates.append(asset)
    return (
        sorted(candidates, key=lambda asset: str(asset["name"]).lower())[0] if candidates else None
    )


def _path_name(value: str) -> str:
    return value.strip().replace("\\", "/").rsplit("/", 1)[-1]


def _checksum_for(text: bytes, selected: str) -> str | None:
    try:
        lines = text.decode("utf-8", "strict").splitlines()
    except UnicodeDecodeError:
        return None
    for line in lines[:10000]:
        match = re.match(r"^\s*([0-9a-f]{64})\s+[*]?(.+?)\s*$", line, re.IGNORECASE)
        if match and _path_name(match.group(2)) == selected:
            return match.group(1).lower()
        match = re.match(r"^\s*(.+?)\s+([0-9a-f]{64})\s*$", line, re.IGNORECASE)
        if match and _path_name(match.group(1)) == selected:
            return match.group(2).lower()
    return None


async def _resolve_binary(
    descriptor: Artifact,
    fetcher: Fetch | None,
) -> Artifact:
    if descriptor.repository is None:
        raise CatalogResolutionError(f"{descriptor.name} has no official repository")
    payload = await _fetch_json(_release_url(descriptor.repository), fetcher)
    tag = _release_tag(payload)
    assets = _release_assets(payload)
    asset = _asset_for(assets, descriptor.asset_patterns)
    asset_name = str(asset["name"])
    digest = _sha256_from_asset(asset)
    if digest is None:
        checksum = _checksum_asset(assets, asset_name)
        if checksum is not None:
            checksum_url = _download_url(descriptor.repository, tag, str(checksum["name"]))
            digest = _checksum_for(
                await _fetch(checksum_url, _MAX_CHECKSUM_BYTES, fetcher), asset_name
            )
    return replace(
        descriptor,
        version=tag.removeprefix("v"),
        url=_download_url(descriptor.repository, tag, asset_name),
        sha256=digest,
    )


async def _resolve_model(
    descriptor: Artifact,
    model_name: str,
    fetcher: Fetch | None,
) -> Artifact:
    if descriptor.repository is None:
        raise CatalogResolutionError(f"{descriptor.name} has no official repository")
    payload = await _fetch_json(_release_url(descriptor.repository), fetcher)
    tag = _release_tag(payload)
    filename = f"{model_name}.traineddata"
    raw_url = (
        f"https://raw.githubusercontent.com/{descriptor.repository}/"
        f"{quote(tag, safe='')}/{quote(filename, safe='')}"
    )
    contents_url = (
        f"{_GITHUB_API}/repos/{descriptor.repository}/contents/"
        f"{quote(filename, safe='')}?ref={quote(tag, safe='')}"
    )
    contents = await _fetch_json(contents_url, fetcher)
    value = contents.get("sha")
    if not isinstance(value, str) or re.fullmatch(r"[0-9a-f]{40}", value, re.IGNORECASE) is None:
        raise CatalogResolutionError(f"GitHub has no usable digest for {descriptor.name}")
    git_sha1 = value.lower()
    return replace(
        descriptor,
        version=tag.removeprefix("v"),
        url=raw_url,
        git_sha1=git_sha1,
    )


async def resolve_artifact(
    name: str,
    *,
    system: str | None = None,
    fetcher: Fetch | None = None,
) -> Artifact:
    """Resolve a descriptor against the current official GitHub release.

    ``fetcher`` is intended for tests and callers that already provide a
    bounded HTTP transport. It receives ``(url, maximum_bytes)`` and must
    return the response body.
    """

    descriptor = artifact_for(name, system=system)
    if descriptor is None:
        raise CatalogResolutionError(f"unsupported dependency: {name}")
    if descriptor.kind == "binary":
        if system is None:
            raise CatalogResolutionError(f"a platform is required for {name}")
        return await _resolve_binary(descriptor, fetcher)
    return await _resolve_model(descriptor, name.removeprefix("tessdata:"), fetcher)


__all__ = [
    "Artifact",
    "BINARY_NAMES",
    "MODEL_NAMES",
    "CATALOG",
    "CatalogResolutionError",
    "artifact_for",
    "resolve_artifact",
]
