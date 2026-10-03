from __future__ import annotations

import json

import pytest

from mypr_mcp.dependency_catalog import (
    CATALOG,
    CatalogResolutionError,
    resolve_artifact,
)


def _fetcher(responses: dict[str, bytes]):
    async def fetch(url: str, maximum: int) -> bytes:
        value = responses[url]
        assert len(value) <= maximum
        return value

    return fetch


def _json(value: object) -> bytes:
    return json.dumps(value).encode()


@pytest.mark.asyncio
async def test_catalog_descriptors_do_not_pin_release_metadata():
    for artifact in (CATALOG["rg"], CATALOG["ast-grep"], CATALOG["rga"], CATALOG["pandoc"]):
        assert artifact.version == ""
        assert artifact.url == ""
        assert artifact.sha256 is None
        assert artifact.repository
    model = CATALOG["tessdata:eng"]
    assert model.version == ""
    assert model.url == ""
    assert model.sha256 is None


@pytest.mark.asyncio
async def test_resolve_binary_uses_latest_release_asset_digest():
    endpoint = "https://api.github.com/repos/BurntSushi/ripgrep/releases/latest"
    responses = {
        endpoint: _json(
            {
                "tag_name": "15.9.0",
                "assets": [
                    {
                        "name": "ripgrep-15.9.0-x86_64-unknown-linux-musl.tar.gz",
                        "digest": "sha256:" + "a" * 64,
                    }
                ],
            }
        )
    }
    artifact = await resolve_artifact("rg", system="x86_64", fetcher=_fetcher(responses))
    assert artifact.version == "15.9.0"
    assert artifact.sha256 == "a" * 64
    assert artifact.url.endswith("/15.9.0/ripgrep-15.9.0-x86_64-unknown-linux-musl.tar.gz")


@pytest.mark.asyncio
async def test_resolve_binary_reads_official_checksum_asset_when_digest_missing():
    endpoint = "https://api.github.com/repos/jgm/pandoc/releases/latest"
    asset = "pandoc-4.0-linux-amd64.tar.gz"
    checksum = "SHA256SUMS"
    checksum_url = f"https://github.com/jgm/pandoc/releases/download/v4.0/{checksum}"
    responses = {
        endpoint: _json(
            {
                "tag_name": "v4.0",
                "assets": [{"name": asset}, {"name": checksum}],
            }
        ),
        checksum_url: ("b" * 64 + "  " + asset + "\n").encode(),
    }
    artifact = await resolve_artifact("pandoc", system="x86_64", fetcher=_fetcher(responses))
    assert artifact.version == "4.0"
    assert artifact.sha256 == "b" * 64


@pytest.mark.asyncio
async def test_resolve_model_uses_latest_tag_and_git_blob_digest():
    release_endpoint = "https://api.github.com/repos/tesseract-ocr/tessdata_fast/releases/latest"
    contents_endpoint = (
        "https://api.github.com/repos/tesseract-ocr/tessdata_fast/contents/"
        "eng.traineddata?ref=4.1.1"
    )
    responses = {
        release_endpoint: _json({"tag_name": "4.1.1", "assets": []}),
        contents_endpoint: _json({"sha": "c" * 40}),
    }
    artifact = await resolve_artifact("tessdata:eng", fetcher=_fetcher(responses))
    assert artifact.version == "4.1.1"
    assert artifact.git_sha1 == "c" * 40
    assert artifact.url.endswith("/tessdata_fast/4.1.1/eng.traineddata")


@pytest.mark.asyncio
async def test_resolve_rejects_bad_upstream_metadata():
    endpoint = "https://api.github.com/repos/BurntSushi/ripgrep/releases/latest"
    fetcher = _fetcher({endpoint: b"[]"})
    with pytest.raises(CatalogResolutionError, match="not an object"):
        await resolve_artifact("rg", system="x86_64", fetcher=fetcher)


@pytest.mark.asyncio
async def test_resolve_rejects_ambiguous_release_assets():
    endpoint = "https://api.github.com/repos/BurntSushi/ripgrep/releases/latest"
    responses = {
        endpoint: _json(
            {
                "tag_name": "15.9.0",
                "assets": [
                    {"name": "ripgrep-15.9.0-x86_64-unknown-linux-musl.tar.gz"},
                    {"name": "ripgrep-15.9.0-x86_64-unknown-linux-musl.tar.gz"},
                ],
            }
        )
    }
    with pytest.raises(CatalogResolutionError, match="could not select one"):
        await resolve_artifact("rg", system="x86_64", fetcher=_fetcher(responses))


@pytest.mark.asyncio
async def test_resolve_rejects_unsafe_release_tag():
    endpoint = "https://api.github.com/repos/BurntSushi/ripgrep/releases/latest"
    responses = {endpoint: _json({"tag_name": "release/latest", "assets": []})}
    with pytest.raises(CatalogResolutionError, match="no usable tag"):
        await resolve_artifact("rg", system="x86_64", fetcher=_fetcher(responses))


@pytest.mark.asyncio
async def test_resolve_model_requires_official_git_blob_digest():
    release_endpoint = "https://api.github.com/repos/tesseract-ocr/tessdata_fast/releases/latest"
    contents_endpoint = (
        "https://api.github.com/repos/tesseract-ocr/tessdata_fast/contents/"
        "eng.traineddata?ref=4.1.1"
    )
    responses = {
        release_endpoint: _json({"tag_name": "4.1.1", "assets": []}),
        contents_endpoint: _json({"sha": "missing"}),
    }
    with pytest.raises(CatalogResolutionError, match="no usable digest"):
        await resolve_artifact("tessdata:eng", fetcher=_fetcher(responses))


@pytest.mark.asyncio
async def test_resolve_rejects_unsupported_platform():
    with pytest.raises(CatalogResolutionError, match="unsupported dependency"):
        await resolve_artifact("rg", system="linux-armv7", fetcher=_fetcher({}))
