from __future__ import annotations

import asyncio
import ssl
import subprocess

import pytest

from mypr_mcp.network_tools import NetworkTools


@pytest.mark.asyncio
async def test_resolve_and_connect_report_structured_results(tmp_path):
    tools = NetworkTools(tmp_path)
    resolved = await tools.resolve("localhost", 80)
    assert resolved["host"] == "localhost"
    assert resolved["addresses"]
    families = {address["family"] for address in resolved["addresses"]}
    assert "AF_INET" in families

    async def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(accept, "127.0.0.1", 0)
    try:
        port = server.sockets[0].getsockname()[1]
        result = await tools.connect("127.0.0.1", port)
        assert result["state"] == "open"
        result = await tools.connect("127.0.0.1", port + 1)
        assert result["state"] in {"closed", "unreachable", "timeout"}
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_tls_rejects_unverified_context_when_verification_requested(tmp_path):
    tools = NetworkTools(tmp_path)
    context = ssl._create_unverified_context()
    with pytest.raises(ValueError, match="verify=True"):
        await tools.tls("localhost", ssl_context=context)


@pytest.mark.asyncio
async def test_tls_verifies_and_pins_self_signed_certificate(tmp_path):
    cert = tmp_path / "cert.pem"
    key = tmp_path / "key.pem"
    await asyncio.to_thread(
        subprocess.run,
        [
            "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
        ],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_context.load_cert_chain(cert, key)

    async def accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(accept, "127.0.0.1", 0, ssl=server_context)
    try:
        port = server.sockets[0].getsockname()[1]
        client_context = ssl.create_default_context(cafile=cert)
        result = await NetworkTools(tmp_path).tls("localhost", port, ssl_context=client_context)
        assert result["verified"] is True
        assert result["version"]
        assert result["cipher"]
        certificate = cert.read_bytes()
        pinned = await NetworkTools(tmp_path).tls(
            "localhost", port, verify=False, cert_pem=certificate
        )
        assert pinned["verified"] is True
        with pytest.raises(ssl.SSLError, match="fingerprint"):
            await NetworkTools(tmp_path).tls("localhost", port, verify=False, fingerprint="00" * 32)
    finally:
        server.close()
        await server.wait_closed()
