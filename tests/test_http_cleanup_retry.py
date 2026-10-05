import httpx2
import pytest

from mypr_mcp.http_tools import HTTPTools


class Transport(httpx2.AsyncBaseTransport):
    def __init__(self, *, fail=False):
        self.fail = fail
        self.calls = 0

    async def handle_async_request(self, request):
        return httpx2.Response(200, content=b"ok")

    async def aclose(self):
        self.calls += 1
        if self.fail:
            self.fail = False
            raise RuntimeError("transport close failed")


async def test_shutdown_retries_native_transport_and_closes_unvisited_mounts(tmp_path):
    tools = HTTPTools(tmp_path)
    failed, mount, successful = Transport(fail=True), Transport(), Transport()
    native = await tools.client(
        "failed", transport=failed, mounts={"https://mounted.test": mount}, trust_env=False
    )
    await tools.client("successful", transport=successful, trust_env=False)
    with pytest.raises(RuntimeError, match="transport close failed"):
        await tools.aclose()
    assert native.is_closed
    assert len(tools._clients) == 1
    assert (failed.calls, mount.calls, successful.calls) == (1, 0, 1)
    await tools.aclose()
    assert (failed.calls, mount.calls, successful.calls) == (2, 1, 1)
    assert not tools._clients and not tools._transports
    await tools.aclose()
    assert (failed.calls, mount.calls, successful.calls) == (2, 1, 1)


async def test_manually_failed_native_close_requires_cleanup_before_replacement(tmp_path):
    tools = HTTPTools(tmp_path)
    transport = Transport(fail=True)
    client = await tools.client(transport=transport, trust_env=False)
    with pytest.raises(RuntimeError, match="transport close failed"):
        await client.aclose()
    with pytest.raises(RuntimeError, match="cleanup is incomplete"):
        await tools.client()
    await tools.close()
    assert transport.calls == 2
    replacement = await tools.client(trust_env=False)
    assert replacement is not client
    await tools.aclose()


async def test_reused_mount_transport_is_closed_once(tmp_path):
    tools = HTTPTools(tmp_path)
    transport = Transport()
    client = await tools.client(
        transport=transport, mounts={"https://mounted.test": transport}, trust_env=False
    )
    async with client:
        assert (await client.get("https://mounted.test")).content == b"ok"
    await tools.aclose()
    assert transport.calls == 1
