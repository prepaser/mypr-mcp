from conftest import execute, mcp_session, result_text


async def test_new_helpers_work_through_the_shared_kernel(workspace):
    async with mcp_session(workspace) as session:
        result = await execute(
            session,
            "assert 'inherit_env' in ws.help('shell.run')\n"
            "assert 'resize' in ws.help('fs.image')\n"
            "assert 'start_page' in ws.help('docs.read')\n"
            "assert 'command' in ws.help('code.configure')\n"
            "ws.code.status()\n"
            "ws.local['v1'] = await ws.modules.write('extension_check', "
            "'def value(): return 1\\n')\n"
            "ws.local['v2'] = await ws.modules.write('extension_check', "
            "'def value(): return 2\\n', "
            "expected_hash=ws.local['v1']['revision'])\n"
            "ws.local['loaded'] = ws.modules.load('extension_check')\n"
            "await ws.modules.restore('extension_check', ws.local['v1']['revision'], "
            "expected_hash=ws.local['v2']['revision'])\n"
            "assert ws.local['loaded'].value() == 2\n"
            "assert (await ws.modules.history('extension_check'))['items'][0]['revision'] "
            "== ws.local['v1']['revision']\n"
            "assert ws.modules.reload('extension_check').value() == 1\n"
            "ws.local['skill'] = await ws.skills.write('extension-check', "
            "'---\\nname: extension-check\\ndescription: Check revisions.\\n"
            "---\\nRead changes.\\n')\n"
            "assert (await ws.skills.history('extension-check'))['items'][0]['revision'] "
            "== ws.local['skill']['revision']\n"
            "print('extensions-ok')",
        )
        assert result["state"] == "succeeded", result_text(result)
        assert "extensions-ok" in result_text(result)
