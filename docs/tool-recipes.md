# External tool recipes

Use `ws.shell` for installed command-line tools and `ws.mcp` for external MCP servers. These recipes keep execution, cancellation, output paging, and history in the existing workspace task system. They do not install tools or add another process manager.

## Benchmark a command

Hyperfine's JSON output can stay in Python while the agent reads only the measurements it needs. Benchmark commands execute on every warmup and measured run, so choose commands whose repeated effects are intended.

```python
import json
import shlex
import sys

ws.local["benchmark_path"] = ws.root / "artifacts" / "benchmark.json"
ws.local["benchmark_command"] = shlex.join([
    sys.executable, "-c", "sum(range(1000000))",
])
ws.local["benchmark"] = await ws.shell.run([
    "hyperfine", "--warmup", "1", "--runs", "5",
    "--export-json", str(ws.local["benchmark_path"]),
    ws.local["benchmark_command"],
], timeout=120, check=True)
ws.local["measurements"] = json.loads(
    (await ws.fs.read(ws.local["benchmark_path"]))["text"]
)
print(ws.local["measurements"]["results"][0]["mean"])
```

## Trace a command's file access

Start a new command under strace to investigate missing files or unexpected paths. A bounded run and a narrow syscall filter are easier to inspect than an unrestricted trace. Tracing an existing process is subject to the host's ptrace permissions.

```python
import sys

ws.local["trace"] = await ws.shell.run([
    "strace", "-f", "-e", "trace=%file", "-s", "128",
    sys.executable, "-c", "import json",
], timeout=15)
print(await ws.tasks.get(ws.local["trace"]["id"]).read(max_bytes=8192))
```

Retain the job ID to reopen saved output with `await ws.tasks.attach(job_id)`. A read or expect timeout does not terminate the traced command; the `shell.run()` execution timeout does.

## Run project checks

```python
ws.local["checks"] = await ws.shell.run(["qlty", "check"], timeout=120)
print(ws.local["checks"]["returncode"])
print(await ws.tasks.get(ws.local["checks"]["id"]).read(max_bytes=8192))
```

Use the project's configured checker and read its exit status. Prefer its machine-readable report when available; parse that report in Python and print only relevant findings. A new checker wrapper is unnecessary when the existing job API already provides the required behavior.

## Explore an existing code index

```python
ws.local["index_status"] = await ws.shell.run(["codegraph", "status"], timeout=10)
print(ws.local["index_status"]["stdout"])
ws.local["impact"] = await ws.shell.run([
    "codegraph", "impact", "symbol_name",
], timeout=30)
print(ws.local["impact"]["stdout"])
```

Replace `symbol_name` with a symbol in the repository. Initialize or synchronize the external index explicitly according to that tool's documentation. mypr does not silently build indexes, download models, or start a second indexing service.

External MCP servers can be connected with `ws.mcp.configure(name, config)`. Use the installed server's documented command and arguments, inspect `list_tools(name)`, then call the selected tool with its advertised schema. Resource templates are discoverable through `list_resource_templates(name)`. Keep service-specific workflows in workspace modules or skills instead of duplicating provider APIs in mypr.
