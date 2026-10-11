# Runtime lifecycle

`Runtime` owns the live services for one workspace manager. Lifecycle methods serialize through `_lifecycle_lock`; admission, kernel reservation, generation, and resource locks remain separate so that accepting work and closing a resource cannot race.

| Resource | Authority | Publication rule |
| --- | --- | --- |
| Web, dependency, mail, and MCP services | `Runtime` service attributes | A close/reset operation is complete only after the service reports completion. |
| Shell and scan jobs | `Runtime.close_shells()` and their watchers | Pending output is drained and recorded before shell cleanup completes. |
| Python task state | `Runtime.lose_python_tasks()` | A task is terminal only after its terminal record is persisted. |
| Runtime history and stores | The persistence owner in `run()` teardown | History remains the durable source for records after a kernel reset. |
| Kernel handles and browser resources | `Runtime.close_kernel()` | Handles are invalid after the kernel generation is closed. |

Completion of a lifecycle operation is distinct from completion of a job. A job may already be terminal while output remains in its journal or in a shell watcher. The lifecycle method waits for that pending output according to the resource owner before returning. A failed cleanup is not reported as a successful lifecycle completion.

Kernel control requests share one scope for message creation and waiter registration/removal. Sending, deadlines, generation checks, and recovery remain with each caller. A reservation keeps its waiter until nested abort recovery finishes; late LSP replies remain independently reconcilable.

## Shell history recovery

Shells owns the process outcome and its terminal job metadata; History stores the lifecycle and output projection. If publishing History output fails, a shell can still finish and persist its actual outcome. At manager startup, active ordinary shell records with matching, valid terminal metadata recover as `succeeded`, `failed`, or `cancelled` before remaining unfinished records become `lost`.

Recovered records retain their original generation and client, connection, and execution identity. The terminal state and recovery event commit together. Each recovered record includes `history_output_incomplete`: the known process outcome does not guarantee that every History output event was published. Recovery preserves existing warnings, keeping at most four with this warning first and setting `warnings_truncated` when more are omitted. It does not rerun commands or reimport job journals. Already terminal History records and output-retention markers remain unchanged.

Missing, corrupt, nonterminal, or mismatched metadata cannot establish an outcome and keeps the existing lost/unknown fallback. A failed History correction aborts startup instead of claiming recovery succeeded. Scans and package jobs retain their separate recovery semantics.

## Reset

`reset()` runs under `_lifecycle_lock` and fails fast in this order:

1. `web.reset()`
2. `dependencies.close()`
3. `close_kernel()`
4. `lose_python_tasks("Workspace reset", state="cancelled")`
5. `close_shells()`
6. `mcp.close()`

Only after every step succeeds does reset load the new configuration snapshot, create the MCP and shell services, allocate a new kernel generation, and start the kernel. A failure marks the runtime unhealthy and prevents recreation.

## Shutdown

`shutdown_resources()` runs under the same lock and attempts independent cleanup in this order:

1. `web.close()`
2. `close_kernel()`
3. `lose_python_tasks("Manager stopped")` (recording errors are suppressed)
4. `dependencies.close()`
5. `close_shells()`
6. `mcp.close()`
7. `mail.close()`

The remaining steps still run after a close failure. All close failures are reported together as `ExceptionGroup("Manager resource cleanup failed", ...)`. The `run()` teardown closes client connections and drains background work before unregistering the manager, retrying remaining service cleanup, and closing stores and persistence. Repeated shutdown calls are safe for resources that have already closed; cleanup that remains owned by the runtime is retried.
