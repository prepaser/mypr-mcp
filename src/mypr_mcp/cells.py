"""Concurrent IPython cell execution for the workspace kernel."""

from __future__ import annotations

import ast
import asyncio
import contextvars
import inspect
import sys
import traceback
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from IPython.core.interactiveshell import ExecutionInfo, ExecutionResult, InteractiveShell

from .kernel_api import (
    NotReady,
    OutputBuffer,
    ResetRequested,
    TaskHandle,
    _cell_output,
    execution_context,
)

_cell_result: contextvars.ContextVar[tuple[ExecutionResult, ...]] = contextvars.ContextVar(
    "mypr_cell_result", default=()
)
_cell_execution_count: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "mypr_cell_execution_count", default=None
)
_cell_source: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "mypr_cell_source", default=None
)


class ContextDisplayHookMixin:
    """Make IPython's display result and prompt count task-local."""

    @property
    def exec_result(self) -> ExecutionResult | None:
        stack = _cell_result.get()
        return stack[-1] if stack else None

    @exec_result.setter
    def exec_result(self, value: ExecutionResult | None) -> None:
        stack = _cell_result.get()
        _cell_result.set(stack + (value,) if value is not None else stack[:-1])

    @property
    def prompt_count(self) -> int:
        result = self.exec_result
        count = result.execution_count if result is not None else _cell_execution_count.get()
        if count is not None:
            return count
        return super().prompt_count  # type: ignore[misc]

    def quiet(self) -> bool:
        result = self.exec_result
        source = result.info.raw_cell if result is not None else _cell_source.get()
        if source is not None:
            return self.semicolon_at_end_of_expression(source)
        return super().quiet()

    def __call__(self, result: Any = None) -> None:
        # IPython deliberately skips fill_exec_result for a trailing semicolon.
        # The workspace API still exposes the value that was evaluated.
        if result is not None and self.quiet():
            self.fill_exec_result(result)
        super().__call__(result)

    def write_format_data(self, format_dict, md_dict=None):
        output = _cell_output.get()
        if output is not None:
            output.write(format_dict.get("text/plain", ""))
        super().write_format_data(format_dict, md_dict)


def install_context_displayhook(shell: InteractiveShell) -> None:
    """Install task-local display state on the already-created ZMQ hook."""

    hook = shell.displayhook
    if not isinstance(hook, ContextDisplayHookMixin):
        hook.__class__ = type(
            "WorkspaceDisplayHook",
            (ContextDisplayHookMixin, hook.__class__),
            {"__module__": __name__},
        )


@dataclass(slots=True)
class CellResult:
    value: Any
    execution_count: int
    user_expressions: dict[str, Any]
    error: BaseException | None = None


class CellHandle(TaskHandle):
    """A TaskHandle backed by one kernel cell."""

    def __init__(
        self,
        task_id: str,
        task: asyncio.Task[Any],
        output: Any,
        source: Any = None,
        run_state: dict[str, bool] | None = None,
        *,
        generation: str | None = None,
        execution_count: int | None = None,
    ) -> None:
        super().__init__(task_id, task, output, source, run_state)
        self.kind = "cell"
        self.generation = generation
        self.execution_count = execution_count
        self._cell_state = "queued"
        self._cell_error: BaseException | None = None
        self._cell_value: Any = None
        self._parent: dict[str, Any] | None = None

    def set_state(self, state: str, error: BaseException | None = None) -> None:
        self._cell_state = state
        self._cell_error = error

    def set_result(self, result: CellResult) -> None:
        self._cell_value = result.value
        self.execution_count = result.execution_count

    def status(self) -> dict[str, Any]:
        result = super().status()
        result.update(
            {
                "status": self._cell_state,
                "kind": "cell",
                "generation": self.generation,
                "execution_count": self.execution_count,
            }
        )
        if self._cell_error is not None:
            result["error"] = f"{type(self._cell_error).__name__}: {self._cell_error}"
        return result

    def result(self) -> Any:
        if self._cell_state in {"queued", "running", "cancelling"}:
            raise NotReady(f"cell {self.id} is still running")
        if self._cell_state == "cancelled":
            raise asyncio.CancelledError
        if self._cell_state == "reset":
            raise ResetRequested
        if self._cell_state == "failed":
            if self._cell_error is not None:
                raise self._cell_error
            raise RuntimeError(f"cell {self.id} failed")
        return self._cell_value

    async def cancel(self) -> bool:
        if self._cell_state in {"succeeded", "failed", "cancelled", "reset"}:
            return False
        self._cancel_requested = True
        self._cell_state = "cancelling"
        self._task.cancel()
        return True

    async def _wait(self) -> Any:
        if asyncio.current_task() is self._task:
            raise RuntimeError("a cell cannot await itself")
        await asyncio.shield(self._task)
        return self.result()


class CellExecutor:
    """Register and execute cells as independent asyncio tasks."""

    def __init__(self, kernel: Any, shell: InteractiveShell, tasks: Any) -> None:
        self.kernel = kernel
        self.shell = shell
        self.tasks = tasks

    def submit(
        self,
        code: str,
        metadata: Mapping[str, Any],
        parent: dict[str, Any],
        ident: Any,
        stream: Any,
        *,
        silent: bool = False,
        store_history: bool = True,
        user_expressions: Mapping[str, str] | None = None,
        allow_stdin: bool = False,
        stop_on_error: bool = False,
    ) -> CellHandle:
        exec_id = str(metadata.get("exec_id") or "")
        if not exec_id:
            raise ValueError("mypr execute metadata requires exec_id")
        generation = str(metadata["generation"])
        count = self.shell.execution_count
        if not silent and store_history:
            self.shell.execution_count += 1

        output = OutputBuffer()
        run_state = {"started": False}
        holder: dict[str, CellHandle] = {}

        async def runner() -> CellResult:
            return await self._run(
                holder["handle"],
                code,
                metadata,
                parent,
                ident,
                count,
                silent=silent,
                store_history=store_history,
                user_expressions=user_expressions,
                allow_stdin=allow_stdin,
                stop_on_error=stop_on_error,
                output=output,
            )

        with execution_context(metadata):
            task = asyncio.create_task(runner(), name=f"mypr:cell:{exec_id}", eager_start=False)
            handle = CellHandle(
                exec_id,
                task,
                output,
                None,
                run_state,
                generation=generation,
                execution_count=count,
            )
        holder["handle"] = handle
        handle._parent = parent
        self.tasks._handles[exec_id] = handle
        task.add_done_callback(lambda task: self._task_done(handle, task))
        return handle

    def _task_done(self, handle: CellHandle, task: asyncio.Task[Any]) -> None:
        if task.cancelled() and handle.status()["status"] not in {
            "cancelled",
            "succeeded",
            "failed",
            "reset",
        }:
            handle.set_state("cancelled", asyncio.CancelledError())
            if handle._parent is not None:
                self._send_event(handle, handle._parent, "cancelled")

    async def _run(
        self,
        handle: CellHandle,
        code: str,
        metadata: Mapping[str, Any],
        parent: dict[str, Any],
        ident: Any,
        execution_count: int,
        *,
        silent: bool,
        store_history: bool,
        user_expressions: Mapping[str, str] | None,
        allow_stdin: bool,
        stop_on_error: bool,
        output: Any,
    ) -> CellResult:
        del allow_stdin, stop_on_error
        cell_token = _cell_output.set(output)
        count_token = _cell_execution_count.set(execution_count)
        source_token = _cell_source.set(code)
        handle.set_state("running")
        self._send_event(handle, parent, "running")
        result: CellResult | None = None
        terminal = "succeeded"
        error: BaseException | None = None
        try:
            self.kernel.set_parent(ident, parent)
            if not silent:
                self.kernel._publish_execute_input(code, parent, execution_count)
            result = await self._execute(
                code,
                execution_count,
                silent=silent,
                store_history=store_history,
                user_expressions=user_expressions,
            )
            handle.set_result(result)
            if result.error is not None:
                terminal = "failed"
                error = result.error
                if isinstance(error, ResetRequested):
                    terminal = "reset"
                elif isinstance(error, asyncio.CancelledError):
                    terminal = "cancelled"
                    raise error
        except asyncio.CancelledError as exc:
            terminal, error = "cancelled", exc
            raise
        except ResetRequested as exc:
            terminal, error = "reset", exc
        except BaseException as exc:
            terminal, error = "failed", exc
        finally:
            if error is not None and terminal == "failed":
                output.write("".join(traceback.format_exception(error)))
            try:
                sys.stdout.flush()
                sys.stderr.flush()
            except Exception:
                pass
            _cell_execution_count.reset(count_token)
            _cell_source.reset(source_token)
            _cell_output.reset(cell_token)
            handle.set_state(terminal, error)
            self._send_event(handle, parent, terminal, error)
        if result is None:
            if terminal == "reset":
                return CellResult(None, execution_count, {})
            if error is not None:
                raise error
            return CellResult(None, execution_count, {})
        return result

    def _compile(self, nodes, filename, silent):
        mode = "none" if silent else self.shell.ast_node_interactivity
        if mode == "last_expr_or_assign":
            last = nodes[-1] if nodes else None
            target = None
            if isinstance(last, ast.Assign) and len(last.targets) == 1:
                target = last.targets[0]
            elif isinstance(last, (ast.AnnAssign, ast.AugAssign)):
                target = last.target
            if isinstance(target, ast.Name):
                node = ast.Expr(ast.Name(target.id, ast.Load()))
                nodes = [*nodes, ast.fix_missing_locations(node)]
            mode = "last_expr"
        if mode == "last_expr":
            mode = "last" if nodes and isinstance(nodes[-1], ast.Expr) else "none"
        if mode not in {"all", "last", "none"}:
            raise ValueError(f"Unsupported interactivity: {mode}")
        compiler = self.shell.compile
        flags = ast.PyCF_ALLOW_TOP_LEVEL_AWAIT if self.shell.autoawait else 0
        compiled = []
        for index, node in enumerate(nodes):
            interactive = mode == "all" or (mode == "last" and index == len(nodes) - 1)
            tree = ast.Interactive([node]) if interactive else ast.Module([node], [])
            with compiler.extra_flags(flags):
                compiled.append(compiler(tree, filename, "single" if interactive else "exec"))
        return compiled

    async def _execute(
        self,
        raw_cell: str,
        execution_count: int,
        *,
        silent: bool,
        store_history: bool,
        user_expressions: Mapping[str, str] | None,
    ) -> CellResult:
        shell = self.shell
        preprocessing_exc: tuple[Any, Any, Any] | None = None
        try:
            transformed = shell.transform_cell(raw_cell)
        except Exception:
            transformed = raw_cell
            preprocessing_exc = sys.exc_info()
        info = ExecutionInfo(
            raw_cell,
            store_history,
            silent,
            True,
            None,
            None,
            transformed_cell=transformed,
        )
        result = ExecutionResult(info)
        result.execution_count = execution_count
        if not raw_cell or raw_cell.isspace():
            return CellResult(None, execution_count, {})
        if silent:
            store_history = False
        if store_history and shell.history_manager:
            shell.history_manager.store_inputs(execution_count, transformed, raw_cell)
        if not silent:
            shell.logger.log(transformed, raw_cell)
        if preprocessing_exc is not None:
            shell.showtraceback(preprocessing_exc)
            result.error_before_exec = preprocessing_exc[1]
            return CellResult(None, execution_count, {}, result.error_before_exec)
        compiler = shell.compile
        with shell.builtin_trap:
            cell_name = compiler.cache(transformed, execution_count, raw_code=raw_cell)
            with shell.display_trap:
                try:
                    code_ast = compiler.ast_parse(transformed, filename=cell_name)
                    code_ast = shell.transform_ast(code_ast)
                    compiled = self._compile(code_ast.body, cell_name, silent)
                except Exception as exc:
                    result.error_before_exec = exc
                    shell.showsyntaxerror()
                    return CellResult(None, execution_count, {}, result.error_before_exec)
                shell.displayhook.exec_result = result
                try:
                    shell.events.trigger("pre_execute")
                    if not silent:
                        shell.events.trigger("pre_run_cell", info)
                    for code in compiled:
                        try:
                            if code.co_flags & inspect.CO_COROUTINE:
                                await eval(code, shell.user_global_ns, shell.user_ns)
                            else:
                                exec(code, shell.user_global_ns, shell.user_ns)
                        except asyncio.CancelledError, ResetRequested:
                            raise
                        except BaseException as exc:
                            result.error_in_exec = exc
                            shell.showtraceback(running_compiled_code=True)
                            break
                finally:
                    shell.displayhook.exec_result = None
                    shell.events.trigger("post_execute")
                    if not silent:
                        shell.events.trigger("post_run_cell", result)
        shell.last_execution_succeeded = result.success
        shell.last_execution_result = result
        if store_history and shell.history_manager:
            shell.history_manager.store_output(execution_count)
            if result.error_in_exec is not None:
                shell.history_manager.exceptions[execution_count] = (
                    shell._format_exception_for_storage(result.error_in_exec)
                )
        expressions = {}
        if result.error_before_exec is None and result.error_in_exec is None:
            expressions = shell.user_expressions(dict(user_expressions or {}))
        error = result.error_before_exec or result.error_in_exec
        return CellResult(result.result, execution_count, expressions, error)

    def _send_event(
        self,
        handle: CellHandle,
        parent: dict[str, Any],
        state: str,
        error: BaseException | None = None,
    ) -> None:
        content: dict[str, Any] = {
            "exec_id": handle.id,
            "generation": handle.generation,
            "state": state,
        }
        if error is not None and state not in {"cancelled", "reset"}:
            content["error"] = f"{type(error).__name__}: {error}"
        self.kernel.session.send(self.kernel.iopub_socket, "mypr_cell", content, parent=parent)
