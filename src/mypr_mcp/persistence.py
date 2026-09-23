from __future__ import annotations

import asyncio
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Any


class PersistenceUnavailable(RuntimeError):
    pass


@dataclass(slots=True)
class _Call:
    function: Callable[..., Any]
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    result: asyncio.Future[Any]


async def await_completion(task: asyncio.Future[Any]) -> Any:
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError as cancelled:
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
        task.result()
        raise cancelled


class PersistenceWorker:
    def __init__(self, queue_size: int = 128, *, on_failure=None):
        if queue_size < 1:
            raise ValueError("queue_size must be positive")
        self._loop = asyncio.get_running_loop()
        self._queue: asyncio.Queue[_Call | None] = asyncio.Queue(queue_size)
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mypr-io")
        self._closed = False
        self._failure = None
        self._on_failure = on_failure
        self._gate = asyncio.Lock()
        self._close_task: asyncio.Task[None] | None = None
        self._task = asyncio.create_task(self._run(), name="mypr:persistence")
        self._task.add_done_callback(self._finished)

    @property
    def available(self) -> bool:
        return not self._closed and not self._task.done() and not self._task.cancelling()

    def _finished(self, task):
        if self._closed and not task.cancelled() and task.exception() is None:
            return
        cause = "cancelled" if task.cancelled() else str(task.exception() or "exited")
        self._failure = PersistenceUnavailable(f"Persistence worker stopped unexpectedly: {cause}")
        self._closed = True
        self._fail_pending(self._failure)
        if self._on_failure is not None:
            self._on_failure(self._failure)

    def _fail_pending(self, error):
        while not self._queue.empty():
            pending = self._queue.get_nowait()
            if pending is not None:
                self._fail(pending, error)
            self._queue.task_done()

    async def call(self, function: Callable[..., Any], /, *args: Any, **kwargs: Any) -> Any:
        async with self._gate:
            if not self.available:
                raise self._failure or PersistenceUnavailable("Persistence worker is unavailable")
            result = self._loop.create_future()
            await self._queue.put(_Call(function, args, kwargs, result))
            if not self.available:
                self._fail_pending(
                    self._failure or PersistenceUnavailable("Persistence worker is unavailable")
                )
        try:
            return await asyncio.shield(result)
        except asyncio.CancelledError as cancelled:
            while not result.done():
                try:
                    await asyncio.shield(result)
                except asyncio.CancelledError:
                    continue
                except BaseException as exc:
                    raise exc from cancelled
            result.result()
            raise cancelled

    async def _run(self) -> None:
        current = None
        try:
            while True:
                current = await self._queue.get()
                if current is None:
                    self._queue.task_done()
                    return
                try:
                    work = self._loop.run_in_executor(
                        self._executor,
                        partial(current.function, *current.args, **current.kwargs),
                    )
                    value = await asyncio.shield(work)
                except asyncio.CancelledError as exc:
                    while not work.done():
                        try:
                            await asyncio.shield(work)
                        except asyncio.CancelledError:
                            continue
                        except BaseException:
                            break
                    if work.cancelled():
                        self._fail(current, exc)
                    else:
                        try:
                            current.result.set_result(work.result())
                        except BaseException as work_error:
                            self._fail(current, work_error)
                    raise
                except BaseException as exc:
                    self._fail(current, exc)
                else:
                    if not current.result.done():
                        current.result.set_result(value)
                finally:
                    self._queue.task_done()
                    current = None
        except asyncio.CancelledError:
            error = RuntimeError("Persistence worker stopped unexpectedly")
            if current is not None:
                self._fail(current, error)
                self._queue.task_done()
            while True:
                try:
                    pending = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if pending is not None:
                    self._fail(pending, error)
                self._queue.task_done()
            raise
        except BaseException as exc:
            if current is not None:
                self._fail(current, exc)
                self._queue.task_done()
            while True:
                try:
                    pending = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if pending is not None:
                    self._fail(pending, exc)
                self._queue.task_done()
            raise

    @staticmethod
    def _fail(call: _Call, error: BaseException) -> None:
        if not call.result.done():
            call.result.set_exception(error)

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        task = self._close_task
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as cancelled:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
            task.result()
            raise cancelled

    async def _close(self) -> None:
        async with self._gate:
            if not self._closed:
                self._closed = True
                if not self._task.done():
                    await self._queue.put(None)
        try:
            await asyncio.gather(self._task, return_exceptions=True)
        finally:
            await asyncio.to_thread(self._executor.shutdown, wait=True)
