import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Optional

import pybotters


WsEventKind = Literal["board", "ticker", "executions", "child_order_events"]


@dataclass(frozen=True)
class WsEvent:
    kind: WsEventKind
    ts: datetime
    operation: str
    data: dict[str, Any]
    recv_mono: float = 0.0
    queue_depth: int = 0


class WsMux:
    """Unify multiple pybotters DataStore watch streams into a single async stream."""

    def __init__(
        self,
        store: pybotters.bitFlyerDataStore,
        *,
        product_code: str,
        kinds: Optional[set[WsEventKind]] = None,
    ) -> None:
        self.store = store
        self.product_code = product_code
        self.kinds = kinds
        self._queue: asyncio.Queue[WsEvent] = asyncio.Queue()
        self._tasks: list[asyncio.Task] = []
        self._closed = False

    async def __aenter__(self) -> "WsMux":
        self.start()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    def start(self) -> None:
        if self._tasks:
            return
        store_name_to_kind: dict[str, WsEventKind] = {
            "board": "board",
            "ticker": "ticker",
            "executions": "executions",
            "childorderevents": "child_order_events",
        }
        tasks: list[asyncio.Task] = []
        for store_name, kind in store_name_to_kind.items():
            if self.kinds is not None and kind not in self.kinds:
                continue
            tasks.append(asyncio.create_task(self._watch(store_name)))
        self._tasks = tasks

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    async def get(self, timeout: Optional[float] = None) -> WsEvent:
        if timeout is None:
            return await self._queue.get()
        if timeout <= 0:
            try:
                return self._queue.get_nowait()
            except asyncio.QueueEmpty:
                raise asyncio.TimeoutError()
        return await asyncio.wait_for(self._queue.get(), timeout=timeout)

    def __aiter__(self) -> "WsMux":
        return self

    async def __anext__(self) -> WsEvent:
        if self._closed:
            raise StopAsyncIteration
        return await self.get()

    async def _watch(self, store_name: str) -> None:
        kind_map: dict[str, WsEventKind] = {
            "board": "board",
            "ticker": "ticker",
            "executions": "executions",
            "childorderevents": "child_order_events",
        }
        kind = kind_map[store_name]
        ds = getattr(self.store, store_name)
        with ds.watch() as stream:
            async for change in stream:
                data = change.data
                if isinstance(data, dict):
                    pc = data.get("product_code")
                    if pc and pc != self.product_code:
                        continue
                now_mono = time.monotonic()
                evt = WsEvent(
                    kind=kind,
                    ts=datetime.now(tz=timezone.utc),
                    operation=change.operation,
                    data=dict(change.data),
                    recv_mono=now_mono,
                    queue_depth=self._queue.qsize() + 1,
                )
                await self._queue.put(evt)
