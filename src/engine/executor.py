from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import random
from secrets import token_hex
from typing import Callable, Optional

import pybotters

from src.types.dto import Action, BoardSnapshot, Side


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _emit_child_order_events(store: pybotters.bitFlyerDataStore, *, items: list[dict]) -> None:
    store.onmessage(
        {
            "jsonrpc": "2.0",
            "method": "channelMessage",
            "params": {"channel": "child_order_events", "message": items},
        },
        None,
    )


@dataclass
class PaperOrder:
    oid: str
    child_order_id: str
    side: Side
    price: Decimal
    remaining: Decimal
    tag: str
    expire_at: datetime


@dataclass(frozen=True)
class _ScheduledFill:
    due_mono: float
    oid: str
    price: Decimal
    size: Decimal


class PaperExecutor:
    """Action -> simulated child_order_events (ORDER/EXECUTION/CANCEL).

    - No REST calls
    - Can force-fill orders to exercise strategy lifecycle
    """

    def __init__(
        self,
        store: pybotters.bitFlyerDataStore,
        *,
        product_code: str,
        fill_mode: str = "one",
        fill_ratio: float = 1.0,
        fill_delay_sec: float = 0.2,
        on_child_event: Optional[Callable[[dict], None]] = None,
    ) -> None:
        self.store = store
        self.product_code = product_code
        self.fill_mode = fill_mode  # none|one|both
        self.fill_ratio = Decimal(str(fill_ratio))
        self.fill_delay_sec = float(fill_delay_sec)
        self.on_child_event = on_child_event

        self._orders: dict[str, PaperOrder] = {}
        self._fills: list[_ScheduledFill] = []
        self._one_scheduled = False

    @property
    def active_orders(self) -> dict[str, PaperOrder]:
        return dict(self._orders)

    def execute(self, actions: list[Action], *, now: datetime, board: Optional[BoardSnapshot] = None) -> None:
        now_mono = time.monotonic()
        for a in actions:
            if a.kind == "place_limit":
                if not a.side or a.price is None or a.size is None:
                    continue
                ttl_ms = int(a.ttl_ms or 0)
                self._place_limit(
                    side=a.side,
                    price=a.price,
                    size=a.size,
                    ttl_ms=ttl_ms,
                    tag=str(a.tag or ""),
                    now=now,
                    now_mono=now_mono,
                )
            elif a.kind == "cancel_all_stall":
                self._cancel_all(tag=str(a.tag or "stall"), now=now)
            elif a.kind == "close_market":
                if not a.side or a.size is None or a.size <= 0:
                    continue
                if board is None:
                    continue
                self._close_market(
                    side=a.side,
                    size=a.size,
                    tag=str(a.tag or "close"),
                    now=now,
                    board=board,
                )

    def poll(self, *, now: datetime, board: Optional[BoardSnapshot] = None) -> None:
        now_mono = time.monotonic()

        due, pending = [], []
        for s in self._fills:
            (due if s.due_mono <= now_mono else pending).append(s)
        self._fills = pending

        for s in due:
            order = self._orders.get(s.oid)
            if not order:
                continue
            fill_size = min(order.remaining, s.size)
            if fill_size <= 0:
                continue
            order.remaining -= fill_size
            evt = {
                "product_code": self.product_code,
                "child_order_id": order.child_order_id,
                "child_order_acceptance_id": order.oid,
                "event_date": _iso_z(now),
                "event_type": "EXECUTION",
                "side": order.side,
                "price": int(s.price),
                "size": float(fill_size),
                "outstanding_size": float(order.remaining),
                "commission": 0.0,
                "sfd": 0.0,
                "tag": order.tag,
            }
            self._emit([evt])
            if order.remaining <= 0:
                self._orders.pop(order.oid, None)

        if not self._orders and not self._fills:
            self._one_scheduled = False

    def _place_limit(
        self,
        *,
        side: Side,
        price: Decimal,
        size: Decimal,
        ttl_ms: int,
        tag: str,
        now: datetime,
        now_mono: float,
    ) -> str:
        oid = f"PAPER-{now.strftime('%Y%m%d-%H%M%S')}-{token_hex(3)}"
        child_order_id = f"PAPERCO-{token_hex(6)}"
        expire_at = now + timedelta(milliseconds=max(0, ttl_ms))
        order = PaperOrder(
            oid=oid,
            child_order_id=child_order_id,
            side=side,
            price=price,
            remaining=size,
            tag=tag,
            expire_at=expire_at,
        )
        self._orders[oid] = order
        self._emit(
            [
                {
                    "product_code": self.product_code,
                    "child_order_id": child_order_id,
                    "child_order_acceptance_id": oid,
                    "event_date": _iso_z(now),
                    "event_type": "ORDER",
                    "child_order_type": "LIMIT",
                    "side": side,
                    "price": int(price),
                    "size": float(size),
                    "expire_date": _iso_z(expire_at),
                    "tag": tag,
                }
            ]
        )

        if self.fill_mode not in {"one", "both"}:
            return oid

        fill_size = size * self.fill_ratio
        fill_size = min(size, max(Decimal("0"), fill_size))
        if fill_size <= 0:
            return oid

        if self.fill_mode == "one" and self._one_scheduled:
            # Randomize which side fills for paired quotes (BUY/SELL placed back-to-back).
            if not self._fills:
                return oid
            last = self._fills[-1]
            expected_due = now_mono + max(0.0, self.fill_delay_sec)
            same_cycle = abs(last.due_mono - expected_due) <= 1e-3
            if same_cycle and random.random() < 0.5:
                self._fills[-1] = _ScheduledFill(
                    due_mono=last.due_mono,
                    oid=oid,
                    price=price,
                    size=fill_size,
                )
            return oid

        self._fills.append(
            _ScheduledFill(
                due_mono=now_mono + max(0.0, self.fill_delay_sec),
                oid=oid,
                price=price,
                size=fill_size,
            )
        )
        if self.fill_mode == "one":
            self._one_scheduled = True
        return oid

    def _cancel_all(self, *, tag: str, now: datetime) -> None:
        cancel_oids = [oid for oid, o in self._orders.items() if o.tag == tag]
        if not cancel_oids:
            return

        self._fills = [s for s in self._fills if s.oid not in set(cancel_oids)]
        for oid in cancel_oids:
            o = self._orders.pop(oid, None)
            if not o:
                continue
            self._emit(
                [
                    {
                        "product_code": self.product_code,
                        "child_order_id": o.child_order_id,
                        "child_order_acceptance_id": o.oid,
                        "event_date": _iso_z(now),
                        "event_type": "CANCEL",
                        "price": int(o.price),
                        "size": float(o.remaining),
                        "tag": o.tag,
                    }
                ]
            )

    def _close_market(self, *, side: Side, size: Decimal, tag: str, now: datetime, board: BoardSnapshot) -> None:
        if side == "BUY":
            px = board.best_ask_price or board.best_bid_price
        else:
            px = board.best_bid_price or board.best_ask_price
        if px is None:
            return

        oid = f"PAPERCLOSE-{now.strftime('%Y%m%d-%H%M%S')}-{token_hex(3)}"
        child_order_id = f"PAPERCLOSECO-{token_hex(6)}"
        self._emit(
            [
                {
                    "product_code": self.product_code,
                    "child_order_id": child_order_id,
                    "child_order_acceptance_id": oid,
                    "event_date": _iso_z(now),
                    "event_type": "ORDER",
                    "child_order_type": "MARKET",
                    "side": side,
                    "price": int(px),
                    "size": float(size),
                    "expire_date": _iso_z(now + timedelta(days=1)),
                    "tag": tag,
                },
                {
                    "product_code": self.product_code,
                    "child_order_id": child_order_id,
                    "child_order_acceptance_id": oid,
                    "event_date": _iso_z(now),
                    "event_type": "EXECUTION",
                    "side": side,
                    "price": int(px),
                    "size": float(size),
                    "outstanding_size": 0.0,
                    "commission": 0.0,
                    "sfd": 0.0,
                    "tag": tag,
                },
            ]
        )

    def _emit(self, items: list[dict]) -> None:
        _emit_child_order_events(self.store, items=items)
        if self.on_child_event:
            for it in items:
                self.on_child_event(it)
