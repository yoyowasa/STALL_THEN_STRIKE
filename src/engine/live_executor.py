from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from loguru import logger

from src.infra.http_bitflyer import BitflyerHttp
from src.types.dto import Action, BoardSnapshot, Side


def _as_decimal(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


@dataclass
class LiveOrder:
    oid: str
    child_order_id: str
    side: Side
    price: Optional[Decimal]
    remaining: Decimal
    tag: str
    created_at: datetime


class LiveExecutor:
    """Action を bitFlyer REST に変換して実行する実運用向け実行器。"""

    def __init__(
        self,
        http: BitflyerHttp,
        *,
        product_code: str,
        reconcile_interval_sec: float = 5.0,
    ) -> None:
        self.http = http
        self.product_code = product_code
        self.reconcile_interval_sec = max(1.0, reconcile_interval_sec)
        self._orders: dict[str, LiveOrder] = {}
        self._tag_by_oid: dict[str, str] = {}
        self._last_reconcile_mono = 0.0

    @property
    def active_orders(self) -> dict[str, LiveOrder]:
        return dict(self._orders)

    def active_order_count(self, *, tag: Optional[str] = None) -> int:
        if tag is None:
            return len(self._orders)
        return sum(1 for order in self._orders.values() if order.tag == tag)

    def annotate_child_event(self, event: dict[str, Any]) -> dict[str, Any]:
        data = dict(event)
        oid = str(data.get("child_order_acceptance_id") or "")
        if not oid:
            return data
        tag = str(data.get("tag") or "")
        if tag:
            self._tag_by_oid[oid] = tag
            return data
        mapped = self._tag_by_oid.get(oid)
        if mapped:
            data["tag"] = mapped
        return data

    def on_child_event(self, event: dict[str, Any]) -> None:
        data = self.annotate_child_event(event)
        oid = str(data.get("child_order_acceptance_id") or "")
        if not oid:
            return
        et = str(data.get("event_type") or "")
        if not et:
            return

        if et == "ORDER":
            tag = str(data.get("tag") or self._tag_by_oid.get(oid) or "")
            if not tag:
                return
            self._tag_by_oid[oid] = tag
            side: Side = "BUY" if str(data.get("side") or "BUY") == "BUY" else "SELL"
            size = _as_decimal(data.get("outstanding_size"))
            if size is None:
                size = _as_decimal(data.get("size")) or Decimal("0")
            self._orders[oid] = LiveOrder(
                oid=oid,
                child_order_id=str(data.get("child_order_id") or ""),
                side=side,
                price=_as_decimal(data.get("price")),
                remaining=max(Decimal("0"), size),
                tag=tag,
                created_at=datetime.now(tz=timezone.utc),
            )
            return

        if et == "EXECUTION":
            order = self._orders.get(oid)
            outstanding = _as_decimal(data.get("outstanding_size"))
            if order is None:
                return
            if outstanding is None:
                return
            order.remaining = max(Decimal("0"), outstanding)
            if order.remaining <= 0:
                self._orders.pop(oid, None)
                self._tag_by_oid.pop(oid, None)
            return

        if et in {"CANCEL", "EXPIRE", "ORDER_FAILED", "REJECTED"}:
            self._orders.pop(oid, None)
            self._tag_by_oid.pop(oid, None)

    async def execute(
        self,
        actions: list[Action],
        *,
        now: datetime,
        board: Optional[BoardSnapshot] = None,
    ) -> None:
        for action in actions:
            if action.kind == "place_limit":
                if not action.side or action.price is None or action.size is None:
                    continue
                await self._place_limit(
                    side=action.side,
                    price=action.price,
                    size=action.size,
                    ttl_ms=int(action.ttl_ms or 0),
                    tag=str(action.tag or ""),
                    now=now,
                )
                continue

            if action.kind == "cancel_all_stall":
                await self._cancel_all(tag=str(action.tag or "stall"))
                continue

            if action.kind == "close_market":
                if not action.side or action.size is None or action.size <= 0:
                    continue
                await self._close_market(
                    side=action.side,
                    size=action.size,
                    tag=str(action.tag or "close"),
                )
                continue

    async def poll(self, *, now: datetime, force_reconcile: bool = False) -> None:
        now_mono = time.monotonic()
        if not force_reconcile and (now_mono - self._last_reconcile_mono) < self.reconcile_interval_sec:
            return
        await self._reconcile_active()
        self._last_reconcile_mono = now_mono

    async def _place_limit(
        self,
        *,
        side: Side,
        price: Decimal,
        size: Decimal,
        ttl_ms: int,
        tag: str,
        now: datetime,
    ) -> None:
        res = await self.http.send_limit_order(
            side=side,
            price=int(price),
            size=float(size),
            product_code=self.product_code,
            ttl_ms=ttl_ms,
            tag=tag or None,
        )
        oid = str(res.get("child_order_acceptance_id") or "")
        if not oid:
            raise RuntimeError(f"send_limit_order response missing id: {res}")
        if tag:
            self._tag_by_oid[oid] = tag
        self._orders[oid] = LiveOrder(
            oid=oid,
            child_order_id="",
            side=side,
            price=price,
            remaining=size,
            tag=tag,
            created_at=now,
        )

    async def _cancel_all(self, *, tag: str) -> None:
        cancel_oids = [oid for oid, order in self._orders.items() if order.tag == tag]
        for oid in cancel_oids:
            try:
                await self.http.cancel_order(oid, product_code=self.product_code)
            except Exception as exc:
                logger.warning(f"cancel_order failed oid={oid} err={exc}")

    async def _close_market(self, *, side: Side, size: Decimal, tag: str) -> None:
        res = await self.http.send_market_order(
            side=side,
            size=float(size),
            product_code=self.product_code,
            tag=tag or None,
        )
        oid = str(res.get("child_order_acceptance_id") or "")
        if not oid:
            raise RuntimeError(f"send_market_order response missing id: {res}")
        if tag:
            self._tag_by_oid[oid] = tag

    async def _reconcile_active(self) -> None:
        try:
            active = await self.http.get_child_orders(
                product_code=self.product_code,
                child_order_state="ACTIVE",
                count=100,
            )
        except Exception as exc:
            logger.warning(f"get_child_orders failed: {exc}")
            return
        if not isinstance(active, list):
            logger.warning(f"get_child_orders unexpected response: {active}")
            return

        active_ids: set[str] = set()
        for row in active:
            oid = str(row.get("child_order_acceptance_id") or "")
            if not oid:
                continue
            active_ids.add(oid)

            mapped_tag = self._tag_by_oid.get(oid) or str(row.get("tag") or "")
            if mapped_tag:
                self._tag_by_oid[oid] = mapped_tag

            order = self._orders.get(oid)
            if order is None and mapped_tag:
                side: Side = "BUY" if str(row.get("side") or "BUY") == "BUY" else "SELL"
                size = _as_decimal(row.get("size")) or Decimal("0")
                rem = _as_decimal(row.get("outstanding_size")) or size
                self._orders[oid] = LiveOrder(
                    oid=oid,
                    child_order_id=str(row.get("child_order_id") or ""),
                    side=side,
                    price=_as_decimal(row.get("price")),
                    remaining=max(Decimal("0"), rem),
                    tag=mapped_tag,
                    created_at=datetime.now(tz=timezone.utc),
                )
                continue

            if order is not None:
                rem = _as_decimal(row.get("outstanding_size"))
                if rem is not None:
                    order.remaining = max(Decimal("0"), rem)

        for oid in list(self._orders.keys()):
            if oid in active_ids:
                continue
            self._orders.pop(oid, None)
            self._tag_by_oid.pop(oid, None)
