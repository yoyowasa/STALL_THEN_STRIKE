from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Optional

from loguru import logger

from src.infra.http_bitflyer import BitflyerHttp
from src.types.dto import Action, BoardSnapshot, Side

API_LIMIT_PER_IP_BACKOFF_MIN_SEC = 1.5
API_LIMIT_PER_IP_BACKOFF_MAX_SEC = 30.0
_API_LIMIT_PER_IP_BACKOFF_SEC = API_LIMIT_PER_IP_BACKOFF_MIN_SEC
_API_LIMIT_PER_IP_COOLDOWN_UNTIL_MONO = 0.0


def _is_api_limit_per_ip_message(msg: str) -> bool:
    s = msg.lower()
    return "over api limit" in s and "per ip address" in s


def is_api_limit_per_ip_message(msg: str) -> bool:
    return _is_api_limit_per_ip_message(msg)


def api_limit_per_ip_cooldown_remaining_sec(*, now_mono: Optional[float] = None) -> float:
    if now_mono is None:
        now_mono = time.monotonic()
    return max(0.0, _API_LIMIT_PER_IP_COOLDOWN_UNTIL_MONO - now_mono)


def is_api_limit_per_ip_cooldown_active(*, now_mono: Optional[float] = None) -> bool:
    return api_limit_per_ip_cooldown_remaining_sec(now_mono=now_mono) > 0.0


def register_api_limit_per_ip_backoff(
    *,
    reason: str,
    err_msg: str,
    now_mono: Optional[float] = None,
) -> float:
    global _API_LIMIT_PER_IP_BACKOFF_SEC
    global _API_LIMIT_PER_IP_COOLDOWN_UNTIL_MONO
    if now_mono is None:
        now_mono = time.monotonic()
    _API_LIMIT_PER_IP_BACKOFF_SEC = min(
        _API_LIMIT_PER_IP_BACKOFF_SEC * 2.0,
        API_LIMIT_PER_IP_BACKOFF_MAX_SEC,
    )
    _API_LIMIT_PER_IP_COOLDOWN_UNTIL_MONO = max(
        _API_LIMIT_PER_IP_COOLDOWN_UNTIL_MONO,
        now_mono + _API_LIMIT_PER_IP_BACKOFF_SEC,
    )
    remaining_sec = api_limit_per_ip_cooldown_remaining_sec(now_mono=now_mono)
    logger.warning(
        "API_LIMIT_PER_IP_COOLDOWN_SET reason={} sleep_sec={} backoff_sec={} err={}",
        reason,
        round(remaining_sec, 3),
        round(_API_LIMIT_PER_IP_BACKOFF_SEC, 3),
        err_msg,
    )
    return remaining_sec


def reset_api_limit_per_ip_backoff(*, reason: str, now_mono: Optional[float] = None) -> None:
    global _API_LIMIT_PER_IP_BACKOFF_SEC
    global _API_LIMIT_PER_IP_COOLDOWN_UNTIL_MONO
    if now_mono is None:
        now_mono = time.monotonic()
    if (
        _API_LIMIT_PER_IP_BACKOFF_SEC == API_LIMIT_PER_IP_BACKOFF_MIN_SEC
        and _API_LIMIT_PER_IP_COOLDOWN_UNTIL_MONO <= now_mono
    ):
        return
    _API_LIMIT_PER_IP_BACKOFF_SEC = API_LIMIT_PER_IP_BACKOFF_MIN_SEC
    _API_LIMIT_PER_IP_COOLDOWN_UNTIL_MONO = 0.0
    logger.info("API_LIMIT_PER_IP_COOLDOWN_RESET reason={}", reason)


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
        http_min_interval_sec: Optional[float] = None,
    ) -> None:
        self.http = http
        self.product_code = product_code
        self.reconcile_interval_sec = max(1.0, reconcile_interval_sec)
        if http_min_interval_sec is None:
            raw = os.getenv("LIVE_HTTP_MIN_INTERVAL_SEC")
            try:
                http_min_interval_sec = float(raw) if raw is not None else 0.2
            except Exception:
                http_min_interval_sec = 0.2
        self.http_min_interval_sec = max(0.0, float(http_min_interval_sec))
        self._http_lock = asyncio.Lock()
        self._http_last_mono = 0.0
        self._orders: dict[str, LiveOrder] = {}
        self._tag_by_oid: dict[str, str] = {}
        self._tag_seen_mono: dict[str, float] = {}
        self.tag_cache_ttl_sec = 3600.0
        self._last_reconcile_mono = 0.0
        self._api_limit_per_ip_skip_last_log_mono = 0.0
        if self.http_min_interval_sec > 0:
            logger.info("LIVE_HTTP_MIN_INTERVAL_SEC={}", self.http_min_interval_sec)

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
            self._remember_tag(oid, tag)
            return data
        mapped = self._tag_by_oid.get(oid)
        if mapped:
            data["tag"] = mapped
            self._tag_seen_mono[oid] = time.monotonic()
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
            self._remember_tag(oid, tag)
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
            return

        if et in {"CANCEL", "EXPIRE", "ORDER_FAILED", "REJECTED"}:
            self._orders.pop(oid, None)

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
            self._prune_tag_cache(now_mono=now_mono)
            return
        await self._reconcile_active()
        self._last_reconcile_mono = now_mono
        self._prune_tag_cache(now_mono=now_mono)

    async def cancel_active_orders_on_start(
        self,
        *,
        settle_retry: int = 5,
        settle_interval_sec: float = 0.5,
        max_count: int = 500,
    ) -> dict[str, int]:
        active = await self._fetch_active_orders(count=max_count)
        before = len(active)
        cancel_requested = 0
        for row in active:
            oid = str(row.get("child_order_acceptance_id") or "")
            if not oid:
                continue
            try:
                await self._cancel_order(oid)
                cancel_requested += 1
            except Exception as exc:
                logger.warning(f"startup cancel failed oid={oid} err={exc}")

        after = before
        for _ in range(max(1, settle_retry)):
            await asyncio.sleep(max(0.1, settle_interval_sec))
            active_now = await self._fetch_active_orders(count=max_count)
            after = len(active_now)
            if after == 0:
                break

        await self._reconcile_active()
        return {
            "active_before": before,
            "cancel_requested": cancel_requested,
            "active_after": after,
        }

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
        await self._await_http_slot()
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
            self._remember_tag(oid, tag)
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
                await self._cancel_order(oid)
            except Exception as exc:
                logger.warning(f"cancel_order failed oid={oid} err={exc}")

    async def _close_market(self, *, side: Side, size: Decimal, tag: str) -> None:
        await self._await_http_slot()
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
            self._remember_tag(oid, tag)

    async def _fetch_active_orders(self, *, count: int = 100) -> list[dict[str, Any]]:
        now_mono = time.monotonic()
        remaining_sec = api_limit_per_ip_cooldown_remaining_sec(now_mono=now_mono)
        if remaining_sec > 0:
            if (now_mono - self._api_limit_per_ip_skip_last_log_mono) >= 1.0:
                logger.warning(
                    "get_child_orders skip due to API_LIMIT_PER_IP_COOLDOWN remaining_sec={}",
                    round(remaining_sec, 3),
                )
                self._api_limit_per_ip_skip_last_log_mono = now_mono
            return []

        try:
            await self._await_http_slot()
            active = await self.http.get_child_orders(
                product_code=self.product_code,
                child_order_state="ACTIVE",
                count=count,
            )
        except Exception as exc:
            err_msg = str(exc)
            if _is_api_limit_per_ip_message(err_msg):
                register_api_limit_per_ip_backoff(
                    reason="get_child_orders_exception",
                    err_msg=err_msg,
                    now_mono=time.monotonic(),
                )
            logger.warning(f"get_child_orders failed: {exc}")
            return []
        if not isinstance(active, list):
            err_msg = str(active)
            if _is_api_limit_per_ip_message(err_msg):
                register_api_limit_per_ip_backoff(
                    reason="get_child_orders_response",
                    err_msg=err_msg,
                    now_mono=time.monotonic(),
                )
            logger.warning(f"get_child_orders unexpected response: {active}")
            return []
        reset_api_limit_per_ip_backoff(reason="get_child_orders_success", now_mono=time.monotonic())
        out: list[dict[str, Any]] = []
        for row in active:
            if isinstance(row, dict):
                out.append(row)
        return out

    async def _reconcile_active(self) -> None:
        active = await self._fetch_active_orders(count=100)

        active_ids: set[str] = set()
        for row in active:
            oid = str(row.get("child_order_acceptance_id") or "")
            if not oid:
                continue
            active_ids.add(oid)

            mapped_tag = self._tag_by_oid.get(oid) or str(row.get("tag") or "")
            if mapped_tag:
                self._remember_tag(oid, mapped_tag)

            order = self._orders.get(oid)
            if order is None:
                side: Side = "BUY" if str(row.get("side") or "BUY") == "BUY" else "SELL"
                size = _as_decimal(row.get("size")) or Decimal("0")
                rem = _as_decimal(row.get("outstanding_size")) or size
                tag = mapped_tag or "__unknown__"
                self._orders[oid] = LiveOrder(
                    oid=oid,
                    child_order_id=str(row.get("child_order_id") or ""),
                    side=side,
                    price=_as_decimal(row.get("price")),
                    remaining=max(Decimal("0"), rem),
                    tag=tag,
                    created_at=datetime.now(tz=timezone.utc),
                )
                continue

            if order is not None:
                if mapped_tag and order.tag == "__unknown__":
                    order.tag = mapped_tag
                rem = _as_decimal(row.get("outstanding_size"))
                if rem is not None:
                    order.remaining = max(Decimal("0"), rem)

        for oid in list(self._orders.keys()):
            if oid in active_ids:
                continue
            self._orders.pop(oid, None)

    def _remember_tag(self, oid: str, tag: str) -> None:
        if not oid or not tag:
            return
        self._tag_by_oid[oid] = tag
        self._tag_seen_mono[oid] = time.monotonic()

    def _prune_tag_cache(self, *, now_mono: float) -> None:
        cutoff = now_mono - self.tag_cache_ttl_sec
        for oid, seen in list(self._tag_seen_mono.items()):
            if oid in self._orders:
                continue
            if seen >= cutoff:
                continue
            self._tag_seen_mono.pop(oid, None)
            self._tag_by_oid.pop(oid, None)

    async def _await_http_slot(self) -> None:
        if self.http_min_interval_sec <= 0:
            return
        async with self._http_lock:
            now_mono = time.monotonic()
            wait_sec = self.http_min_interval_sec - (now_mono - self._http_last_mono)
            if wait_sec > 0:
                await asyncio.sleep(wait_sec)
            self._http_last_mono = time.monotonic()

    async def _cancel_order(self, oid: str) -> Any:
        await self._await_http_slot()
        return await self.http.cancel_order(oid, product_code=self.product_code)
