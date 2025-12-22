from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional

from src.config.loader import StallStrategyConfig
from src.engine.inventory import FillEvent, InventoryManager
from src.types.dto import Action, BoardSnapshot, Side


@dataclass(frozen=True)
class DecisionMeta:
    ts: datetime
    decision_type: str
    reason: str
    actions: list[Action]
    best_age_ms: int
    spread_ticks: Optional[int]
    inventory_btc: Decimal


@dataclass
class _OpenOrder:
    oid: str
    side: Side
    size: Decimal
    price: Decimal
    tag: str
    expire_at: datetime


class StallThenStrikeStrategy:
    """Best が静止したら両面に一撃置く（paper 検証用の最小実装）。"""

    def __init__(
        self,
        cfg: StallStrategyConfig,
        *,
        tick_size: Decimal,
        inventory: InventoryManager,
        tag: str = "stall",
    ) -> None:
        self.cfg = cfg
        self.tick_size = tick_size
        self.inventory = inventory
        self.tag = tag

        self._open: dict[str, _OpenOrder] = {}
        self._orders_until: Optional[datetime] = None
        self._pos_opened_at: Optional[datetime] = None
        self._last_pos: Decimal = Decimal("0")
        self._close_in_flight: bool = False
        self._close_requested_at: Optional[datetime] = None

    def _append_cancel_once(self, actions: list[Action]) -> None:
        for a in actions:
            if a.kind == "cancel_all_stall" and a.tag == self.tag:
                return
        actions.append(Action(kind="cancel_all_stall", tag=self.tag))

    @property
    def open_order_count(self) -> int:
        return len(self._open)

    def on_order_event(self, evt: dict, *, now: datetime) -> None:
        tag = str(evt.get("tag") or "")
        if tag != self.tag:
            return
        oid = str(evt.get("child_order_acceptance_id") or "")
        if not oid:
            return

        et = str(evt.get("event_type") or "")
        if et == "ORDER":
            expire_at = now + timedelta(milliseconds=int(self.cfg.ttl_ms))
            if isinstance(evt.get("expire_date"), str):
                try:
                    expire_at = datetime.fromisoformat(evt["expire_date"].replace("Z", "+00:00"))
                except Exception:
                    pass
            self._open[oid] = _OpenOrder(
                oid=oid,
                side=str(evt.get("side") or "BUY"),  # type: ignore[assignment]
                size=Decimal(str(evt.get("size") or "0")),
                price=Decimal(str(evt.get("price") or "0")),
                tag=tag,
                expire_at=expire_at,
            )
        elif et == "CANCEL":
            self._open.pop(oid, None)
        elif et == "EXECUTION":
            outstanding = evt.get("outstanding_size")
            if outstanding is not None:
                try:
                    if Decimal(str(outstanding)) <= 0:
                        self._open.pop(oid, None)
                except Exception:
                    pass

    def on_fill(self, fill: FillEvent) -> None:
        pos = self.inventory.position_btc
        if self._last_pos == 0 and pos != 0:
            self._pos_opened_at = fill.ts
        if pos == 0:
            self._pos_opened_at = None
            self._close_in_flight = False
            self._close_requested_at = None
        self._last_pos = pos

    def on_board(self, board: BoardSnapshot, *, now: datetime) -> tuple[list[Action], DecisionMeta]:
        actions: list[Action] = []

        if board.best_bid_price is not None and board.best_ask_price is not None:
            mark = (board.best_bid_price + board.best_ask_price) / 2
            self.inventory.update_mark(mark)

        inv = self.inventory.state
        if inv.side == "flat":
            self._close_in_flight = False
            self._close_requested_at = None
        if self.inventory.kill_switch():
            if self._open:
                self._append_cancel_once(actions)
            if inv.side != "flat":
                close_side: Side = "SELL" if inv.side == "long" else "BUY"
                actions.append(Action(kind="close_market", side=close_side, size=inv.size, tag="risk"))
            meta = DecisionMeta(
                ts=now,
                decision_type="kill",
                reason="risk limit reached",
                actions=actions,
                best_age_ms=board.best_age_ms,
                spread_ticks=board.spread_ticks,
                inventory_btc=self.inventory.position_btc,
            )
            return actions, meta

        cancel_due = bool(self._orders_until and now >= self._orders_until and self._open)

        # 1) if position open: cancel remaining orders and close on TTL/reverse
        if inv.side != "flat":
            if cancel_due:
                self._append_cancel_once(actions)

            should_close = False
            reason = ""

            if self._pos_opened_at and now - self._pos_opened_at >= timedelta(milliseconds=int(self.cfg.ttl_ms)):
                should_close = True
                reason = "position_ttl"
            else:
                avg = inv.avg_price
                mark = self.inventory.pnl.state.mark_price
                if avg is not None and mark is not None:
                    thr = self.tick_size * Decimal(str(self.cfg.max_reverse_ticks))
                    if inv.side == "long" and (avg - mark) >= thr:
                        should_close = True
                        reason = "reverse_long"
                    elif inv.side == "short" and (mark - avg) >= thr:
                        should_close = True
                        reason = "reverse_short"

            if should_close and inv.size > 0:
                if self._open:
                    self._append_cancel_once(actions)

                allow_close = (not self._close_in_flight) or (
                    self._close_requested_at is not None
                    and (now - self._close_requested_at) >= timedelta(seconds=2)
                )

                if allow_close:
                    close_side = "SELL" if inv.side == "long" else "BUY"
                    actions.append(Action(kind="close_market", side=close_side, size=inv.size, tag="close"))
                    self._close_in_flight = True
                    self._close_requested_at = now
                    meta = DecisionMeta(
                        ts=now,
                        decision_type="close",
                        reason=reason,
                        actions=actions,
                        best_age_ms=board.best_age_ms,
                        spread_ticks=board.spread_ticks,
                        inventory_btc=self.inventory.position_btc,
                    )
                    return actions, meta

                meta = DecisionMeta(
                    ts=now,
                    decision_type="hold",
                    reason="close_pending",
                    actions=actions,
                    best_age_ms=board.best_age_ms,
                    spread_ticks=board.spread_ticks,
                    inventory_btc=self.inventory.position_btc,
                )
                return actions, meta

            meta = DecisionMeta(
                ts=now,
                decision_type="hold",
                reason="position_open",
                actions=actions,
                best_age_ms=board.best_age_ms,
                spread_ticks=board.spread_ticks,
                inventory_btc=self.inventory.position_btc,
            )
            return actions, meta

        # 3) entry
        if cancel_due:
            self._append_cancel_once(actions)

        stall_ready = (
            board.best_bid_price is not None
            and board.best_ask_price is not None
            and board.best_age_ms >= self.cfg.stall_T_ms
            and board.spread_ticks is not None
            and board.spread_ticks >= self.cfg.min_spread_tick
        )
        inv_ok = inv.size < Decimal(str(self.cfg.max_inventory_btc))

        if stall_ready and inv_ok and not self._open:
            bid = board.best_bid_price
            ask = board.best_ask_price
            mid = (bid + ask) / 2

            off = max(0, int(getattr(self.cfg, "quote_offset_ticks", 1)))
            mode = str(getattr(self.cfg, "quote_mode", "mid")).lower()
            if mode == "inside":
                buy_px = (bid + self.tick_size * off).quantize(self.tick_size, rounding=ROUND_HALF_UP)
                sell_px = (ask - self.tick_size * off).quantize(self.tick_size, rounding=ROUND_HALF_UP)
            else:
                buy_px = (mid - self.tick_size * off).quantize(self.tick_size, rounding=ROUND_HALF_UP)
                sell_px = (mid + self.tick_size * off).quantize(self.tick_size, rounding=ROUND_HALF_UP)

            if buy_px >= sell_px:
                meta = DecisionMeta(
                    ts=now,
                    decision_type="idle",
                    reason="quote_crossed",
                    actions=actions,
                    best_age_ms=board.best_age_ms,
                    spread_ticks=board.spread_ticks,
                    inventory_btc=self.inventory.position_btc,
                )
                return actions, meta

            size = Decimal(str(self.cfg.size_min))
            actions.extend(
                [
                    Action(
                        kind="place_limit",
                        side="BUY",
                        price=buy_px,
                        size=size,
                        ttl_ms=int(self.cfg.ttl_ms),
                        tag=self.tag,
                    ),
                    Action(
                        kind="place_limit",
                        side="SELL",
                        price=sell_px,
                        size=size,
                        ttl_ms=int(self.cfg.ttl_ms),
                        tag=self.tag,
                    ),
                ]
            )
            self._orders_until = now + timedelta(milliseconds=int(self.cfg.ttl_ms))
            meta = DecisionMeta(
                ts=now,
                decision_type="entry",
                reason="stall_ready",
                actions=actions,
                best_age_ms=board.best_age_ms,
                spread_ticks=board.spread_ticks,
                inventory_btc=self.inventory.position_btc,
            )
            return actions, meta

        meta = DecisionMeta(
            ts=now,
            decision_type="idle",
            reason="no_action",
            actions=actions,
            best_age_ms=board.best_age_ms,
            spread_ticks=board.spread_ticks,
            inventory_btc=self.inventory.position_btc,
        )
        return actions, meta
