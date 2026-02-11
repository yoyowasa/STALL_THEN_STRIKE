from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import os  # 【ABガード】環境変数で「期待するca_threshold」を受け取るため
import time  # 何をするimportか：C/A比の窓（ms）を動かすために現在時刻msを作る
from typing import Optional, Any

from loguru import logger

from src.config.loader import StallStrategyConfig
from src.engine.inventory import FillEvent, InventoryManager
from src.features.cancel_add_ratio import CancelAddRatio  # 何をするimportか：Best層のCancel/Add比を計算する部品を使う
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
        metrics: Optional[Any] = None,
    ) -> None:
        self.cfg = cfg
        self._ca = CancelAddRatio(win_ms=self.cfg.ca_ratio_win_ms)  # 何をするコードか：直近win_msのBest層C/A比を計算する状態を初期化する
        self._metrics = metrics
        expected_thr = os.getenv("AB_EXPECT_CA_THRESHOLD")  # 【ABガード】このランで期待するca_threshold（例: "1.3" / "999"）を外から渡す
        actual_thr = float(self.cfg.ca_threshold)  # 【ABガード】実際に読み込まれたca_threshold（ログと判定の唯一の真実）
        actual_win_ms = int(self.cfg.ca_ratio_win_ms)  # 【ABガード】実際に読み込まれた窓（ms）

        logger.info(
            "ca_gate_config win_ms={} thr={} expected_thr={}",
            actual_win_ms,
            actual_thr,
            expected_thr,
        )  # 【可観測性】paper本体ログに残す
        print(
            f"ca_gate_config win_ms={actual_win_ms} thr={actual_thr} expected_thr={expected_thr}"
        )  # 【可観測性】ca_gate_stdout にも同じ内容を残す

        if expected_thr is not None and abs(float(expected_thr) - actual_thr) > 1e-12:  # 【ABガード】想定と違う設定なら、比較が壊れるので即停止
            raise RuntimeError(
                f"AB_EXPECT_CA_THRESHOLD mismatch: expected={expected_thr} actual={actual_thr}"
            )  # 【ABガード】無駄な600秒を回さない
        self._ca_last_log_ms = 0  # 何をするコードか：C/Aゲートの状態ログを1秒に1回だけ出すための前回ログ時刻(ms)を持つ
        self._ca_gate_total = 0
        self._ca_gate_allow = 0
        self._ca_gate_block = 0
        self.tick_size = tick_size
        self.inventory = inventory
        self.tag = tag
        self._min_order_size = Decimal(str(self.cfg.size_min))

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

    @property
    def ca_gate_stats(self) -> tuple[int, int, int]:
        return self._ca_gate_total, self._ca_gate_allow, self._ca_gate_block

    def reconcile_open_orders(
        self,
        *,
        active_orders: list[dict[str, Any]],
        now: datetime,
    ) -> tuple[int, int]:
        """Align local open-order view with executor's reconciled active orders."""
        desired: dict[str, _OpenOrder] = {}
        default_expire = now + timedelta(milliseconds=int(self.cfg.ttl_ms))
        for row in active_orders:
            if not isinstance(row, dict):
                continue
            oid = str(row.get("oid") or "")
            if not oid:
                continue
            side: Side = "BUY" if str(row.get("side") or "BUY") == "BUY" else "SELL"
            rem = row.get("remaining")
            size = Decimal(str(rem if rem is not None else row.get("size") or "0"))
            price = Decimal(str(row.get("price") or "0"))
            desired[oid] = _OpenOrder(
                oid=oid,
                side=side,
                size=max(Decimal("0"), size),
                price=price,
                tag=self.tag,
                expire_at=default_expire,
            )

        removed = 0
        for oid in list(self._open.keys()):
            if oid in desired:
                continue
            self._open.pop(oid, None)
            removed += 1

        added = 0
        for oid, order in desired.items():
            current = self._open.get(oid)
            if current is None:
                self._open[oid] = order
                added += 1
                continue
            current.side = order.side
            current.size = order.size
            current.price = order.price
            current.expire_at = order.expire_at

        return removed, added

    def on_order_event(self, evt: dict, *, now: datetime) -> None:
        oid = str(evt.get("child_order_acceptance_id") or "")
        if not oid:
            return
        et = str(evt.get("event_type") or "")
        tag = str(evt.get("tag") or "")
        is_our_tag = tag == self.tag
        is_known_oid = oid in self._open

        if et == "ORDER":
            if not is_our_tag:
                return
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
            return

        # Some child_order_events arrive without tag; process terminal updates
        # if oid is already tracked locally.
        if not (is_our_tag or is_known_oid):
            return

        if et in {"CANCEL", "EXPIRE", "ORDER_FAILED", "REJECTED"}:
            self._open.pop(oid, None)
            return

        if et == "EXECUTION":
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

        best_bid_price = board.best_bid_price
        best_bid_size = board.best_bid_size
        best_ask_price = board.best_ask_price
        best_ask_size = board.best_ask_size

        now_ms = int(time.time() * 1000)  # 何をするコードか：C/A比のローリング窓のための現在時刻(ms)を作る
        if (
            best_bid_price is not None
            and best_bid_size is not None
            and best_ask_price is not None
            and best_ask_size is not None
        ):
            self._ca.update(
                ts_ms=now_ms,
                best_bid_price=float(best_bid_price),
                best_bid_size=float(best_bid_size),
                best_ask_price=float(best_ask_price),
                best_ask_size=float(best_ask_size),
            )  # 何をするコードか：Best層の変化を取り込み、直近win_msのCancel/Add比を更新する

        add_sum, cancel_sum, _ = self._ca.snapshot()
        # Cancel/Add比は「add=0」の窓だと評価不能なので、ゼロ割りを避けて ALLOW 扱いにする
        ratio = (cancel_sum / add_sum) if add_sum > 0 else 0.0  # add=0 のとき ratio=inf にしない（誤ブロック防止）

        if now_ms - self._ca_last_log_ms >= 1000:  # 何をするコードか：ログを毎ティック出さず、1秒に1回だけに絞る
            state = "ALLOW" if ratio <= self.cfg.ca_threshold else "BLOCK"  # 何をするコードか：しきい値以内なら発注可、超えたらブロックと判定する
            self._ca_gate_total += 1
            if state == "ALLOW":
                self._ca_gate_allow += 1
            else:
                self._ca_gate_block += 1
            if self._metrics is not None and hasattr(self._metrics, "record_ca_gate"):
                self._metrics.record_ca_gate(state, now_mono=time.monotonic())
            print(  # 何をするコードか：C/Aゲートの“今”を実行ログに出して、効いているか確認できるようにする
                f"ca_gate state={state} win_ms={self.cfg.ca_ratio_win_ms} "
                f"thr={self.cfg.ca_threshold} add={add_sum} cancel={cancel_sum} ratio={ratio}"
            )
            self._ca_last_log_ms = now_ms  # 何をするコードか：今回ログを出した時刻を保存する

        if best_bid_price is not None and best_ask_price is not None:
            mark = (best_bid_price + best_ask_price) / 2
            self.inventory.update_mark(mark)

        inv = self.inventory.state
        if inv.side == "flat":
            self._close_in_flight = False
            self._close_requested_at = None
        if self.inventory.kill_switch():
            if self._open:
                self._append_cancel_once(actions)
            if inv.side != "flat":
                if inv.size >= self._min_order_size:
                    close_side: Side = "SELL" if inv.side == "long" else "BUY"
                    actions.append(Action(kind="close_market", side=close_side, size=inv.size, tag="risk"))
            meta = DecisionMeta(
                ts=now,
                decision_type="kill",
                reason=(
                    "risk_limit_reached"
                    if (inv.side == "flat" or inv.size >= self._min_order_size)
                    else "risk_limit_with_dust_position"
                ),
                actions=actions,
                best_age_ms=board.best_age_ms,
                spread_ticks=board.spread_ticks,
                inventory_btc=self.inventory.position_btc,
            )
            return actions, meta

        ca_ratio = ratio
        ca_blocked = ca_ratio > self.cfg.ca_threshold
        if ca_blocked and self._open:
            logger.info(
                f"CA gate cancel_all ratio={ca_ratio:.4f} threshold={self.cfg.ca_threshold:.4f}"
            )
            self._append_cancel_once(actions)

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

                if inv.size < self._min_order_size:
                    self._close_in_flight = False
                    self._close_requested_at = None
                    meta = DecisionMeta(
                        ts=now,
                        decision_type="hold",
                        reason="position_dust_below_min",
                        actions=actions,
                        best_age_ms=board.best_age_ms,
                        spread_ticks=board.spread_ticks,
                        inventory_btc=self.inventory.position_btc,
                    )
                    return actions, meta

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
            if ca_blocked:
                logger.info(
                    f"CA gate entry_blocked ratio={ca_ratio:.4f} threshold={self.cfg.ca_threshold:.4f}"
                )
                meta = DecisionMeta(
                    ts=now,
                    decision_type="idle",
                    reason="ca_gate_ratio_high",
                    actions=actions,
                    best_age_ms=board.best_age_ms,
                    spread_ticks=board.spread_ticks,
                    inventory_btc=self.inventory.position_btc,
                )
                return actions, meta

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
