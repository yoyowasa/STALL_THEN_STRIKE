import argparse
import asyncio
import os
import subprocess
import time
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from src.config.loader import load_app_config
from src.engine.executor import PaperExecutor
from src.engine.inventory import FillEvent, InventoryManager
from src.engine.strategy_stall import StallThenStrikeStrategy
from src.infra.orderbook_view import OrderBookView
from src.infra.pyb_session import PyBotterSession
from src.infra.ws_mux import WsMux
from src.types.dto import Action, BoardSnapshot, Side


def _setup_logging() -> None:
    log_dir = Path("logs") / "runtime"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    logger.add(log_dir / f"paper-{ts}.log", level="INFO", enqueue=True)


def _default_channels(product_code: str) -> list[str]:
    return [
        f"lightning_board_snapshot_{product_code}",
        f"lightning_board_{product_code}",
        f"lightning_ticker_{product_code}",
        f"lightning_executions_{product_code}",
    ]


def _git_rev() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def _safe_get(obj: Any, *path: str, default: Any = None) -> Any:
    cur = obj
    for key in path:
        if cur is None:
            return default
        if isinstance(cur, dict):
            if key in cur:
                cur = cur[key]
                continue
            return default
        if hasattr(cur, key):
            cur = getattr(cur, key)
            continue
        return default
    return default if cur is None else cur


def _first_not_none(*values: Any, default: Any = None) -> Any:
    for v in values:
        if v is not None:
            return v
    return default


def _env_int(name: str) -> Optional[int]:
    val = os.getenv(name)
    if not val:
        return None
    try:
        return int(val)
    except ValueError:
        logger.warning(f"{name} invalid int: {val}")
        return None


def _env_float(name: str) -> Optional[float]:
    val = os.getenv(name)
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        logger.warning(f"{name} invalid float: {val}")
        return None


def _fmt_guard(v: Any) -> str:
    return "na" if v is None else str(v)


def _guard_thresholds() -> dict[str, Optional[float | int]]:
    thresholds: dict[str, Optional[float | int]] = {
        "consistency_ok_consecutive": _env_int("ENTRY_GUARD_REQUIRE_CONSISTENCY_OK_CONSECUTIVE"),
        "cooldown_remaining_sec": _env_float("ENTRY_GUARD_REQUIRE_COOLDOWN_REMAINING_SEC"),
        "event_latency_ms_p99": _env_float("ENTRY_GUARD_REQUIRE_EVENT_LATENCY_MS_P99"),
        "queue_depth_max": _env_int("ENTRY_GUARD_REQUIRE_QUEUE_DEPTH_MAX"),
        "ca_gate_block_rate": _env_float("ENTRY_GUARD_REQUIRE_CA_GATE_BLOCK_RATE"),
        "active_count": _env_int("ENTRY_GUARD_REQUIRE_ACTIVE_COUNT"),
        "errors": _env_int("ENTRY_GUARD_REQUIRE_ERRORS"),
    }
    if thresholds["active_count"] is None:
        thresholds["active_count"] = 0
    if thresholds["errors"] is None:
        thresholds["errors"] = 0
    return thresholds


def _guard_actual(
    *,
    engine: Any,
    strategy: StallThenStrikeStrategy,
    executor: PaperExecutor,
    error_count: int,
) -> dict[str, Any]:
    ca_total, ca_allow, ca_block = strategy.ca_gate_stats
    ca_block_rate = _first_not_none(
        _safe_get(engine, "metrics", "ca_gate_block_rate", default=None),
        (ca_block / ca_total) if ca_total else None,
        default=None,
    )
    return {
        "consistency_ok_consecutive": _first_not_none(
            _safe_get(engine, "health", "consistency_ok_consecutive", default=None),
            _safe_get(engine, "consistency_ok_consecutive", default=None),
            default=None,
        ),
        "cooldown_remaining_sec": _first_not_none(
            _safe_get(engine, "health", "cooldown_remaining_sec", default=None),
            _safe_get(engine, "cooldown_remaining_sec", default=None),
            default=None,
        ),
        "event_latency_ms_p99": _first_not_none(
            _safe_get(engine, "metrics", "event_latency_ms_p99", default=None),
            _safe_get(engine, "event_latency_ms_p99", default=None),
            default=None,
        ),
        "queue_depth_max": _first_not_none(
            _safe_get(engine, "metrics", "queue_depth_max", default=None),
            _safe_get(engine, "queue_depth_max", default=None),
            default=None,
        ),
        "ca_gate_block_rate": ca_block_rate,
        "active_count": len(executor.active_orders),
        "errors": error_count,
        "ca_gate_total": ca_total,
        "ca_gate_allow": ca_allow,
        "ca_gate_block": ca_block,
    }


def _guard_result(
    actual: dict[str, Any],
    thresholds: dict[str, Optional[float | int]],
    *,
    only: Optional[set[str]] = None,
) -> tuple[bool, str]:
    missing_metrics: list[str] = []
    missing_thresholds: list[str] = []
    threshold_fails: list[str] = []

    def _check(name: str, compare) -> None:
        if only is not None and name not in only:
            return
        actual_val = actual.get(name)
        thresh_val = thresholds.get(name)
        if actual_val is None:
            missing_metrics.append(name)
        if thresh_val is None:
            missing_thresholds.append(name)
            return
        if actual_val is None:
            return
        if not compare(actual_val, thresh_val):
            threshold_fails.append(name)

    _check("consistency_ok_consecutive", lambda a, t: a >= t)
    _check("cooldown_remaining_sec", lambda a, t: a <= t)
    _check("event_latency_ms_p99", lambda a, t: a <= t)
    _check("queue_depth_max", lambda a, t: a <= t)
    _check("ca_gate_block_rate", lambda a, t: a <= t)
    _check("active_count", lambda a, t: a == t)
    _check("errors", lambda a, t: a == t)

    reasons: list[str] = []
    if missing_metrics:
        reasons.append("missing_metric:" + ",".join(missing_metrics))
    if missing_thresholds:
        reasons.append("missing_threshold:" + ",".join(missing_thresholds))
    if threshold_fails:
        reasons.append("threshold_fail:" + ",".join(threshold_fails))
    return (len(reasons) == 0), ";".join(reasons)


def _emit_guard(
    prefix: str,
    *,
    cfg,
    cfg_rev: str,
    cfg_files: list[str],
    mode: str,
    actual: dict[str, Any],
    thresholds: dict[str, Optional[float | int]],
) -> None:
    logger.info(
        f"{prefix}_ACTUAL "
        f"cfg_rev={cfg_rev} cfg_files={cfg_files} mode={mode} "
        f"consistency_ok_consecutive={_fmt_guard(actual['consistency_ok_consecutive'])} "
        f"cooldown_remaining_sec={_fmt_guard(actual['cooldown_remaining_sec'])} "
        f"event_latency_ms_p99={_fmt_guard(actual['event_latency_ms_p99'])} "
        f"queue_depth_max={_fmt_guard(actual['queue_depth_max'])} "
        f"ca_gate_win_ms={cfg.strategy.ca_ratio_win_ms} ca_gate_thr={cfg.strategy.ca_threshold} "
        f"ca_gate_block_rate={_fmt_guard(actual['ca_gate_block_rate'])} "
        f"active_count={_fmt_guard(actual['active_count'])} errors={_fmt_guard(actual['errors'])}"
    )
    logger.info(
        f"{prefix}_THRESH "
        f"require_consistency_ok_consecutive>={_fmt_guard(thresholds['consistency_ok_consecutive'])} "
        f"require_cooldown_remaining_sec<={_fmt_guard(thresholds['cooldown_remaining_sec'])} "
        f"require_event_latency_ms_p99<={_fmt_guard(thresholds['event_latency_ms_p99'])} "
        f"require_queue_depth_max<={_fmt_guard(thresholds['queue_depth_max'])} "
        f"require_ca_gate_block_rate<={_fmt_guard(thresholds['ca_gate_block_rate'])} "
        f"require_active_count=={_fmt_guard(thresholds['active_count'])} "
        f"require_errors=={_fmt_guard(thresholds['errors'])}"
    )
    passed, reason = _guard_result(actual, thresholds)
    logger.info(f"{prefix}_RESULT pass={str(passed).lower()} reason=\"{reason}\"")


class RollingWindow:
    def __init__(self, window_sec: float) -> None:
        self.window_sec = max(0.1, window_sec)
        self.samples: deque[tuple[float, float]] = deque()

    def add(self, ts_mono: float, value: float) -> None:
        self.samples.append((ts_mono, float(value)))
        self._trim(ts_mono)

    def _trim(self, now_mono: float) -> None:
        cutoff = now_mono - self.window_sec
        while self.samples and self.samples[0][0] < cutoff:
            self.samples.popleft()

    def percentile(self, p: float, now_mono: float) -> Optional[float]:
        self._trim(now_mono)
        if not self.samples:
            return None
        values = sorted(v for _, v in self.samples)
        idx = int((p / 100.0) * (len(values) - 1))
        return float(values[max(0, min(idx, len(values) - 1))])


class RollingWindowMax:
    def __init__(self, window_sec: float) -> None:
        self.window_sec = max(0.1, window_sec)
        self.samples: deque[tuple[float, int]] = deque()

    def add(self, ts_mono: float, value: int) -> None:
        self.samples.append((ts_mono, int(value)))
        self._trim(ts_mono)

    def _trim(self, now_mono: float) -> None:
        cutoff = now_mono - self.window_sec
        while self.samples and self.samples[0][0] < cutoff:
            self.samples.popleft()

    def max(self, now_mono: float) -> Optional[int]:
        self._trim(now_mono)
        if not self.samples:
            return None
        return max(v for _, v in self.samples)


class RollingWindowRate:
    def __init__(self, window_sec: float) -> None:
        self.window_sec = max(0.1, window_sec)
        self.samples: deque[tuple[float, bool]] = deque()

    def add(self, ts_mono: float, is_block: bool) -> None:
        self.samples.append((ts_mono, bool(is_block)))
        self._trim(ts_mono)

    def _trim(self, now_mono: float) -> None:
        cutoff = now_mono - self.window_sec
        while self.samples and self.samples[0][0] < cutoff:
            self.samples.popleft()

    def rate(self, now_mono: float) -> Optional[float]:
        self._trim(now_mono)
        if not self.samples:
            return None
        total = len(self.samples)
        blocked = sum(1 for _, b in self.samples if b)
        return blocked / total if total else None


class RuntimeHealth:
    def __init__(self, cooldown_sec: float) -> None:
        self.consistency_ok_consecutive = 0
        self._cooldown_sec = max(0.0, cooldown_sec)
        self._cooldown_until_mono = 0.0

    def record_consistency(self, ok: bool, *, now_mono: float) -> None:
        if ok:
            self.consistency_ok_consecutive += 1
            return
        self.consistency_ok_consecutive = 0
        if self._cooldown_sec > 0:
            self._cooldown_until_mono = max(self._cooldown_until_mono, now_mono + self._cooldown_sec)

    @property
    def cooldown_remaining_sec(self) -> float:
        remaining = self._cooldown_until_mono - time.monotonic()
        return max(0.0, remaining)


class RuntimeMetrics:
    def __init__(self, *, latency_window_sec: float, queue_window_sec: float, ca_gate_window_sec: float) -> None:
        self._latency = RollingWindow(latency_window_sec)
        self._queue_depth = RollingWindowMax(queue_window_sec)
        self._ca_gate = RollingWindowRate(ca_gate_window_sec)

    def record_event_latency(self, latency_ms: float, *, now_mono: float) -> None:
        self._latency.add(now_mono, latency_ms)

    def record_queue_depth(self, depth: int, *, now_mono: float) -> None:
        self._queue_depth.add(now_mono, depth)

    def record_ca_gate(self, state: str, *, now_mono: float) -> None:
        self._ca_gate.add(now_mono, state == "BLOCK")

    @property
    def event_latency_ms_p99(self) -> Optional[float]:
        return self._latency.percentile(99, time.monotonic())

    @property
    def queue_depth_max(self) -> Optional[int]:
        return self._queue_depth.max(time.monotonic())

    @property
    def ca_gate_block_rate(self) -> Optional[float]:
        return self._ca_gate.rate(time.monotonic())


class RuntimeEngine:
    def __init__(self, *, health: RuntimeHealth, metrics: RuntimeMetrics) -> None:
        self.health = health
        self.metrics = metrics


async def _run_listen(cfg_path: str | None, duration_sec: int) -> None:
    cfg = load_app_config("configs/base.yml", cfg_path)
    _setup_logging()

    channels = list(dict.fromkeys(cfg.ws.channels + _default_channels(cfg.exchange.product_code)))
    logger.info(
        "START "
        f"mode=listen override={cfg_path} duration_sec={duration_sec} "
        f"product_code={cfg.exchange.product_code} ws_url={cfg.exchange.ws_url}"
    )
    logger.info(f"Subscribing channels={channels}")

    async with PyBotterSession(cfg) as sess:
        await sess.connect_ws(channels, on_info=lambda msg: logger.info(f"WS info {msg}"))
        store = sess.get_store()
        ob_view = OrderBookView(
            store,
            cfg.exchange.product_code,
            tick_size=Decimal("1"),
        )

        end = datetime.now(tz=timezone.utc).timestamp() + duration_sec
        last_best: tuple[Decimal | None, Decimal | None] | None = None
        async with WsMux(store, product_code=cfg.exchange.product_code) as mux:
            try:
                while datetime.now(tz=timezone.utc).timestamp() < end:
                    evt = await mux.get(timeout=10)
                    if evt.kind == "board":
                        snap = ob_view.snapshot(datetime.now(tz=timezone.utc))
                        best = (snap.best_bid_price, snap.best_ask_price)
                        if best != last_best:
                            last_best = best
                            logger.info(
                                f"board bid={snap.best_bid_price} ask={snap.best_ask_price} "
                                f"age_ms={snap.best_age_ms} spread_ticks={snap.spread_ticks}"
                            )
                    elif evt.kind == "ticker":
                        logger.info(f"ticker {evt.operation}: {evt.data}")
                    elif evt.kind == "executions":
                        logger.info(f"executions {evt.operation}: {evt.data}")
                    elif evt.kind == "child_order_events":
                        logger.info(f"child_order_events {evt.operation}: {evt.data}")
            except asyncio.CancelledError:
                return


async def _run_trade(
    cfg_path: str | None,
    *,
    duration_sec: int,
    eval_interval_ms: int,
    summary_interval_sec: int,
    fill_mode: str,
    fill_ratio: float,
    fill_delay_sec: float,
) -> None:
    cfg = load_app_config("configs/base.yml", cfg_path)
    _setup_logging()

    channels = list(dict.fromkeys(cfg.ws.channels + _default_channels(cfg.exchange.product_code)))
    logger.info(
        "START "
        f"mode=trade override={cfg_path} duration_sec={duration_sec} "
        f"eval_interval_ms={eval_interval_ms} summary_interval_sec={summary_interval_sec} "
        f"fill_mode={fill_mode} fill_ratio={fill_ratio} fill_delay_sec={fill_delay_sec} "
        f"product_code={cfg.exchange.product_code} "
        f"stall_T_ms={cfg.strategy.stall_T_ms} min_spread_tick={cfg.strategy.min_spread_tick} "
        f"ttl_ms={cfg.strategy.ttl_ms} max_reverse_ticks={cfg.strategy.max_reverse_ticks} "
        f"quote_mode={cfg.strategy.quote_mode} quote_offset_ticks={cfg.strategy.quote_offset_ticks} "
        f"size_min={cfg.strategy.size_min} max_inventory_btc={cfg.strategy.max_inventory_btc} "
        f"risk_daily_pnl_jpy={cfg.risk.daily_pnl_jpy} risk_max_dd_jpy={cfg.risk.max_dd_jpy}"
    )
    logger.info(f"cfg.env={cfg.env} product_code={cfg.exchange.product_code}")
    logger.info(f"Subscribing channels={channels} (paper trade)")
    cfg_files = ["configs/base.yml"]
    if cfg_path:
        cfg_files.append(cfg_path)
    cfg_rev = _git_rev()

    async with PyBotterSession(cfg) as sess:
        await sess.connect_ws(channels, on_info=lambda msg: logger.info(f"WS info {msg}"))
        store = sess.get_store()
        ob_view = OrderBookView(
            store,
            cfg.exchange.product_code,
            tick_size=Decimal("1"),
        )

        guard_thresholds = _guard_thresholds()
        guard_latency_window_sec = _env_float("ENTRY_GUARD_EVENT_LATENCY_WINDOW_SEC") or 60.0
        guard_queue_window_sec = _env_float("ENTRY_GUARD_QUEUE_DEPTH_WINDOW_SEC") or 60.0
        guard_ca_gate_window_sec = _env_float("ENTRY_GUARD_CA_GATE_WINDOW_SEC") or 60.0
        guard_consistency_interval_sec = _env_float("ENTRY_GUARD_CONSISTENCY_CHECK_INTERVAL_SEC") or 1.0
        guard_cooldown_sec = _env_float("ENTRY_GUARD_COOLDOWN_SEC") or 0.0

        metrics = RuntimeMetrics(
            latency_window_sec=guard_latency_window_sec,
            queue_window_sec=guard_queue_window_sec,
            ca_gate_window_sec=guard_ca_gate_window_sec,
        )
        health = RuntimeHealth(cooldown_sec=guard_cooldown_sec)
        engine = RuntimeEngine(health=health, metrics=metrics)

        inventory = InventoryManager(cfg.risk)
        strategy = StallThenStrikeStrategy(
            cfg.strategy,
            tick_size=Decimal("1"),
            inventory=inventory,
            metrics=metrics,
        )

        end_mono = time.monotonic() + duration_sec
        last_eval = 0.0
        eval_interval = max(1, eval_interval_ms) / 1000.0
        last_summary = 0.0
        last_decision_type: Optional[str] = None

        last_snap: Optional[BoardSnapshot] = None
        trade_count = 0

        def _evt_ts(data: dict) -> datetime:
            v = data.get("event_date")
            if isinstance(v, str):
                try:
                    return datetime.fromisoformat(v.replace("Z", "+00:00"))
                except Exception:
                    pass
            return datetime.now(tz=timezone.utc)

        def handle_child_event(data: dict) -> None:
            nonlocal trade_count
            ts = _evt_ts(data)
            strategy.on_order_event(data, now=ts)
            if data.get("event_type") != "EXECUTION":
                return
            fill = FillEvent(
                ts=ts,
                order_id=str(data.get("child_order_acceptance_id") or ""),
                side=str(data.get("side") or "BUY"),  # type: ignore[arg-type]
                price=Decimal(str(data.get("price") or "0")),
                size=Decimal(str(data.get("size") or "0")),
                tag=str(data.get("tag") or "") or None,
            )
            inventory.apply_fill(fill)
            strategy.on_fill(fill)
            trade_count += 1
            st = inventory.pnl.state
            logger.info(
                f"FILL {fill.side} @{fill.price} size={fill.size} oid={fill.order_id} "
                f"pos={st.position_btc} avg={st.avg_price} pnl={st.total_pnl_jpy} tag={fill.tag}"
            )

        executor = PaperExecutor(
            store,
            product_code=cfg.exchange.product_code,
            fill_mode=fill_mode,
            fill_ratio=fill_ratio,
            fill_delay_sec=fill_delay_sec,
            on_child_event=handle_child_event,
        )
        error_count = 0
        entry_actual = _guard_actual(
            engine=engine,
            strategy=strategy,
            executor=executor,
            error_count=error_count,
        )
        _emit_guard(
            "ENTRY_GUARD",
            cfg=cfg,
            cfg_rev=cfg_rev,
            cfg_files=cfg_files,
            mode=cfg.env,
            actual=entry_actual,
            thresholds=guard_thresholds,
        )
        last_consistency_check = 0.0
        guard_for_orders = {
            "consistency_ok_consecutive",
            "cooldown_remaining_sec",
            "event_latency_ms_p99",
            "queue_depth_max",
            "ca_gate_block_rate",
            "errors",
        }
        entry_guard_last_pass: Optional[bool] = None
        entry_guard_last_reason = ""
        entry_guard_last_log = 0.0
        entry_guard_blocked_place_limit_total = 0
        entry_guard_enforce_last_reason = ""

        def _check_consistency() -> bool:
            return strategy.open_order_count == len(executor.active_orders)

        async def _consume_ws_events(mux: WsMux) -> None:
            while True:
                try:
                    evt = await mux.get(timeout=1)
                except asyncio.TimeoutError:
                    continue
                now_mono = time.monotonic()
                if evt.recv_mono:
                    metrics.record_event_latency((now_mono - evt.recv_mono) * 1000.0, now_mono=now_mono)
                metrics.record_queue_depth(evt.queue_depth, now_mono=now_mono)

        async def ensure_close_board(now: datetime) -> BoardSnapshot:
            nonlocal last_snap
            if last_snap and (last_snap.best_bid_price is not None or last_snap.best_ask_price is not None):
                return last_snap

            for _ in range(20):
                snap = ob_view.snapshot(now)
                if snap.best_bid_price is not None or snap.best_ask_price is not None:
                    last_snap = snap
                    return snap
                await asyncio.sleep(0.05)
                now = datetime.now(tz=timezone.utc)

            px = inventory.pnl.state.mark_price or inventory.avg_price
            if px is None:
                px = Decimal("0")
            last_snap = BoardSnapshot(
                ts=now,
                best_bid_price=px,
                best_bid_size=None,
                best_ask_price=px,
                best_ask_size=None,
                best_age_ms=0,
                spread_ticks=0,
            )
            return last_snap

        async with WsMux(store, product_code=cfg.exchange.product_code) as mux:
            ws_task = asyncio.create_task(_consume_ws_events(mux))
            cancelled = False
            try:
                while time.monotonic() < end_mono:
                    now = datetime.now(tz=timezone.utc)
                    now_mono = time.monotonic()

                    executor.poll(now=now)

                    if now_mono - last_eval >= eval_interval:
                        last_eval = now_mono
                        snap = ob_view.snapshot(now)
                        last_snap = snap
                        actions, meta = strategy.on_board(snap, now=now)
                        if any(a.kind == "place_limit" for a in actions):
                            guard_actual = _guard_actual(
                                engine=engine,
                                strategy=strategy,
                                executor=executor,
                                error_count=error_count,
                            )
                            guard_pass, guard_reason = _guard_result(
                                guard_actual,
                                guard_thresholds,
                                only=guard_for_orders,
                            )
                            if not guard_pass:
                                blocked = [a for a in actions if a.kind == "place_limit"]
                                actions = [a for a in actions if a.kind != "place_limit"]
                                entry_guard_blocked_place_limit_total += len(blocked)
                                entry_guard_enforce_last_reason = guard_reason
                                if (now_mono - entry_guard_last_log >= 1.0) or (
                                    guard_reason != entry_guard_last_reason
                                ):
                                    logger.info('ENTRY_GUARD_ENFORCE pass=false reason="{}" blocked_place_limit={}', guard_reason, len(blocked))
                                    entry_guard_last_log = now_mono
                                    entry_guard_last_reason = guard_reason
                                entry_guard_last_pass = False
                            else:
                                if entry_guard_last_pass is False:
                                    logger.info('ENTRY_GUARD_ENFORCE pass=true reason=""')
                                entry_guard_last_pass = True
                        executor.execute(actions, now=now, board=snap)
                        executor.poll(now=now)

                        if actions or (meta.decision_type != (last_decision_type or "idle")):
                            st = inventory.pnl.state
                            logger.info(
                                f"DECISION {meta.decision_type} reason={meta.reason} "
                                f"age_ms={meta.best_age_ms} spread={meta.spread_ticks} "
                                f"open_orders={strategy.open_order_count} pos={st.position_btc} pnl={st.total_pnl_jpy} "
                                f"actions={[a.kind for a in actions]}"
                            )
                            last_decision_type = meta.decision_type

                    if now_mono - last_summary >= summary_interval_sec:
                        last_summary = now_mono
                        st = inventory.pnl.state
                        logger.info(
                            f"SUMMARY open_orders={strategy.open_order_count} active_orders={len(executor.active_orders)} "
                            f"pos={st.position_btc} avg={st.avg_price} mark={st.mark_price} "
                            f"realized={st.realized_pnl_jpy} unrealized={st.unrealized_pnl_jpy} total={st.total_pnl_jpy}"
                        )

                    if now_mono - last_consistency_check >= guard_consistency_interval_sec:
                        last_consistency_check = now_mono
                        health.record_consistency(_check_consistency(), now_mono=now_mono)

                    await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                cancelled = True
            finally:
                ws_task.cancel()
                await asyncio.gather(ws_task, return_exceptions=True)
                now = datetime.now(tz=timezone.utc)
                pos0 = inventory.position_btc
                pnl0 = inventory.pnl.state.total_pnl_jpy
                open0 = strategy.open_order_count
                active0 = len(executor.active_orders)

                executor.execute([Action(kind="cancel_all_stall", tag=strategy.tag)], now=now)

                pos = inventory.position_btc
                if pos != 0:
                    board = await ensure_close_board(now)
                    close_side: Side = "SELL" if pos > 0 else "BUY"
                    executor.execute(
                        [Action(kind="close_market", side=close_side, size=abs(pos), tag="shutdown")],
                        now=now,
                        board=board,
                    )
                executor.poll(now=now)

                promote_actual = _guard_actual(
                    engine=engine,
                    strategy=strategy,
                    executor=executor,
                    error_count=error_count,
                )
                _emit_guard(
                    "PROMOTE_GUARD",
                    cfg=cfg,
                    cfg_rev=cfg_rev,
                    cfg_files=cfg_files,
                    mode=cfg.env,
                    actual=promote_actual,
                    thresholds=guard_thresholds,
                )
                logger.info(
                    'ENTRY_GUARD_ENFORCE_SUMMARY blocked_place_limit_total={} last_reason="{}"',
                    entry_guard_blocked_place_limit_total,
                    entry_guard_enforce_last_reason,
                )

                pos1 = inventory.position_btc
                pnl1 = inventory.pnl.state.total_pnl_jpy
                logger.info(
                    "SHUTDOWN "
                    f"cancelled={cancelled} pos_before={pos0} pos_after={pos1} "
                    f"open_orders_before={open0} open_orders_after={strategy.open_order_count} "
                    f"active_orders_before={active0} active_orders_after={len(executor.active_orders)} "
                    f"pnl_before={pnl0} pnl_after={pnl1}"
                )
                ca_total, ca_allow, ca_block = strategy.ca_gate_stats
                block_rate = (ca_block / ca_total) if ca_total else None
                logger.info(f"trades={trade_count}")
                logger.info(
                    f"CA_GATE_SUMMARY total={ca_total} allow={ca_allow} block={ca_block} "
                    f"block_rate={_fmt_guard(block_rate)}"
                )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--override", default="configs/paper.yml", help="Override config YAML (default: configs/paper.yml)")
    parser.add_argument("--mode", choices=["trade", "listen"], default="trade")
    parser.add_argument("--duration-sec", type=int, default=60)
    parser.add_argument("--eval-interval-ms", type=int, default=50)
    parser.add_argument("--summary-interval-sec", type=int, default=10)

    parser.add_argument("--fill-mode", choices=["none", "one", "both"], default="one")
    parser.add_argument("--fill-ratio", type=float, default=1.0)
    parser.add_argument("--fill-delay-sec", type=float, default=0.2)

    args = parser.parse_args()
    try:
        if args.mode == "listen":
            asyncio.run(_run_listen(args.override, args.duration_sec))
        else:
            asyncio.run(
                _run_trade(
                    args.override,
                    duration_sec=args.duration_sec,
                    eval_interval_ms=args.eval_interval_ms,
                    summary_interval_sec=args.summary_interval_sec,
                    fill_mode=args.fill_mode,
                    fill_ratio=args.fill_ratio,
                    fill_delay_sec=args.fill_delay_sec,
                )
            )
    except KeyboardInterrupt:
        return
    except asyncio.CancelledError:
        return


if __name__ == "__main__":
    main()
