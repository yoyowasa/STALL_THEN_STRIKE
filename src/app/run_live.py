import argparse
import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional

from loguru import logger

from src.app.run_paper import (
    RuntimeEngine,
    RuntimeHealth,
    RuntimeMetrics,
    _default_channels,
    _emit_guard,
    _env_float,
    _git_rev,
    _guard_actual,
    _guard_result,
    _guard_thresholds,
)
from src.config.loader import load_app_config
from src.engine.inventory import FillEvent, InventoryManager
from src.engine.live_executor import LiveExecutor
from src.engine.strategy_stall import StallThenStrikeStrategy
from src.infra.http_bitflyer import BitflyerHttp
from src.infra.orderbook_view import OrderBookView
from src.infra.pyb_session import PyBotterSession
from src.infra.ws_mux import WsMux
from src.types.dto import Action, BoardSnapshot, Side


def _setup_logging() -> None:
    log_dir = Path("logs") / "runtime"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    logger.add(log_dir / f"live-{ts}.log", level="INFO", enqueue=True)


def _live_channels(product_code: str, base_channels: list[str]) -> list[str]:
    return list(
        dict.fromkeys(base_channels + _default_channels(product_code) + ["child_order_events"])
    )


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
            except asyncio.CancelledError:
                return


async def _run_trade(
    cfg_path: str | None,
    *,
    duration_sec: int,
    eval_interval_ms: int,
    summary_interval_sec: int,
) -> None:
    cfg = load_app_config("configs/base.yml", cfg_path)
    _setup_logging()

    channels = _live_channels(cfg.exchange.product_code, cfg.ws.channels)
    logger.info(
        "START "
        f"mode=trade override={cfg_path} duration_sec={duration_sec} "
        f"eval_interval_ms={eval_interval_ms} summary_interval_sec={summary_interval_sec} "
        f"product_code={cfg.exchange.product_code} "
        f"stall_T_ms={cfg.strategy.stall_T_ms} min_spread_tick={cfg.strategy.min_spread_tick} "
        f"ttl_ms={cfg.strategy.ttl_ms} max_reverse_ticks={cfg.strategy.max_reverse_ticks} "
        f"quote_mode={cfg.strategy.quote_mode} quote_offset_ticks={cfg.strategy.quote_offset_ticks} "
        f"size_min={cfg.strategy.size_min} max_inventory_btc={cfg.strategy.max_inventory_btc} "
        f"risk_daily_pnl_jpy={cfg.risk.daily_pnl_jpy} risk_max_dd_jpy={cfg.risk.max_dd_jpy}"
    )
    logger.info(f"cfg.env={cfg.env} product_code={cfg.exchange.product_code}")
    logger.info(f"Subscribing channels={channels} (live trade)")
    cfg_files = ["configs/base.yml"]
    if cfg_path:
        cfg_files.append(cfg_path)
    cfg_rev = _git_rev()

    sub_acks: dict[str, bool] = {}
    child_subscribed = asyncio.Event()

    def on_info(msg: dict[str, Any]) -> None:
        logger.info(f"WS info {msg}")
        sub_id = msg.get("id")
        if not isinstance(sub_id, str) or not sub_id.startswith("sub:"):
            return
        sub_acks[sub_id] = bool(msg.get("result") is True)
        if sub_id == "sub:child_order_events" and msg.get("result") is True:
            child_subscribed.set()

    async with PyBotterSession(cfg) as sess:
        if not sess.api_key or not sess.api_secret:
            raise RuntimeError("BF_API_KEY/BF_API_SECRET が未設定です。")

        await sess.connect_ws(channels, on_info=on_info)
        try:
            await asyncio.wait_for(child_subscribed.wait(), timeout=10)
        except asyncio.TimeoutError as exc:
            raise RuntimeError("child_order_events の購読確認に失敗しました。") from exc

        store = sess.get_store()
        http = BitflyerHttp(sess.get_client(), cfg.exchange)
        ob_view = OrderBookView(
            store,
            cfg.exchange.product_code,
            tick_size=Decimal("1"),
        )

        guard_thresholds = _guard_thresholds()
        guard_latency_window_sec = _env_float("ENTRY_GUARD_EVENT_LATENCY_WINDOW_SEC") or 60.0
        guard_queue_window_sec = _env_float("ENTRY_GUARD_QUEUE_DEPTH_WINDOW_SEC") or 60.0
        guard_ca_gate_window_sec = _env_float("ENTRY_GUARD_CA_GATE_WINDOW_SEC") or 60.0
        guard_consistency_interval_sec = (
            _env_float("ENTRY_GUARD_CONSISTENCY_CHECK_INTERVAL_SEC") or 1.0
        )
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
        error_count = 0

        def _evt_ts(data: dict[str, Any]) -> datetime:
            v = data.get("event_date")
            if isinstance(v, str):
                try:
                    return datetime.fromisoformat(v.replace("Z", "+00:00"))
                except Exception:
                    pass
            return datetime.now(tz=timezone.utc)

        def handle_child_event(data: dict[str, Any]) -> None:
            nonlocal trade_count
            ts = _evt_ts(data)
            strategy.on_order_event(data, now=ts)
            if data.get("event_type") != "EXECUTION":
                return
            side: Side = "BUY" if str(data.get("side") or "BUY") == "BUY" else "SELL"
            fill = FillEvent(
                ts=ts,
                order_id=str(data.get("child_order_acceptance_id") or ""),
                side=side,
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

        executor = LiveExecutor(
            http,
            product_code=cfg.exchange.product_code,
            reconcile_interval_sec=5.0,
        )

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
            return strategy.open_order_count == executor.active_order_count(tag=strategy.tag)

        async def _consume_ws_events(mux: WsMux) -> None:
            nonlocal error_count
            while True:
                try:
                    evt = await mux.get(timeout=1)
                except asyncio.TimeoutError:
                    continue
                now_mono = time.monotonic()
                if evt.recv_mono:
                    latency_ms = (now_mono - evt.recv_mono) * 1000.0
                    metrics.record_event_latency(latency_ms, now_mono=now_mono)
                metrics.record_queue_depth(evt.queue_depth, now_mono=now_mono)

                if evt.kind != "child_order_events":
                    continue
                try:
                    enriched = executor.annotate_child_event(evt.data)
                    executor.on_child_event(enriched)
                    handle_child_event(enriched)
                except Exception:
                    error_count += 1
                    logger.exception("child_order_events 処理で例外が発生しました。")

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

                    await executor.poll(now=now)

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
                                    logger.info(
                                        'ENTRY_GUARD_ENFORCE pass=false reason="{}" '
                                        "blocked_place_limit={}",
                                        guard_reason,
                                        len(blocked),
                                    )
                                    entry_guard_last_log = now_mono
                                    entry_guard_last_reason = guard_reason
                                entry_guard_last_pass = False
                            else:
                                if entry_guard_last_pass is False:
                                    logger.info('ENTRY_GUARD_ENFORCE pass=true reason=""')
                                entry_guard_last_pass = True

                        try:
                            await executor.execute(actions, now=now, board=snap)
                        except Exception:
                            error_count += 1
                            logger.exception("注文実行で例外が発生しました。")

                        await executor.poll(now=now)

                        if actions or (meta.decision_type != (last_decision_type or "idle")):
                            st = inventory.pnl.state
                            logger.info(
                                f"DECISION {meta.decision_type} reason={meta.reason} "
                                f"age_ms={meta.best_age_ms} spread={meta.spread_ticks} "
                                f"open_orders={strategy.open_order_count} "
                                f"active_orders={executor.active_order_count(tag=strategy.tag)} "
                                f"pos={st.position_btc} pnl={st.total_pnl_jpy} "
                                f"actions={[a.kind for a in actions]}"
                            )
                            last_decision_type = meta.decision_type

                    if now_mono - last_summary >= summary_interval_sec:
                        last_summary = now_mono
                        st = inventory.pnl.state
                        logger.info(
                            f"SUMMARY open_orders={strategy.open_order_count} "
                            f"active_orders={executor.active_order_count(tag=strategy.tag)} "
                            f"pos={st.position_btc} avg={st.avg_price} mark={st.mark_price} "
                            f"realized={st.realized_pnl_jpy} unrealized={st.unrealized_pnl_jpy} "
                            f"total={st.total_pnl_jpy}"
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
                active0 = executor.active_order_count(tag=strategy.tag)

                try:
                    await executor.execute(
                        [Action(kind="cancel_all_stall", tag=strategy.tag)],
                        now=now,
                    )
                except Exception:
                    error_count += 1
                    logger.exception("終了処理の cancel_all で例外が発生しました。")

                pos = inventory.position_btc
                if pos != 0:
                    board = await ensure_close_board(now)
                    close_side: Side = "SELL" if pos > 0 else "BUY"
                    try:
                        await executor.execute(
                            [Action(kind="close_market", side=close_side, size=abs(pos), tag="shutdown")],
                            now=now,
                            board=board,
                        )
                    except Exception:
                        error_count += 1
                        logger.exception("終了処理の close_market で例外が発生しました。")
                await executor.poll(now=now, force_reconcile=True)

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
                    'ENTRY_GUARD_ENFORCE_SUMMARY blocked_place_limit_total={} '
                    'last_reason="{}"',
                    entry_guard_blocked_place_limit_total,
                    entry_guard_enforce_last_reason,
                )

                pos1 = inventory.position_btc
                pnl1 = inventory.pnl.state.total_pnl_jpy
                logger.info(
                    "SHUTDOWN "
                    f"cancelled={cancelled} pos_before={pos0} pos_after={pos1} "
                    f"open_orders_before={open0} open_orders_after={strategy.open_order_count} "
                    f"active_orders_before={active0} "
                    f"active_orders_after={executor.active_order_count(tag=strategy.tag)} "
                    f"pnl_before={pnl0} pnl_after={pnl1}"
                )
                ca_total, ca_allow, ca_block = strategy.ca_gate_stats
                block_rate = (ca_block / ca_total) if ca_total else None
                logger.info(f"trades={trade_count}")
                logger.info(
                    f"CA_GATE_SUMMARY total={ca_total} allow={ca_allow} block={ca_block} "
                    f"block_rate={'na' if block_rate is None else block_rate}"
                )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--override",
        default="configs/live.yml",
        help="Override config YAML (default: configs/live.yml)",
    )
    parser.add_argument("--mode", choices=["trade", "listen"], default="trade")
    parser.add_argument("--duration-sec", type=int, default=60)
    parser.add_argument("--eval-interval-ms", type=int, default=50)
    parser.add_argument("--summary-interval-sec", type=int, default=10)
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
                )
            )
    except KeyboardInterrupt:
        return
    except asyncio.CancelledError:
        return


if __name__ == "__main__":
    main()
