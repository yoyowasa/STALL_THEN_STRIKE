import argparse
import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Optional

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

    async with PyBotterSession(cfg) as sess:
        await sess.connect_ws(channels, on_info=lambda msg: logger.info(f"WS info {msg}"))
        store = sess.get_store()
        ob_view = OrderBookView(
            store,
            cfg.exchange.product_code,
            tick_size=Decimal("1"),
        )

        inventory = InventoryManager(cfg.risk)
        strategy = StallThenStrikeStrategy(cfg.strategy, tick_size=Decimal("1"), inventory=inventory)

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

                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            cancelled = True
        finally:
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
            block_rate = (ca_block / ca_total) if ca_total else 0
            logger.info(f"trades={trade_count}")
            logger.info(
                f"ca_gate total={ca_total} allow={ca_allow} block={ca_block} block_rate={block_rate}"
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
