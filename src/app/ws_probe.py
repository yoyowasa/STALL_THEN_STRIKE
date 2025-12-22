"""
WS probe: subscribe board/ticker/executions/child_order_events and optionally place/cancel a tiny order.

Usage:
    python -m src.app.ws_probe --mode probe                  # listen only
    python -m src.app.ws_probe --mode order-far              # far-from-market place+cancel
    python -m src.app.ws_probe --mode order-near             # near-best place+cancel
    python -m src.app.ws_probe --mode order-near --wait-sec 60
"""
import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from loguru import logger

from src.config.loader import load_app_config
from src.infra.http_bitflyer import BitflyerHttp
from src.infra.orderbook_view import OrderBookView
from src.infra.pyb_session import PyBotterSession


DEFAULT_CHANNELS = [
    "lightning_board_snapshot_FX_BTC_JPY",
    "lightning_board_FX_BTC_JPY",
    "lightning_ticker_FX_BTC_JPY",
    "lightning_executions_FX_BTC_JPY",
    "child_order_events",
]


def _setup_logging() -> None:
    log_dir = Path("logs") / "runtime"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    logger.add(log_dir / f"ws-probe-{ts}.log", level="INFO", enqueue=True)


async def place_and_cancel(
    http: BitflyerHttp,
    *,
    side: str,
    price: Decimal,
    size: Decimal,
    hold_sec: float,
) -> str:
    res = await http.send_limit_order(side=side, price=int(price), size=float(size))
    oid = res.get("child_order_acceptance_id")
    logger.info(f"Placed {side} @{price} size={size} -> {oid}")
    await asyncio.sleep(hold_sec)
    cancel_res = await http.cancel_order(oid)
    logger.info(f"Cancel requested for {oid}: {cancel_res}")
    return oid


async def run(mode: str, wait_sec: int, side: str, hold_sec: float) -> None:
    cfg = load_app_config("configs/base.yml", "configs/paper.yml")

    _setup_logging()

    msg_counts: Counter[str] = Counter()
    channel_counts: Counter[str] = Counter()
    child_subscribed = asyncio.Event()

    def on_message(msg: dict) -> None:
        method = msg.get("method") or "no_method"
        msg_counts[method] += 1
        params = msg.get("params")
        if isinstance(params, dict):
            ch = params.get("channel")
            if ch:
                channel_counts[ch] += 1
                if "child_order" in ch:
                    logger.info(f"WS raw child channel: {msg}")

    async with PyBotterSession(cfg) as sess:
        def on_info(msg: dict) -> None:
            logger.info(f"WS info {msg}")
            if msg.get("id") == "sub:child_order_events" and msg.get("result") is True:
                child_subscribed.set()

        await sess.connect_ws(
            DEFAULT_CHANNELS,
            on_info=on_info,
            on_message=on_message,
        )
        store = sess.get_store()
        client = sess.get_client()
        http = BitflyerHttp(client, cfg.exchange)
        ob_view = OrderBookView(store, cfg.exchange.product_code, tick_size=Decimal("1"))

        async def consume_child():
            with store.childorderevents.watch() as stream:
                try:
                    async for change in stream:
                        logger.info(f"child_order_events {change.operation}: {change.data}")
                except asyncio.CancelledError:
                    return

        async def consume_ticker():
            with store.ticker.watch() as stream:
                try:
                    change = await asyncio.wait_for(stream.__anext__(), timeout=10)
                    logger.info(f"ticker {change.operation}: {change.data}")
                except Exception as e:
                    logger.warning(f"ticker stream end: {e}")

        async def consume_executions():
            with store.executions.watch() as stream:
                try:
                    change = await asyncio.wait_for(stream.__anext__(), timeout=10)
                    logger.info(f"executions {change.operation}: {change.data}")
                except Exception as e:
                    logger.warning(f"executions stream end: {e}")

        consumers = [
            asyncio.create_task(consume_child()),
            asyncio.create_task(consume_ticker()),
            asyncio.create_task(consume_executions()),
        ]

        if mode.startswith("order"):
            try:
                await asyncio.wait_for(child_subscribed.wait(), timeout=10)
            except asyncio.TimeoutError:
                logger.warning("child_order_events subscription not confirmed within 10s; proceeding anyway")

            board = await http.get_board()
            mid = Decimal(str(board.get("mid_price")))
            bids = board.get("bids") or []
            asks = board.get("asks") or []
            best_bid = Decimal(str(bids[0]["price"])) if bids else max(Decimal(1), mid - Decimal("10"))
            best_ask = Decimal(str(asks[0]["price"])) if asks else (mid + Decimal("10"))

            if mode == "order-near":
                px = best_bid if side == "BUY" else best_ask
                logger.info(
                    f"mid={mid}, best_bid={best_bid}, best_ask={best_ask}, placing near {side} at {px}"
                )
            else:
                px = max(Decimal(1), mid - Decimal("2000000")) if side == "BUY" else (mid + Decimal("2000000"))
                logger.info(f"mid={mid}, placing far {side} at {px}")

            active = await http.get_child_orders(child_order_state="ACTIVE", count=10)
            logger.info(f"ACTIVE before: {len(active)} orders")

            await place_and_cancel(
                http,
                side=side,
                price=px,
                size=Decimal("0.001"),
                hold_sec=hold_sec,
            )

            await asyncio.sleep(wait_sec)

            active_after = await http.get_child_orders(child_order_state="ACTIVE", count=10)
            logger.info(f"ACTIVE after: {len(active_after)} orders")
        else:
            # probe mode: sample a few board snapshots for sanity
            with store.board.watch() as stream:
                for _ in range(3):
                    await stream.__anext__()
                    snap = ob_view.snapshot(datetime.now(tz=timezone.utc))
                    logger.info(
                        f"board snapshot bid={snap.best_bid_price} ask={snap.best_ask_price} "
                        f"age_ms={snap.best_age_ms} spread_ticks={snap.spread_ticks}"
                    )
            await asyncio.sleep(5)

        for c in consumers:
            c.cancel()
        await asyncio.gather(*consumers, return_exceptions=True)

    logger.info(f"WS message counts: {dict(msg_counts)}")
    logger.info(f"WS channel counts: {dict(channel_counts)}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["probe", "order-far", "order-near"],
        default="order-near",
        help="probe: listen only, order-far/near: place+cancel tiny order to trigger child_order_events",
    )
    parser.add_argument(
        "--wait-sec",
        type=int,
        default=30,
        help="Seconds to wait after place+cancel for child_order_events (default 30)",
    )
    parser.add_argument("--side", choices=["BUY", "SELL"], default="BUY")
    parser.add_argument(
        "--hold-sec",
        type=float,
        default=2.0,
        help="Seconds to keep the order ACTIVE before cancel (default 2.0)",
    )
    args = parser.parse_args()
    try:
        asyncio.run(run(args.mode, args.wait_sec, args.side, args.hold_sec))
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
