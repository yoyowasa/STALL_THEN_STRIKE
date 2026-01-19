"""
Near-production smoke test runner.

- Connects WS (board/ticker/executions/child_order_events)
- Samples board snapshots + logs stall trigger conditions (no strategy orders)
- Optionally places+cancel a far-from-market tiny order to validate private WS events

Examples:
    .\\.venv\\Scripts\\python -m src.app.run_smoke --override configs/live.yml --duration-sec 120
    .\\.venv\\Scripts\\python -m src.app.run_smoke --override configs/live.yml --enable-orders --confirm I_UNDERSTAND
    .\\.venv\\Scripts\\python -m src.app.run_smoke --override configs/live.yml --simulate-orders --duration-sec 120
    .\\.venv\\Scripts\\python -m src.app.run_smoke --override configs/live.yml --simulate-orders --simulate-fills --duration-sec 120
"""

import argparse
import asyncio
import time
import subprocess
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from secrets import token_hex
from typing import Optional, Callable
from typing import Any

from loguru import logger

from src.config.loader import AppConfig, load_app_config
from src.engine.inventory import PnLTracker
from src.infra.http_bitflyer import BitflyerHttp
from src.infra.orderbook_view import OrderBookView
from src.infra.pyb_session import PyBotterSession
from src.infra.ws_mux import WsEvent, WsMux


def _setup_logging(name: str) -> None:
    log_dir = Path("logs") / "runtime"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    logger.add(log_dir / f"{name}-{ts}.log", level="INFO", enqueue=True)

def _git_rev() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"


def _unique(seq: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in seq:
        if x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _channels_for(cfg: AppConfig) -> list[str]:
    pc = cfg.exchange.product_code
    needed = [
        f"lightning_board_snapshot_{pc}",
        f"lightning_board_{pc}",
        f"lightning_ticker_{pc}",
        f"lightning_executions_{pc}",
        "child_order_events",
    ]
    return _unique(cfg.ws.channels + needed)

def rollout_entry_gate_summary(
    *,
    consistency_ok_consecutive: Optional[int],
    cooldown_remaining_sec: Optional[float],
    mismatch_size_last: Optional[float],
    event_latency_ms_p99: Optional[float],
    queue_depth_max: Optional[int],
    ca_gate_win_ms: Optional[int],
    ca_gate_thr: Optional[float],
    ca_gate_total: Optional[int],
    ca_gate_allow: Optional[int],
    ca_gate_block: Optional[int],
    ca_gate_block_rate: Optional[float],
    end_pos: Decimal,
    active_count_end: Optional[int],
    major_alerts: int,
    cfg_rev: str,
    cfg_files: list[str],
    mode: str,
) -> None:
    def _fmt(v: Optional[object]) -> str:
        return "na" if v is None else str(v)

    block_rate = ca_gate_block_rate
    if block_rate is None and ca_gate_total is not None and ca_gate_total > 0 and ca_gate_block is not None:
        block_rate = ca_gate_block / ca_gate_total
    block_rate_str = "na" if block_rate is None else f"{block_rate:.6f}"

    logger.info(
        "rollout_entry_gate_summary "
        f"consistency_ok_consecutive={_fmt(consistency_ok_consecutive)} "
        f"cooldown_remaining_sec={_fmt(cooldown_remaining_sec)} "
        f"mismatch_size_last={_fmt(mismatch_size_last)} "
        f"event_latency_ms_p99={_fmt(event_latency_ms_p99)} "
        f"queue_depth_max={_fmt(queue_depth_max)} "
        f"ca_gate(win_ms={_fmt(ca_gate_win_ms)}, thr={_fmt(ca_gate_thr)}) "
        f"ca_gate_block_rate={block_rate_str} "
        f"ca_gate_total={_fmt(ca_gate_total)} ca_gate_allow={_fmt(ca_gate_allow)} ca_gate_block={_fmt(ca_gate_block)} "
        f"end_pos={end_pos} active_count_end={_fmt(active_count_end)} "
        f"major_alerts={major_alerts} cfg_rev={cfg_rev} cfg_files={cfg_files} mode={mode}"
    )


@dataclass
class OrderWsExpect:
    oid: str
    expect_execution: bool = False
    expect_cancel: bool = False
    seen_order: bool = False
    seen_execution: bool = False
    seen_cancel: bool = False
    deadline_mono: float = 0.0


def _iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _inject_child_order_events(
    store,
    *,
    items: list[dict],
) -> None:
    store.onmessage(
        {
            "jsonrpc": "2.0",
            "method": "channelMessage",
            "params": {"channel": "child_order_events", "message": items},
        },
        None,
    )


def _sim_inject_order(
    store,
    *,
    product_code: str,
    side: str,
    price: Decimal,
    size: Decimal,
) -> tuple[str, str]:
    oid = f"SIM-{datetime.now().strftime('%Y%m%d-%H%M%S')}-{token_hex(3)}"
    child_order_id = f"SIMCO-{token_hex(6)}"
    now = datetime.now(tz=timezone.utc)
    expire = now + timedelta(days=365)
    _inject_child_order_events(
        store,
        items=[
            {
                "product_code": product_code,
                "child_order_id": child_order_id,
                "child_order_acceptance_id": oid,
                "event_date": _iso_z(now),
                "event_type": "ORDER",
                "child_order_type": "LIMIT",
                "side": side,
                "price": int(price),
                "size": float(size),
                "expire_date": _iso_z(expire),
            }
        ],
    )
    logger.info(f"SIM ORDER injected {side} @{price} size={size} -> {oid}")
    return oid, child_order_id


async def _sim_cancel_later(
    store,
    *,
    product_code: str,
    oid: str,
    child_order_id: str,
    price: Decimal,
    size: Decimal,
    hold_sec: float,
) -> None:
    await asyncio.sleep(hold_sec)
    _inject_child_order_events(
        store,
        items=[
            {
                "product_code": product_code,
                "child_order_id": child_order_id,
                "child_order_acceptance_id": oid,
                "event_date": _iso_z(datetime.now(tz=timezone.utc)),
                "event_type": "CANCEL",
                "price": int(price),
                "size": float(size),
            }
        ],
    )
    logger.info(f"SIM CANCEL injected {oid}")


async def _sim_exec_later(
    store,
    *,
    product_code: str,
    oid: str,
    child_order_id: str,
    side: str,
    exec_price: Decimal,
    exec_size: Decimal,
    outstanding_size: Decimal,
    delay_sec: float,
) -> None:
    await asyncio.sleep(delay_sec)
    _inject_child_order_events(
        store,
        items=[
            {
                "product_code": product_code,
                "child_order_id": child_order_id,
                "child_order_acceptance_id": oid,
                "event_date": _iso_z(datetime.now(tz=timezone.utc)),
                "event_type": "EXECUTION",
                "side": side,
                "price": int(exec_price),
                "size": float(exec_size),
                "outstanding_size": float(outstanding_size),
                "commission": 0.0,
                "sfd": 0.0,
            }
        ],
    )
    logger.info(f"SIM EXECUTION injected {side} @{exec_price} size={exec_size} oid={oid}")


async def _place_far_order(
    http: BitflyerHttp,
    *,
    side: str,
    size: Decimal,
    distance_jpy: Decimal,
) -> str:
    board = await http.get_board()
    mid = Decimal(str(board.get("mid_price")))
    if side == "BUY":
        px = max(Decimal(1), mid - distance_jpy)
    else:
        px = mid + distance_jpy
    res = await http.send_limit_order(side=side, price=int(px), size=float(size), tag="smoke")
    oid = res.get("child_order_acceptance_id")
    if not oid:
        raise RuntimeError(f"send_limit_order response missing id: {res}")
    logger.info(f"ORDER placed {side} @{px} size={size} -> {oid}")
    return oid


async def _cancel_later(
    http: BitflyerHttp,
    oid: str,
    hold_sec: float,
    *,
    poll_sec: float,
    poll_interval_sec: float,
    log_warn: Optional[Callable[[str], None]] = None,
) -> None:
    await asyncio.sleep(hold_sec)
    res = await http.cancel_order(oid)
    logger.info(f"ORDER cancel requested {oid}: {res}")
    if poll_sec <= 0:
        return
    deadline = time.monotonic() + poll_sec
    last_count = None
    last_hit = None
    def _warn(msg: str) -> None:
        if log_warn:
            log_warn(msg)
        else:
            logger.warning(msg)
    while True:
        try:
            orders = await http.get_child_orders(child_order_state="ACTIVE", count=50)
        except Exception as e:
            _warn(f"ACTIVE poll error oid={oid}: {e}")
            orders = None
        if isinstance(orders, list):
            last_count = len(orders)
            last_hit = len([o for o in orders if o.get("child_order_acceptance_id") == oid])
            logger.info(f"ACTIVE poll count={last_count} hit={last_hit} oid={oid}")
            if last_hit == 0:
                break
        else:
            logger.info(f"ACTIVE poll response oid={oid}: {orders}")
        if time.monotonic() >= deadline:
            break
        await asyncio.sleep(poll_interval_sec)
    if last_count is not None and last_hit is not None:
        logger.info(f"ACTIVE poll final count={last_count} hit={last_hit} oid={oid}")


async def main_async(args: argparse.Namespace) -> None:
    cfg = load_app_config("configs/base.yml", args.override)
    _setup_logging("smoke")

    major_alerts = 0
    def warn(msg: str) -> None:
        nonlocal major_alerts
        major_alerts += 1
        logger.warning(msg)

    cfg_rev = args.cfg_rev or _git_rev()
    cfg_files = ["configs/base.yml"]
    if args.override:
        cfg_files.append(args.override)

    channels = _channels_for(cfg)
    logger.info(f"cfg.env={cfg.env} product_code={cfg.exchange.product_code}")
    logger.info(f"subscribing channels={channels}")

    sub_acks: dict[str, bool] = {}
    all_subscribed = asyncio.Event()
    child_subscribed = asyncio.Event()

    def on_info(msg: dict) -> None:
        logger.info(f"WS info {msg}")
        sub_id = msg.get("id")
        if isinstance(sub_id, str) and sub_id.startswith("sub:"):
            sub_acks[sub_id] = bool(msg.get("result") is True)
            if sub_id == "sub:child_order_events" and msg.get("result") is True:
                child_subscribed.set()
            if len(sub_acks) >= len(channels) and all(sub_acks.values()):
                all_subscribed.set()

    ws_counts: Counter[str] = Counter()
    ws_channels: Counter[str] = Counter()

    def on_message(msg: dict) -> None:
        method = msg.get("method") or "no_method"
        ws_counts[method] += 1
        params = msg.get("params")
        if isinstance(params, dict):
            ch = params.get("channel")
            if ch:
                ws_channels[ch] += 1

    if args.enable_orders and args.simulate_orders:
        raise SystemExit("Choose one: --enable-orders or --simulate-orders")

    if args.simulate_fills and not args.simulate_orders:
        raise SystemExit("--simulate-fills requires --simulate-orders")

    if args.enable_orders and args.confirm != "I_UNDERSTAND":
        raise SystemExit("Refusing to place real orders. Pass `--confirm I_UNDERSTAND`.")

    async with PyBotterSession(cfg) as sess:
        await sess.connect_ws(channels, on_info=on_info, on_message=on_message)
        store = sess.get_store()
        http = BitflyerHttp(sess.get_client(), cfg.exchange)

        if args.check_balance:
            bal = await http.get_balance()
            if isinstance(bal, list):
                codes = [row.get("currency_code") for row in bal]
                logger.info(f"getbalance OK entries={len(bal)} currencies={codes} (amounts hidden)")
            else:
                logger.info(f"getbalance OK (non-list): {bal}")

        ob_view = OrderBookView(store, cfg.exchange.product_code, tick_size=Decimal("1"))
        pnl = PnLTracker()
        async with WsMux(store, product_code=cfg.exchange.product_code) as mux:
            end_mono = time.monotonic() + args.duration_sec
            counts: Counter[str] = Counter()
            last_summary = time.monotonic()
            last_collateral_poll = 0.0

            last_eval = 0.0
            eval_interval = args.eval_interval_ms / 1000.0
            last_trigger = 0.0
            trigger_cooldown = 1.0
            stall_ready = False
            last_mark: Optional[Decimal] = None

            order_expect: Optional[OrderWsExpect] = None
            cancel_task: Optional[asyncio.Task] = None
            exec_task: Optional[asyncio.Task] = None
            next_order_mono = time.monotonic() + args.order_interval_sec
            sim_side = args.order_side

            try:
                await asyncio.wait_for(all_subscribed.wait(), timeout=10)
            except asyncio.TimeoutError:
                warn("Not all subscriptions confirmed within 10s; continuing anyway")

            while time.monotonic() < end_mono:
                now_mono = time.monotonic()

                if args.poll_collateral_sec > 0 and (now_mono - last_collateral_poll) >= args.poll_collateral_sec:
                    last_collateral_poll = now_mono
                    col = await http.get_collateral()
                    logger.info(
                        f"REAL collateral={col.get('collateral')} open_position_pnl={col.get('open_position_pnl')} "
                        f"require={col.get('require_collateral')} keep_rate={col.get('keep_rate')}"
                    )

                if (
                    (args.enable_orders or args.simulate_orders)
                    and stall_ready
                    and order_expect is None
                    and now_mono >= next_order_mono
                ):
                    try:
                        await asyncio.wait_for(child_subscribed.wait(), timeout=10)
                    except asyncio.TimeoutError:
                        warn("child_order_events subscription not confirmed within 10s; proceeding anyway")

                    if args.enable_orders:
                        active_before = await http.get_child_orders(child_order_state="ACTIVE", count=10)
                        logger.info(
                            f"ACTIVE before: {len(active_before) if isinstance(active_before, list) else active_before}"
                        )
                        oid = await _place_far_order(
                            http,
                            side=args.order_side,
                            size=Decimal(str(args.order_size_btc)),
                            distance_jpy=Decimal(str(args.order_distance_jpy)),
                        )
                        cancel_task = asyncio.create_task(
                            _cancel_later(
                                http,
                                oid,
                                args.order_hold_sec,
                                poll_sec=args.post_cancel_poll_sec,
                                poll_interval_sec=args.post_cancel_poll_interval_sec,
                                log_warn=warn,
                            )
                        )
                        expect_execution = False
                        expect_cancel = True
                    else:
                        snap = ob_view.snapshot(datetime.now(tz=timezone.utc))
                        if snap.best_bid_price is None or snap.best_ask_price is None:
                            warn("SIM order skipped: best bid/ask not ready")
                            next_order_mono = time.monotonic() + 1
                            continue
                        mid = (snap.best_bid_price + snap.best_ask_price) / 2
                        px = (
                            max(Decimal(1), mid - Decimal(str(args.order_distance_jpy)))
                            if sim_side == "BUY"
                            else (mid + Decimal(str(args.order_distance_jpy)))
                        )
                        order_size = Decimal(str(args.order_size_btc))
                        fill_size = order_size * Decimal(str(args.sim_fill_ratio)) if args.simulate_fills else Decimal("0")
                        fill_size = min(order_size, max(Decimal("0"), fill_size))
                        remaining = order_size - fill_size
                        exec_px = snap.best_ask_price if sim_side == "BUY" else snap.best_bid_price
                        oid, child_order_id = _sim_inject_order(
                            store,
                            product_code=cfg.exchange.product_code,
                            side=sim_side,
                            price=px,
                            size=order_size,
                        )
                        if args.simulate_fills and fill_size > 0:
                            exec_task = asyncio.create_task(
                                _sim_exec_later(
                                    store,
                                    product_code=cfg.exchange.product_code,
                                    oid=oid,
                                    child_order_id=child_order_id,
                                    side=sim_side,
                                    exec_price=exec_px,
                                    exec_size=fill_size,
                                    outstanding_size=remaining,
                                    delay_sec=args.sim_exec_delay_sec,
                                )
                            )
                        if remaining > 0:
                            cancel_task = asyncio.create_task(
                                _sim_cancel_later(
                                    store,
                                    product_code=cfg.exchange.product_code,
                                    oid=oid,
                                    child_order_id=child_order_id,
                                    price=px,
                                    size=remaining,
                                    hold_sec=args.order_hold_sec,
                                )
                            )
                        expect_execution = args.simulate_fills and fill_size > 0
                        expect_cancel = remaining > 0
                        if args.sim_side_mode == "alternate":
                            sim_side = "SELL" if sim_side == "BUY" else "BUY"

                    order_expect = OrderWsExpect(
                        oid=oid,
                        expect_execution=expect_execution,
                        expect_cancel=expect_cancel,
                        deadline_mono=time.monotonic() + args.order_ws_timeout_sec,
                    )
                    next_order_mono = time.monotonic() + args.order_interval_sec

                try:
                    evt: WsEvent = await mux.get(timeout=1)
                except asyncio.TimeoutError:
                    evt = None  # type: ignore[assignment]

                if evt is not None:
                    counts[evt.kind] += 1

                    if evt.kind == "board":
                        if now_mono - last_eval >= eval_interval:
                            last_eval = now_mono
                            snap = ob_view.snapshot(datetime.now(tz=timezone.utc))
                            spread_ok = (
                                snap.spread_ticks is not None
                                and snap.spread_ticks >= cfg.strategy.min_spread_tick
                            )
                            age_ok = snap.best_age_ms >= cfg.strategy.stall_T_ms
                            stall_ready = age_ok and spread_ok
                            if snap.best_bid_price is not None and snap.best_ask_price is not None:
                                last_mark = (snap.best_bid_price + snap.best_ask_price) / 2
                                pnl.update_mark(last_mark)
                            if age_ok and spread_ok and (now_mono - last_trigger) >= trigger_cooldown:
                                last_trigger = now_mono
                                logger.info(
                                    "STALL trigger "
                                    f"age_ms={snap.best_age_ms} spread_ticks={snap.spread_ticks} "
                                    f"bid={snap.best_bid_price} ask={snap.best_ask_price}"
                                )

                    elif evt.kind == "child_order_events":
                        if order_expect and evt.data.get("child_order_acceptance_id") == order_expect.oid:
                            et = evt.data.get("event_type")
                            logger.info(f"child_order_events {evt.operation}: {evt.data}")
                            if et == "ORDER":
                                order_expect.seen_order = True
                            elif et == "EXECUTION":
                                order_expect.seen_execution = True
                                try:
                                    pnl.apply_execution(
                                        side=str(evt.data.get("side")),
                                        price=Decimal(str(evt.data.get("price"))),
                                        size=Decimal(str(evt.data.get("size"))),
                                    )
                                except Exception as e:
                                    warn(f"PnL apply_execution failed: {e} evt={evt.data}")
                            elif et == "CANCEL":
                                order_expect.seen_cancel = True

                if order_expect and now_mono >= order_expect.deadline_mono:
                    warn(
                        f"child_order_events timeout for {order_expect.oid}: "
                        f"ORDER={order_expect.seen_order} EXEC={order_expect.seen_execution} CANCEL={order_expect.seen_cancel}"
                    )
                    order_expect = None

                if order_expect and order_expect.seen_order:
                    if order_expect.expect_execution and not order_expect.seen_execution:
                        pass
                    elif order_expect.expect_cancel and not order_expect.seen_cancel:
                        pass
                    else:
                        if args.enable_orders:
                            active_after = await http.get_child_orders(child_order_state="ACTIVE", count=10)
                            logger.info(
                                f"ACTIVE after: {len(active_after) if isinstance(active_after, list) else active_after}"
                            )
                        st = pnl.state
                        logger.info(
                            f"child_order_events OK for {order_expect.oid} "
                            f"SIM_PNL pos={st.position_btc} avg={st.avg_price} mark={st.mark_price} "
                            f"realized={st.realized_pnl_jpy} unrealized={st.unrealized_pnl_jpy} total={st.total_pnl_jpy}"
                        )
                        order_expect = None

                if now_mono - last_summary >= args.summary_interval_sec:
                    last_summary = now_mono
                    st = pnl.state
                    logger.info(
                        f"summary events={dict(counts)} ws_methods={dict(ws_counts)} ws_channels={dict(ws_channels)} "
                        f"SIM_PNL pos={st.position_btc} avg={st.avg_price} mark={st.mark_price} "
                        f"realized={st.realized_pnl_jpy} unrealized={st.unrealized_pnl_jpy} total={st.total_pnl_jpy}"
                    )
                    env = dict(locals())
                    env["cfg"] = cfg.strategy
                    active_count: Any = None
                    try:
                        active_now = await http.get_child_orders(child_order_state="ACTIVE", count=50)
                        if isinstance(active_now, list):
                            active_count = len(active_now)
                        else:
                            logger.info(f"ACTIVE summary response: {active_now}")
                    except Exception as e:
                        warn(f"ACTIVE summary error: {e}")

                    active_after = env.get("active_after")
                    if isinstance(active_after, list):
                        env["active_after"] = len(active_after)
                    else:
                        env["active_after"] = active_after
                    if active_count is None:
                        active_count = env.get("active_after")
                    env["active_count"] = active_count

                    error_count = major_alerts
                    env["error_count"] = error_count
                    env["errors"] = major_alerts
                    _emit_entry_guard(logger, env)

            if exec_task:
                await asyncio.gather(exec_task, return_exceptions=True)
            if cancel_task:
                await asyncio.gather(cancel_task, return_exceptions=True)
            end_pos = pnl.state.position_btc
            active_count_end = None
            try:
                orders = await http.get_child_orders(child_order_state="ACTIVE", count=50)
                if isinstance(orders, list):
                    active_count_end = len(orders)
                    logger.info(f"ACTIVE final count={active_count_end}")
                else:
                    logger.info(f"ACTIVE final response: {orders}")
            except Exception as e:
                warn(f"ACTIVE final error: {e}")
            rollout_entry_gate_summary(
                consistency_ok_consecutive=None,
                cooldown_remaining_sec=None,
                mismatch_size_last=None,
                event_latency_ms_p99=None,
                queue_depth_max=None,
                ca_gate_win_ms=cfg.strategy.ca_ratio_win_ms,
                ca_gate_thr=cfg.strategy.ca_threshold,
                ca_gate_total=None,
                ca_gate_allow=None,
                ca_gate_block=None,
                ca_gate_block_rate=None,
                end_pos=end_pos,
                active_count_end=active_count_end,
                major_alerts=major_alerts,
                cfg_rev=cfg_rev,
                cfg_files=cfg_files,
                mode=args.mode_label,
            )


def _safe_get(obj: Any, *path: str, default: Any = None) -> Any:
    # 何をする: dictキー / オブジェクト属性 を混在していても、path順に安全に辿って値を返す（途中で欠けたらdefault）
    cur = obj

    for key in path:
        # 何をする: 途中で None になったらそれ以上辿れないので default で打ち切る
        if cur is None:
            return default

        # 何をする: dict の場合はキーで辿る
        if isinstance(cur, dict):
            if key in cur:
                cur = cur[key]
                continue
            return default

        # 何をする: dict以外は属性として辿る
        if hasattr(cur, key):
            cur = getattr(cur, key)
            continue

        return default

    # 何をする: 最終結果が None のときだけ default にする（0 や False はそのまま返す）
    return default if cur is None else cur


def _safe_call(maybe_fn: Any, /, *args: Any, **kwargs: Any) -> Any:
    # callable なら実行し、例外が出ても None で握りつぶして監視ログ出力を止めない
    if not callable(maybe_fn):
        return None
    try:
        return maybe_fn(*args, **kwargs)
    except Exception:
        return None


def _first_not_none(*values: Any, default: Any = None) -> Any:
    # 複数候補から「最初に見つかった None ではない値」を採用する（全部 None なら default）
    for v in values:
        if v is not None:
            return v
    return default


def _safe_obj_keys(obj: Any, *, limit: int = 30) -> str:  # デバッグ用に「中身のキー/属性」を安全に見える化する
    if obj is None:  # Noneならそのまま返す
        return "None"  # Noneを明示する
    try:  # 例外が出ても落とさない
        if isinstance(obj, dict):  # dictならkeys()を見る
            keys = sorted(str(k) for k in obj.keys())  # キー名を昇順にする
        else:  # 通常オブジェクトなら __dict__ を見る
            try:
                keys = sorted(vars(obj).keys())  # 属性名を昇順にする
            except Exception:
                keys = sorted(k for k in dir(obj) if not k.startswith("_"))
        if len(keys) > limit:  # 長すぎるとログが壊れるので制限する
            keys = keys[:limit] + ["..."]  # 省略記号を付ける
        return f"{type(obj).__name__} keys={keys}"  # 型名とキー一覧を返す
    except Exception:  # どんな例外でも握りつぶす（デバッグで落ちるのが一番危険）
        return type(obj).__name__  # 最低限、型名だけ返す


def _emit_entry_guard(logger, env: dict[str, Any]) -> None:
    # 入口条件（整合/クールダウン/遅延/queue/ca_gate/後始末）を1行ログで証明できる形にまとめて出す
    cfg = env.get("cfg") or env.get("config") or env.get("settings")

    # ENTRY_GUARD: args由来のスタブ(dict)を避け、実体のengine/runnerを優先して拾う
    engine = None
    for cand in (env.get("pos_engine"), env.get("app"), env.get("runner"), env.get("engine")):
        if cand is None:
            continue
        if isinstance(cand, dict) and set(cand.keys()) <= {"health", "metrics"}:
            health = cand.get("health") or {}
            metrics = cand.get("metrics") or {}
            # すべてNoneなら「スタブ」とみなしてスキップ（実値が入っている場合は採用）
            if all(v is None for v in health.values()) and all(v is None for v in metrics.values()):
                continue
        engine = cand
        break

    # ENTRY_GUARD: {"cfg": StallStrategyConfig} みたいなスタブdictを避け、実体strategyを優先して拾う
    strategy = None
    for cand in (
        env.get("stall_strategy"),
        env.get("strategy"),
        _safe_get(engine, "strategy", default=None),
        _safe_get(engine, "stall_strategy", default=None),
    ):
        if cand is None:
            continue
        if isinstance(cand, dict) and set(cand.keys()) <= {"cfg"}:
            continue
        strategy = cand
        break

    engine_health = _first_not_none(
        # engine が health をメソッド提供している場合に拾う（例: get_health/health_snapshot）
        _safe_call(_safe_get(engine, "get_health", default=None)),
        _safe_call(_safe_get(engine, "health_snapshot", default=None)),
        _safe_get(engine, "health", default=None),
        default=None,
    )
    engine_metrics = _first_not_none(
        # engine が metrics をメソッド提供している場合に拾う（例: get_metrics/metrics_snapshot）
        _safe_call(_safe_get(engine, "get_metrics", default=None)),
        _safe_call(_safe_get(engine, "metrics_snapshot", default=None)),
        _safe_get(engine, "metrics", default=None),
        default=None,
    )

    ca_win_ms = _first_not_none(
        _safe_get(cfg, "ca_ratio_win_ms", default=None),
        _safe_get(strategy, "cfg", "ca_ratio_win_ms", default=None),
        default=None,
    )
    ca_thr = _first_not_none(
        _safe_get(cfg, "ca_threshold", default=None),
        _safe_get(strategy, "cfg", "ca_threshold", default=None),
        default=None,
    )

    consistency_ok_consecutive = _first_not_none(
        _safe_get(engine, "health", "consistency_ok_consecutive", default=None),
        _safe_get(engine, "consistency_ok_consecutive", default=None),
        default=None,
    )
    cooldown_remaining_sec = _first_not_none(
        _safe_get(engine, "health", "cooldown_remaining_sec", default=None),
        _safe_get(engine, "cooldown_remaining_sec", default=None),
        default=None,
    )
    event_latency_ms_p99 = _first_not_none(
        _safe_get(engine, "metrics", "event_latency_ms_p99", default=None),
        _safe_get(engine, "event_latency_ms_p99", default=None),
        default=None,
    )
    queue_depth_max = _first_not_none(
        _safe_get(engine, "metrics", "queue_depth_max", default=None),
        _safe_get(engine, "queue_depth_max", default=None),
        default=None,
    )
    ca_block_rate = _first_not_none(
        _safe_get(engine, "metrics", "ca_gate_block_rate", default=None),
        _safe_get(strategy, "ca_gate", "block_rate", default=None),
        default=None,
    )
    active_count = _first_not_none(
        env.get("active_count"),
        env.get("active_after"),
        _safe_get(engine, "active_count", default=None),
        default=None,
    )
    errors = _first_not_none(
        env.get("error_count"),
        env.get("errors"),
        default=0,
    )

    if (
        consistency_ok_consecutive is None
        or cooldown_remaining_sec is None
        or event_latency_ms_p99 is None
        or queue_depth_max is None
        or ca_block_rate is None
    ):
        logger.info(
            "ENTRY_GUARD_SRC engine={} engine_health={} engine_metrics={} strategy={} strategy_cfg={} strategy_ca_gate={}",
            _safe_obj_keys(engine),
            _safe_obj_keys(engine_health),
            _safe_obj_keys(engine_metrics),
            _safe_obj_keys(strategy),
            _safe_obj_keys(_safe_get(strategy, "cfg", default=None)),
            _safe_obj_keys(_safe_get(strategy, "ca_gate", default=None)),
        )

    logger.info(
        "ENTRY_GUARD consistency_ok_consecutive={} cooldown_remaining_sec={} event_latency_ms_p99={} queue_depth_max={} ca_gate_win_ms={} ca_gate_thr={} ca_gate_block_rate={} active_count={} errors={}",
        consistency_ok_consecutive,
        cooldown_remaining_sec,
        event_latency_ms_p99,
        queue_depth_max,
        ca_win_ms,
        ca_thr,
        ca_block_rate,
        active_count,
        errors,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--override", default="configs/live.yml")
    parser.add_argument("--duration-sec", type=int, default=120)
    parser.add_argument("--summary-interval-sec", type=int, default=10)
    parser.add_argument("--eval-interval-ms", type=int, default=50)
    parser.add_argument("--poll-collateral-sec", type=int, default=30)

    parser.add_argument("--check-balance", action="store_true")

    parser.add_argument("--enable-orders", action="store_true")
    parser.add_argument(
        "--simulate-orders",
        action="store_true",
        help="Inject pseudo child_order_events into the local DataStore (no real orders)",
    )
    parser.add_argument("--simulate-fills", action="store_true", help="Also inject EXECUTION for simulated orders")
    parser.add_argument("--sim-fill-ratio", type=float, default=0.5, help="Fill ratio (0..1) for simulated orders")
    parser.add_argument("--sim-exec-delay-sec", type=float, default=0.2)
    parser.add_argument(
        "--sim-side-mode",
        choices=["fixed", "alternate"],
        default="fixed",
        help="fixed: always use --order-side, alternate: flip BUY/SELL each simulated order",
    )
    parser.add_argument("--confirm", default="")
    parser.add_argument("--order-side", choices=["BUY", "SELL"], default="BUY")
    parser.add_argument("--order-size-btc", type=float, default=0.001)
    parser.add_argument("--order-distance-jpy", type=int, default=2_000_000)
    parser.add_argument("--order-hold-sec", type=float, default=2.0)
    parser.add_argument("--order-interval-sec", type=int, default=60)
    parser.add_argument("--order-ws-timeout-sec", type=int, default=20)
    parser.add_argument("--post-cancel-poll-sec", type=float, default=10.0)
    parser.add_argument("--post-cancel-poll-interval-sec", type=float, default=1.0)
    parser.add_argument("--mode-label", default="canary")
    parser.add_argument("--cfg-rev", default="")

    args = parser.parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
