import argparse
import asyncio
from collections import deque
import json
import os
import socket
import time
import urllib.request
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
    level = os.getenv("LOG_LEVEL", "INFO")
    rotation = os.getenv("LIVE_LOG_ROTATION", "200 MB")
    retention = os.getenv("LIVE_LOG_RETENTION", "14 days")
    compression = os.getenv("LIVE_LOG_COMPRESSION", "zip")
    logger.add(
        log_dir / f"live-{ts}.log",
        level=level,
        enqueue=True,
        rotation=rotation,
        retention=retention,
        compression=compression,
    )


def _live_channels(product_code: str, base_channels: list[str]) -> list[str]:
    return list(
        dict.fromkeys(base_channels + _default_channels(product_code) + ["child_order_events"])
    )


def _env_bool(name: str, default: bool) -> bool:
    val = os.getenv(name)
    if val is None:
        return default
    norm = val.strip().lower()
    if norm in {"1", "true", "yes", "on"}:
        return True
    if norm in {"0", "false", "no", "off"}:
        return False
    return default


def _env_int(name: str, default: int) -> int:
    val = os.getenv(name)
    if val is None:
        return default
    try:
        return int(val)
    except Exception:
        return default


def _trim_recent_errors(
    samples: deque[float],
    *,
    now_mono: float,
    window_sec: float,
) -> int:
    if window_sec <= 0:
        return len(samples)
    cutoff = now_mono - window_sec
    while samples and samples[0] < cutoff:
        samples.popleft()
    return len(samples)


def _filter_actions_by_interval(
    actions: list[Action],
    *,
    now_mono: float,
    min_interval_by_kind: dict[str, float],
    last_sent_mono_by_kind: dict[str, float],
) -> tuple[list[Action], dict[str, int]]:
    if not actions:
        return actions, {}

    allowed_kind: dict[str, bool] = {}
    blocked: dict[str, int] = {}
    out: list[Action] = []
    for action in actions:
        kind = action.kind
        allowed = allowed_kind.get(kind)
        if allowed is None:
            min_interval = max(0.0, float(min_interval_by_kind.get(kind, 0.0)))
            if min_interval <= 0:
                allowed = True
            else:
                last = last_sent_mono_by_kind.get(kind, -1e18)
                allowed = (now_mono - last) >= min_interval
                if allowed:
                    last_sent_mono_by_kind[kind] = now_mono
            allowed_kind[kind] = allowed

        if allowed:
            out.append(action)
        else:
            blocked[kind] = blocked.get(kind, 0) + 1
    return out, blocked


def _is_self_trade_error(exc: BaseException) -> bool:
    cur: BaseException | None = exc
    visited: set[int] = set()
    while cur is not None and id(cur) not in visited:
        visited.add(id(cur))
        msg = str(cur).lower()
        if "self trade" in msg:
            return True
        if "-159" in msg and "status" in msg:
            return True
        cur = cur.__cause__ or cur.__context__
    return False


def _is_api_limit_error(exc: BaseException) -> bool:
    cur: BaseException | None = exc
    visited: set[int] = set()
    while cur is not None and id(cur) not in visited:
        visited.add(id(cur))
        msg = str(cur).lower()
        if "over api limit" in msg:
            return True
        if "-1" in msg and "status" in msg and "api limit" in msg:
            return True
        cur = cur.__cause__ or cur.__context__
    return False


def _is_api_limit_response(payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    message = str(payload.get("error_message") or "").lower()
    if "api limit" in message:
        return True
    status = payload.get("status")
    return status == -1 and "over" in message and "limit" in message


def _is_min_order_size_error(exc: BaseException) -> bool:
    cur: BaseException | None = exc
    visited: set[int] = set()
    while cur is not None and id(cur) not in visited:
        visited.add(id(cur))
        msg = str(cur).lower()
        if "minimum order size" in msg:
            return True
        if "-110" in msg and "status" in msg:
            return True
        cur = cur.__cause__ or cur.__context__
    return False


def _is_irregular_order_error(exc: BaseException) -> bool:
    cur: BaseException | None = exc
    visited: set[int] = set()
    while cur is not None and id(cur) not in visited:
        visited.add(id(cur))
        msg = str(cur).lower()
        if "irregular number of orders" in msg:
            return True
        if "-509" in msg and "status" in msg:
            return True
        cur = cur.__cause__ or cur.__context__
    return False


def _dust_normalize_plan(
    *,
    side: str,
    size_btc: Decimal,
    min_order_size_btc: Decimal,
) -> tuple[tuple[Side, Decimal], tuple[Side, Decimal]] | None:
    if min_order_size_btc <= 0:
        return None
    if size_btc <= 0 or size_btc >= min_order_size_btc:
        return None
    if side == "long":
        return ("SELL", size_btc + min_order_size_btc), ("BUY", min_order_size_btc)
    if side == "short":
        return ("BUY", size_btc + min_order_size_btc), ("SELL", min_order_size_btc)
    return None


def _resolve_alert_webhook_url() -> str:
    for key in (
        "LIVE_ALERT_WEBHOOK_URL",
        "ALERT_WEBHOOK_URL",
        "DISCORD_WEBHOOK_URL",
        "SLACK_WEBHOOK_URL",
        "WEBHOOK_URL",
    ):
        v = os.getenv(key)
        if v and v.strip():
            return v.strip()
    return ""


def _build_alert_text(
    *,
    event: str,
    level: str,
    detail: str = "",
    cfg_path: str | None = None,
    product_code: str | None = None,
) -> str:
    lines = [
        f"[stall_then_strike/live][{level}] {event}",
        f"time_utc={datetime.now(tz=timezone.utc).isoformat()}",
        f"host={socket.gethostname()} pid={os.getpid()}",
    ]
    if cfg_path:
        lines.append(f"override={cfg_path}")
    if product_code:
        lines.append(f"product_code={product_code}")
    if detail:
        lines.append(detail)
    return "\n".join(lines)


def _post_webhook_json(url: str, payload: dict[str, Any], *, timeout_sec: float) -> None:
    user_agent = os.getenv(
        "LIVE_ALERT_USER_AGENT",
        "stall-then-strike/1.0 (+https://github.com/yoyowasa/STALL_THEN_STRIKE)",
    )
    req = urllib.request.Request(
        url=url,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "User-Agent": user_agent,
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        code = getattr(resp, "status", None)
        if code is None:
            code = resp.getcode()
        if code >= 400:
            raise RuntimeError(f"webhook status={code}")


def _send_webhook_alert_best_effort(
    *,
    event: str,
    level: str = "ERROR",
    detail: str = "",
    cfg_path: str | None = None,
    product_code: str | None = None,
) -> None:
    if not _env_bool("LIVE_ALERT_ENABLED", True):
        return
    url = _resolve_alert_webhook_url()
    if not url:
        return

    timeout_sec = _env_float("LIVE_ALERT_TIMEOUT_SEC") or 5.0
    timeout_sec = max(1.0, float(timeout_sec))
    text = _build_alert_text(
        event=event,
        level=level,
        detail=detail,
        cfg_path=cfg_path,
        product_code=product_code,
    )
    url_lower = url.lower()
    is_discord = "discord.com/api/webhooks/" in url_lower or "discordapp.com/api/webhooks/" in url_lower
    is_slack = "hooks.slack.com/" in url_lower
    if is_discord:
        payload: dict[str, Any] = {"content": text}
    elif is_slack:
        payload = {"text": text}
    else:
        payload = {"content": text, "text": text}
    username = os.getenv("LIVE_ALERT_USERNAME") or os.getenv("ALERT_WEBHOOK_USERNAME")
    if username and not is_slack:
        payload["username"] = username

    try:
        _post_webhook_json(url, payload, timeout_sec=timeout_sec)
        logger.info("WEBHOOK_ALERT_SENT event={} level={}", event, level)
    except Exception as exc:
        logger.warning(f"WEBHOOK_ALERT_FAILED event={event} err={exc}")


async def _send_webhook_alert(
    *,
    event: str,
    level: str = "ERROR",
    detail: str = "",
    cfg_path: str | None = None,
    product_code: str | None = None,
) -> None:
    await asyncio.to_thread(
        _send_webhook_alert_best_effort,
        event=event,
        level=level,
        detail=detail,
        cfg_path=cfg_path,
        product_code=product_code,
    )


def _positions_to_net(positions: Any) -> tuple[Decimal, Optional[Decimal], int]:
    if not isinstance(positions, list):
        return Decimal("0"), None, 0

    net = Decimal("0")
    signed_notional = Decimal("0")
    rows = 0
    for row in positions:
        if not isinstance(row, dict):
            continue
        side = str(row.get("side") or "")
        if side not in {"BUY", "SELL"}:
            continue
        size = Decimal(str(row.get("size") or "0"))
        price = Decimal(str(row.get("price") or "0"))
        if size <= 0 or price <= 0:
            continue
        sign = Decimal("1") if side == "BUY" else Decimal("-1")
        net += sign * size
        signed_notional += sign * size * price
        rows += 1

    if net == 0:
        return Decimal("0"), None, rows
    avg_price = abs(signed_notional / net)
    return net, avg_price, rows


def _positions_response_to_net(
    positions: Any,
) -> tuple[Optional[Decimal], Optional[Decimal], int]:
    if not isinstance(positions, list):
        return None, None, 0
    net, avg_price, rows = _positions_to_net(positions)
    return net, avg_price, rows


async def _fetch_exchange_position(
    *,
    http: BitflyerHttp,
    product_code: str,
) -> tuple[Decimal, Optional[Decimal], int]:
    try:
        positions = await http.get_positions(product_code=product_code)
    except Exception as exc:
        logger.warning(f"get_positions failed: {exc}")
        return Decimal("0"), None, 0
    net, avg_price, rows = _positions_to_net(positions)
    if not isinstance(positions, list):
        logger.warning(f"get_positions unexpected response: {positions}")
    return net, avg_price, rows


async def _fetch_exchange_position_known(
    *,
    http: BitflyerHttp,
    product_code: str,
) -> tuple[Optional[Decimal], Optional[Decimal], int]:
    try:
        positions = await http.get_positions(product_code=product_code)
    except Exception as exc:
        logger.warning(f"get_positions failed: {exc}")
        return None, None, 0
    net, avg_price, rows = _positions_response_to_net(positions)
    if net is None:
        logger.warning(f"get_positions unexpected response: {positions}")
        return None, None, 0
    return net, avg_price, rows


async def _fetch_exchange_position_retry(
    *,
    http: BitflyerHttp,
    product_code: str,
    retries: int,
    wait_sec: float,
    context: str,
) -> tuple[Optional[Decimal], Optional[Decimal], int]:
    retries = max(1, retries)
    wait_sec = max(0.2, wait_sec)
    last_rows = 0
    for attempt in range(1, retries + 1):
        net, avg_price, rows = await _fetch_exchange_position_known(
            http=http,
            product_code=product_code,
        )
        last_rows = rows
        if net is not None:
            if attempt > 1:
                logger.info(
                    "get_positions recovered context={} attempt={} retries={}",
                    context,
                    attempt,
                    retries,
                )
            return net, avg_price, rows
        if attempt < retries:
            logger.warning(
                "get_positions retry context={} attempt={} retries={} wait_sec={}",
                context,
                attempt,
                retries,
                wait_sec,
            )
            await asyncio.sleep(wait_sec)
    return None, None, last_rows


async def _sync_inventory_from_exchange(
    *,
    http: BitflyerHttp,
    product_code: str,
    inventory: InventoryManager,
    strategy: StallThenStrikeStrategy,
) -> tuple[Decimal, Optional[Decimal], int]:
    net, avg_price, rows = await _fetch_exchange_position(
        http=http,
        product_code=product_code,
    )
    if net == 0:
        return Decimal("0"), None, rows

    if avg_price is None or avg_price <= 0:
        avg_price = Decimal("0")
    fill_side: Side = "BUY" if net > 0 else "SELL"
    fill = FillEvent(
        ts=datetime.now(tz=timezone.utc),
        order_id="bootstrap-position",
        side=fill_side,
        price=avg_price,
        size=abs(net),
        tag="bootstrap",
    )
    inventory.apply_fill(fill)
    strategy.on_fill(fill)
    return net, avg_price, rows


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

        end = (
            None
            if duration_sec <= 0
            else datetime.now(tz=timezone.utc).timestamp() + duration_sec
        )
        last_best: tuple[Decimal | None, Decimal | None] | None = None
        async with WsMux(store, product_code=cfg.exchange.product_code) as mux:
            try:
                while end is None or datetime.now(tz=timezone.utc).timestamp() < end:
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
        api_limit_backoff_sec = _env_float("LIVE_API_LIMIT_BACKOFF_SEC") or 1.5
        api_limit_backoff_sec = max(0.2, float(api_limit_backoff_sec))
        irregular_order_backoff_sec = _env_float("LIVE_IRREGULAR_ORDER_BACKOFF_SEC") or 3.0
        irregular_order_backoff_sec = max(0.5, float(irregular_order_backoff_sec))
        api_limit_halt_window_sec = _env_float("LIVE_API_LIMIT_HALT_WINDOW_SEC") or 30.0
        api_limit_halt_window_sec = max(1.0, float(api_limit_halt_window_sec))
        api_limit_halt_threshold = max(1, _env_int("LIVE_API_LIMIT_HALT_THRESHOLD", 25))
        api_limit_halt_cooldown_sec = _env_float("LIVE_API_LIMIT_HALT_COOLDOWN_SEC") or 30.0
        api_limit_halt_cooldown_sec = max(5.0, float(api_limit_halt_cooldown_sec))
        api_limit_halt_new_orders = _env_bool("LIVE_API_LIMIT_HALT_NEW_ORDERS", True)
        irregular_halt_window_sec = _env_float("LIVE_IRREGULAR_HALT_WINDOW_SEC") or 120.0
        irregular_halt_window_sec = max(5.0, float(irregular_halt_window_sec))
        irregular_halt_threshold = max(1, _env_int("LIVE_IRREGULAR_HALT_THRESHOLD", 3))
        irregular_halt_cooldown_sec = _env_float("LIVE_IRREGULAR_HALT_COOLDOWN_SEC") or 180.0
        irregular_halt_cooldown_sec = max(10.0, float(irregular_halt_cooldown_sec))
        irregular_halt_new_orders = _env_bool("LIVE_IRREGULAR_HALT_NEW_ORDERS", True)

        place_min_interval_sec = _env_float("LIVE_PLACE_MIN_INTERVAL_SEC")
        cancel_min_interval_sec = _env_float("LIVE_CANCEL_MIN_INTERVAL_SEC")
        close_min_interval_sec = _env_float("LIVE_CLOSE_MIN_INTERVAL_SEC")
        action_min_interval_by_kind: dict[str, float] = {
            "place_limit": max(0.0, float(place_min_interval_sec if place_min_interval_sec is not None else 0.8)),
            "cancel_all_stall": max(
                0.0,
                float(cancel_min_interval_sec if cancel_min_interval_sec is not None else 0.8),
            ),
            "close_market": max(
                0.0,
                float(close_min_interval_sec if close_min_interval_sec is not None else 1.0),
            ),
        }
        dust_alert_interval_sec = _env_float("LIVE_DUST_ALERT_INTERVAL_SEC") or 120.0
        dust_alert_interval_sec = max(10.0, float(dust_alert_interval_sec))
        dust_normalize_enabled = _env_bool("LIVE_DUST_NORMALIZE_ENABLED", True)
        dust_normalize_cooldown_sec = _env_float("LIVE_DUST_NORMALIZE_COOLDOWN_SEC") or 180.0
        dust_normalize_cooldown_sec = max(10.0, float(dust_normalize_cooldown_sec))
        dust_normalize_wait_sec = _env_float("LIVE_DUST_NORMALIZE_WAIT_SEC") or 0.5
        dust_normalize_wait_sec = max(0.1, float(dust_normalize_wait_sec))
        min_order_size_btc = Decimal(str(cfg.strategy.size_min))

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

        end_mono = None if duration_sec <= 0 else time.monotonic() + duration_sec
        last_eval = 0.0
        eval_interval = max(1, eval_interval_ms) / 1000.0
        last_summary = 0.0
        last_decision_type: Optional[str] = None
        last_snap: Optional[BoardSnapshot] = None
        trade_count = 0
        error_count_total = 0
        error_window_sec = _env_float("ENTRY_GUARD_ERRORS_WINDOW_SEC")
        if error_window_sec is None:
            error_window_sec = 300.0
        error_window_sec = max(0.0, float(error_window_sec))
        error_recent_mono: deque[float] = deque()

        def _guard_error_count(now_mono: float) -> int:
            if error_window_sec <= 0:
                return error_count_total
            return _trim_recent_errors(
                error_recent_mono,
                now_mono=now_mono,
                window_sec=error_window_sec,
            )

        def _record_error(context: str) -> None:
            nonlocal error_count_total
            error_count_total += 1
            now_mono = time.monotonic()
            if error_window_sec > 0:
                error_recent_mono.append(now_mono)
            guard_errors = _guard_error_count(now_mono)
            logger.warning(
                "GUARD_ERROR context={} total_errors={} guard_errors={} window_sec={}",
                context,
                error_count_total,
                guard_errors,
                error_window_sec,
            )

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

        cancel_active_on_start = _env_bool("LIVE_CANCEL_ACTIVE_ON_START", True)
        if cancel_active_on_start:
            startup = await executor.cancel_active_orders_on_start(
                settle_retry=5,
                settle_interval_sec=0.5,
                max_count=500,
            )
            logger.info(
                "STARTUP_ACTIVE_ORDERS active_before={} cancel_requested={} active_after={}",
                startup["active_before"],
                startup["cancel_requested"],
                startup["active_after"],
            )
            if startup["active_after"] > 0:
                await _send_webhook_alert(
                    event="STARTUP_ACTIVE_ORDERS_REMAIN",
                    level="WARN",
                    detail=(
                        f"active_before={startup['active_before']} "
                        f"cancel_requested={startup['cancel_requested']} "
                        f"active_after={startup['active_after']}"
                    ),
                    cfg_path=cfg_path,
                    product_code=cfg.exchange.product_code,
                )
        else:
            await executor.poll(now=datetime.now(tz=timezone.utc), force_reconcile=True)
            remain = executor.active_order_count()
            logger.warning(
                "LIVE_CANCEL_ACTIVE_ON_START=false のため既存ACTIVE注文を維持します "
                f"(active_count={remain})"
            )

        net_pos, avg_pos, pos_rows = await _sync_inventory_from_exchange(
            http=http,
            product_code=cfg.exchange.product_code,
            inventory=inventory,
            strategy=strategy,
        )
        logger.info(
            "STARTUP_POSITION_SYNC positions_rows={} net_position_btc={} avg_price={}",
            pos_rows,
            net_pos,
            avg_pos,
        )
        logger.info(
            "ENTRY_GUARD_ERRORS_WINDOW window_sec={} require_errors<={}",
            error_window_sec,
            guard_thresholds.get("errors"),
        )

        entry_actual = _guard_actual(
            engine=engine,
            strategy=strategy,
            executor=executor,
            error_count=_guard_error_count(time.monotonic()),
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
        dust_last_alert_mono = 0.0
        dust_normalize_last_attempt_mono = -1e9
        api_limit_recent_mono: deque[float] = deque()
        irregular_order_recent_mono: deque[float] = deque()
        new_order_halt_until_mono = 0.0
        new_order_halt_reason = ""
        new_order_halt_last_log_mono = 0.0
        action_last_sent_mono_by_kind: dict[str, float] = {}
        action_throttle_last_log_mono = 0.0

        def _check_consistency() -> bool:
            return strategy.open_order_count == executor.active_order_count(tag=strategy.tag)

        def _sync_strategy_open_orders(now: datetime) -> None:
            active_rows: list[dict[str, Any]] = []
            for order in executor.active_orders.values():
                if order.tag != strategy.tag:
                    continue
                active_rows.append(
                    {
                        "oid": order.oid,
                        "side": order.side,
                        "remaining": order.remaining,
                        "price": order.price,
                    }
                )
            removed, added = strategy.reconcile_open_orders(active_orders=active_rows, now=now)
            if removed or added:
                logger.warning(
                    "OPEN_ORDER_SYNC removed={} added={} strategy_open={} executor_active={}",
                    removed,
                    added,
                    strategy.open_order_count,
                    executor.active_order_count(tag=strategy.tag),
                )

        async def _handle_known_action_exception(exc: BaseException, *, context: str) -> bool:
            nonlocal new_order_halt_until_mono
            nonlocal new_order_halt_reason
            if _is_self_trade_error(exc):
                logger.warning(
                    "{} self-trade detected; skip guard error increment err={}",
                    context,
                    exc,
                )
                return True
            if _is_min_order_size_error(exc):
                logger.error(
                    "{} minimum-order-size detected; skip guard error increment err={}",
                    context,
                    exc,
                )
                return True
            if _is_api_limit_error(exc):
                now_mono = time.monotonic()
                api_limit_recent_mono.append(now_mono)
                recent_api_limit = _trim_recent_errors(
                    api_limit_recent_mono,
                    now_mono=now_mono,
                    window_sec=api_limit_halt_window_sec,
                )
                if api_limit_halt_new_orders and recent_api_limit >= api_limit_halt_threshold:
                    new_until = now_mono + api_limit_halt_cooldown_sec
                    if new_until > new_order_halt_until_mono:
                        new_order_halt_until_mono = new_until
                        new_order_halt_reason = "api_limit"
                        logger.error(
                            "API_LIMIT_HALT_ENTER context={} recent_api_limit={} window_sec={} cooldown_sec={}",
                            context,
                            recent_api_limit,
                            api_limit_halt_window_sec,
                            api_limit_halt_cooldown_sec,
                        )
                logger.warning(
                    "{} api-limit detected; skip guard error increment and backoff {} sec err={}",
                    context,
                    api_limit_backoff_sec,
                    exc,
                )
                await asyncio.sleep(api_limit_backoff_sec)
                return True
            if _is_irregular_order_error(exc):
                now_mono = time.monotonic()
                irregular_order_recent_mono.append(now_mono)
                recent_irregular = _trim_recent_errors(
                    irregular_order_recent_mono,
                    now_mono=now_mono,
                    window_sec=irregular_halt_window_sec,
                )
                if irregular_halt_new_orders and recent_irregular >= irregular_halt_threshold:
                    new_until = now_mono + irregular_halt_cooldown_sec
                    if new_until > new_order_halt_until_mono:
                        new_order_halt_until_mono = new_until
                        new_order_halt_reason = "irregular_order"
                        logger.error(
                            "IRREGULAR_ORDER_HALT_ENTER context={} recent_irregular={} window_sec={} cooldown_sec={}",
                            context,
                            recent_irregular,
                            irregular_halt_window_sec,
                            irregular_halt_cooldown_sec,
                        )
                logger.error(
                    "{} irregular-order detected; skip guard error increment and backoff {} sec err={}",
                    context,
                    irregular_order_backoff_sec,
                    exc,
                )
                await asyncio.sleep(irregular_order_backoff_sec)
                return True
            return False

        async def _maybe_normalize_dust(
            *,
            now: datetime,
            now_mono: float,
            snap: BoardSnapshot,
        ) -> None:
            nonlocal dust_normalize_last_attempt_mono
            if not dust_normalize_enabled:
                return
            if (now_mono - dust_normalize_last_attempt_mono) < dust_normalize_cooldown_sec:
                return
            if strategy.open_order_count > 0 or executor.active_order_count(tag=strategy.tag) > 0:
                return

            inv_state = inventory.state
            plan = _dust_normalize_plan(
                side=inv_state.side,
                size_btc=inv_state.size,
                min_order_size_btc=min_order_size_btc,
            )
            if plan is None:
                return

            (leg1_side, leg1_size), (leg2_side, leg2_size) = plan
            dust_normalize_last_attempt_mono = now_mono
            logger.warning(
                "DUST_NORMALIZE_START side={} size_btc={} min_order_size_btc={} "
                "leg1={} {} leg2={} {}",
                inv_state.side,
                inv_state.size,
                min_order_size_btc,
                leg1_side,
                leg1_size,
                leg2_side,
                leg2_size,
            )
            try:
                await executor.execute(
                    [Action(kind="close_market", side=leg1_side, size=leg1_size, tag="dust_norm1")],
                    now=now,
                    board=snap,
                )
                await asyncio.sleep(dust_normalize_wait_sec)
                await executor.poll(now=now, force_reconcile=True)
                _sync_strategy_open_orders(now)

                await executor.execute(
                    [Action(kind="close_market", side=leg2_side, size=leg2_size, tag="dust_norm2")],
                    now=now,
                    board=snap,
                )
                await asyncio.sleep(dust_normalize_wait_sec)
                await executor.poll(now=now, force_reconcile=True)
                _sync_strategy_open_orders(now)
                logger.warning(
                    "DUST_NORMALIZE_SENT side={} size_btc={} leg1={} {} leg2={} {}",
                    inv_state.side,
                    inv_state.size,
                    leg1_side,
                    leg1_size,
                    leg2_side,
                    leg2_size,
                )
            except Exception as exc:
                if not await _handle_known_action_exception(exc, context="dust_normalize"):
                    _record_error("dust_normalize")
                    logger.exception("dust_normalize execution failed.")

        async def _consume_ws_events(mux: WsMux) -> None:
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
                    _record_error("child_order_events")
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
                while end_mono is None or time.monotonic() < end_mono:
                    now = datetime.now(tz=timezone.utc)
                    now_mono = time.monotonic()

                    await executor.poll(now=now)
                    _sync_strategy_open_orders(now)

                    if now_mono - last_eval >= eval_interval:
                        last_eval = now_mono
                        snap = ob_view.snapshot(now)
                        last_snap = snap
                        actions, meta = strategy.on_board(snap, now=now)

                        if meta.reason == "position_dust_below_min":
                            inv_state = inventory.state
                            if (now_mono - dust_last_alert_mono) >= dust_alert_interval_sec:
                                logger.error(
                                    "POSITION_DUST_BLOCK side={} size_btc={} min_order_size_btc={} reason={}",
                                    inv_state.side,
                                    inv_state.size,
                                    cfg.strategy.size_min,
                                    meta.reason,
                                )
                                await _send_webhook_alert(
                                    event="POSITION_DUST_BLOCK",
                                    level="ERROR",
                                    detail=(
                                        f"side={inv_state.side} "
                                        f"size_btc={inv_state.size} "
                                        f"min_order_size_btc={cfg.strategy.size_min} "
                                        f"reason={meta.reason}"
                                    ),
                                    cfg_path=cfg_path,
                                    product_code=cfg.exchange.product_code,
                                )
                                dust_last_alert_mono = now_mono
                            await _maybe_normalize_dust(now=now, now_mono=now_mono, snap=snap)

                        if now_mono < new_order_halt_until_mono:
                            blocked = [a for a in actions if a.kind == "place_limit"]
                            if blocked:
                                actions = [a for a in actions if a.kind != "place_limit"]
                                entry_guard_blocked_place_limit_total += len(blocked)
                                entry_guard_enforce_last_reason = f"{new_order_halt_reason}_halt"
                                if now_mono - new_order_halt_last_log_mono >= 1.0:
                                    logger.warning(
                                        "NEW_ORDER_HALT_ACTIVE reason={} remaining_sec={} blocked_place_limit={}",
                                        new_order_halt_reason,
                                        round(new_order_halt_until_mono - now_mono, 3),
                                        len(blocked),
                                    )
                                    new_order_halt_last_log_mono = now_mono

                        if any(a.kind == "place_limit" for a in actions):
                            guard_actual = _guard_actual(
                                engine=engine,
                                strategy=strategy,
                                executor=executor,
                                error_count=_guard_error_count(now_mono),
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

                        actions, blocked_by_interval = _filter_actions_by_interval(
                            actions,
                            now_mono=now_mono,
                            min_interval_by_kind=action_min_interval_by_kind,
                            last_sent_mono_by_kind=action_last_sent_mono_by_kind,
                        )
                        if blocked_by_interval and (now_mono - action_throttle_last_log_mono) >= 1.0:
                            logger.info(
                                "ACTION_THROTTLE blocked={} min_interval_by_kind={}",
                                blocked_by_interval,
                                action_min_interval_by_kind,
                            )
                            action_throttle_last_log_mono = now_mono

                        try:
                            await executor.execute(actions, now=now, board=snap)
                        except Exception as exc:
                            if not await _handle_known_action_exception(exc, context="execute_actions"):
                                _record_error("execute_actions")
                                logger.exception("注文実行で例外が発生しました。")

                        await executor.poll(now=now)
                        _sync_strategy_open_orders(now)

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
                    _record_error("shutdown_cancel_all")
                    logger.exception("終了処理の cancel_all で例外が発生しました。")

                close_retry_max = max(1, _env_int("LIVE_CLOSE_MAX_RETRY", 3))
                close_retry_wait_sec = _env_float("LIVE_CLOSE_RETRY_WAIT_SEC") or 0.7
                close_retry_wait_sec = max(0.2, float(close_retry_wait_sec))
                shutdown_fetch_grace_sec = _env_float("LIVE_SHUTDOWN_FETCH_GRACE_SEC") or 2.0
                shutdown_fetch_grace_sec = max(0.0, float(shutdown_fetch_grace_sec))
                shutdown_unknown_confirm_timeout_sec = (
                    _env_float("LIVE_SHUTDOWN_UNKNOWN_CONFIRM_TIMEOUT_SEC") or 90.0
                )
                shutdown_unknown_confirm_timeout_sec = max(
                    5.0, float(shutdown_unknown_confirm_timeout_sec)
                )
                shutdown_unknown_confirm_interval_sec = (
                    _env_float("LIVE_SHUTDOWN_UNKNOWN_CONFIRM_INTERVAL_SEC") or 3.0
                )
                shutdown_unknown_confirm_interval_sec = max(
                    0.5, float(shutdown_unknown_confirm_interval_sec)
                )
                shutdown_unknown_confirm_backoff_multiplier = (
                    _env_float("LIVE_SHUTDOWN_UNKNOWN_CONFIRM_BACKOFF_MULTIPLIER") or 1.8
                )
                shutdown_unknown_confirm_backoff_multiplier = max(
                    1.0, float(shutdown_unknown_confirm_backoff_multiplier)
                )
                shutdown_unknown_confirm_backoff_max_sec = (
                    _env_float("LIVE_SHUTDOWN_UNKNOWN_CONFIRM_BACKOFF_MAX_SEC") or 20.0
                )
                shutdown_unknown_confirm_backoff_max_sec = max(
                    shutdown_unknown_confirm_interval_sec,
                    float(shutdown_unknown_confirm_backoff_max_sec),
                )
                shutdown_dust_normalize_enabled = _env_bool(
                    "LIVE_SHUTDOWN_DUST_NORMALIZE_ENABLED",
                    True,
                )
                shutdown_dust_normalize_wait_sec = (
                    _env_float("LIVE_SHUTDOWN_DUST_NORMALIZE_WAIT_SEC") or 0.8
                )
                shutdown_dust_normalize_wait_sec = max(
                    0.1, float(shutdown_dust_normalize_wait_sec)
                )
                position_fetch_retry = max(1, _env_int("LIVE_POSITION_FETCH_RETRY", 5))
                position_fetch_retry_wait_sec = (
                    _env_float("LIVE_POSITION_FETCH_RETRY_WAIT_SEC") or 1.0
                )
                position_fetch_retry_wait_sec = max(0.2, float(position_fetch_retry_wait_sec))
                min_order_size_btc = Decimal(str(cfg.strategy.size_min))
                if shutdown_fetch_grace_sec > 0:
                    logger.info(
                        "SHUTDOWN_FETCH_GRACE wait_sec={}",
                        shutdown_fetch_grace_sec,
                    )
                    await asyncio.sleep(shutdown_fetch_grace_sec)

                async def _fetch_shutdown_position(
                    context: str,
                ) -> tuple[Optional[Decimal], bool]:
                    api_limited = False
                    for fetch_attempt in range(1, position_fetch_retry + 1):
                        try:
                            positions = await http.get_positions(
                                product_code=cfg.exchange.product_code
                            )
                        except Exception as exc:
                            if _is_api_limit_error(exc):
                                api_limited = True
                            logger.warning(f"get_positions failed: {exc}")
                            positions = None
                        if positions is not None:
                            net, _, _ = _positions_response_to_net(positions)
                            if net is not None:
                                if fetch_attempt > 1:
                                    logger.info(
                                        "get_positions recovered context={} attempt={} retries={}",
                                        context,
                                        fetch_attempt,
                                        position_fetch_retry,
                                    )
                                return net, api_limited
                            if _is_api_limit_response(positions):
                                api_limited = True
                            logger.warning(f"get_positions unexpected response: {positions}")
                        if fetch_attempt < position_fetch_retry:
                            logger.warning(
                                "get_positions retry context={} attempt={} retries={} wait_sec={}",
                                context,
                                fetch_attempt,
                                position_fetch_retry,
                                position_fetch_retry_wait_sec,
                            )
                            await asyncio.sleep(position_fetch_retry_wait_sec)
                    return None, api_limited

                async def _try_shutdown_dust_normalize(
                    *,
                    residual_position_btc: Decimal,
                    attempt: int,
                ) -> bool:
                    if not shutdown_dust_normalize_enabled:
                        return False
                    side = "long" if residual_position_btc > 0 else "short"
                    plan = _dust_normalize_plan(
                        side=side,
                        size_btc=abs(residual_position_btc),
                        min_order_size_btc=min_order_size_btc,
                    )
                    if plan is None:
                        return False

                    (leg1_side, leg1_size), (leg2_side, leg2_size) = plan
                    logger.error(
                        "SHUTDOWN_DUST_NORMALIZE_START residual_position_btc={} attempt={} "
                        "leg1={} {} leg2={} {}",
                        residual_position_btc,
                        attempt,
                        leg1_side,
                        leg1_size,
                        leg2_side,
                        leg2_size,
                    )
                    try:
                        close_board = await ensure_close_board(now)
                        await executor.execute(
                            [
                                Action(
                                    kind="close_market",
                                    side=leg1_side,
                                    size=leg1_size,
                                    tag="shutdown_dust_norm1",
                                )
                            ],
                            now=now,
                            board=close_board,
                        )
                        await asyncio.sleep(shutdown_dust_normalize_wait_sec)
                        await executor.poll(now=now, force_reconcile=True)
                        await executor.execute(
                            [
                                Action(
                                    kind="close_market",
                                    side=leg2_side,
                                    size=leg2_size,
                                    tag="shutdown_dust_norm2",
                                )
                            ],
                            now=now,
                            board=close_board,
                        )
                        await asyncio.sleep(shutdown_dust_normalize_wait_sec)
                        await executor.poll(now=now, force_reconcile=True)
                        logger.error(
                            "SHUTDOWN_DUST_NORMALIZE_SENT residual_position_btc={} attempt={}",
                            residual_position_btc,
                            attempt,
                        )
                        return True
                    except Exception as exc:
                        if not await _handle_known_action_exception(
                            exc,
                            context="shutdown_dust_normalize",
                        ):
                            _record_error("shutdown_dust_normalize")
                            logger.exception(
                                "SHUTDOWN_DUST_NORMALIZE_FAILED residual_position_btc={} attempt={}",
                                residual_position_btc,
                                attempt,
                            )
                        return False

                exchange_pos_before, shutdown_before_api_limited = await _fetch_shutdown_position(
                    context="shutdown_before"
                )
                exchange_pos_after = exchange_pos_before
                shutdown_dust_blocked = False
                if exchange_pos_before is None:
                    confirm_deadline = time.monotonic() + shutdown_unknown_confirm_timeout_sec
                    confirm_attempt = 0
                    confirm_wait_sec = shutdown_unknown_confirm_interval_sec
                    if shutdown_before_api_limited:
                        confirm_wait_sec = min(
                            shutdown_unknown_confirm_backoff_max_sec,
                            shutdown_unknown_confirm_interval_sec
                            * shutdown_unknown_confirm_backoff_multiplier,
                        )
                        logger.warning(
                            "SHUTDOWN_POSITION_CONFIRM_BACKOFF reason=shutdown_before_api_limit "
                            "wait_sec={} next_wait_sec={} multiplier={} max_wait_sec={}",
                            shutdown_unknown_confirm_interval_sec,
                            confirm_wait_sec,
                            shutdown_unknown_confirm_backoff_multiplier,
                            shutdown_unknown_confirm_backoff_max_sec,
                        )
                    while exchange_pos_before is None and time.monotonic() < confirm_deadline:
                        confirm_attempt += 1
                        logger.warning(
                            "SHUTDOWN_POSITION_CONFIRM_WAIT attempt={} interval_sec={} timeout_sec={}",
                            confirm_attempt,
                            confirm_wait_sec,
                            shutdown_unknown_confirm_timeout_sec,
                        )
                        await asyncio.sleep(confirm_wait_sec)
                        exchange_pos_before, confirm_api_limited = await _fetch_shutdown_position(
                            context=f"shutdown_confirm_{confirm_attempt}"
                        )
                        if exchange_pos_before is None:
                            if confirm_api_limited:
                                next_wait_sec = min(
                                    shutdown_unknown_confirm_backoff_max_sec,
                                    max(
                                        confirm_wait_sec,
                                        shutdown_unknown_confirm_interval_sec,
                                    )
                                    * shutdown_unknown_confirm_backoff_multiplier,
                                )
                                logger.warning(
                                    "SHUTDOWN_POSITION_CONFIRM_BACKOFF reason=api_limit "
                                    "attempt={} wait_sec={} next_wait_sec={} multiplier={} "
                                    "max_wait_sec={}",
                                    confirm_attempt,
                                    confirm_wait_sec,
                                    next_wait_sec,
                                    shutdown_unknown_confirm_backoff_multiplier,
                                    shutdown_unknown_confirm_backoff_max_sec,
                                )
                                confirm_wait_sec = next_wait_sec
                            else:
                                confirm_wait_sec = shutdown_unknown_confirm_interval_sec
                    exchange_pos_after = exchange_pos_before
                    if exchange_pos_before is None:
                        _record_error("shutdown_position_unknown")
                        logger.error(
                            "SHUTDOWN_POSITION_UNKNOWN retries={} wait_sec={} confirm_timeout_sec={} "
                            "backoff_multiplier={} backoff_max_sec={}",
                            position_fetch_retry,
                            position_fetch_retry_wait_sec,
                            shutdown_unknown_confirm_timeout_sec,
                            shutdown_unknown_confirm_backoff_multiplier,
                            shutdown_unknown_confirm_backoff_max_sec,
                        )
                        await _send_webhook_alert(
                            event="SHUTDOWN_POSITION_UNKNOWN",
                            level="ERROR",
                            detail=(
                                f"position_fetch_retry={position_fetch_retry} "
                                f"position_fetch_retry_wait_sec={position_fetch_retry_wait_sec} "
                                f"confirm_timeout_sec={shutdown_unknown_confirm_timeout_sec} "
                                "confirm_backoff_reason=api_limit_or_unknown"
                            ),
                            cfg_path=cfg_path,
                            product_code=cfg.exchange.product_code,
                        )
                    exchange_pos_after = exchange_pos_before

                if exchange_pos_before is not None and exchange_pos_before != 0:
                    for attempt in range(1, close_retry_max + 1):
                        if exchange_pos_after is None:
                            logger.warning(
                                "SHUTDOWN_CLOSE_RETRY attempt={} residual_position_btc=unknown",
                                attempt,
                            )
                            await asyncio.sleep(max(close_retry_wait_sec, position_fetch_retry_wait_sec))
                            exchange_pos_after, _ = await _fetch_shutdown_position(
                                context=f"shutdown_retry_unknown_{attempt}"
                            )
                            continue
                        if abs(exchange_pos_after) < min_order_size_btc:
                            normalized = await _try_shutdown_dust_normalize(
                                residual_position_btc=exchange_pos_after,
                                attempt=attempt,
                            )
                            if normalized:
                                exchange_pos_after, _ = await _fetch_shutdown_position(
                                    context=f"shutdown_dust_norm_{attempt}"
                                )
                                continue
                            shutdown_dust_blocked = True
                            logger.error(
                                "SHUTDOWN_CLOSE_DUST residual_position_btc={} min_order_size_btc={}",
                                exchange_pos_after,
                                min_order_size_btc,
                            )
                            await _send_webhook_alert(
                                event="SHUTDOWN_CLOSE_DUST",
                                level="ERROR",
                                detail=(
                                    f"exchange_pos_after={exchange_pos_after} "
                                    f"min_order_size_btc={min_order_size_btc}"
                                ),
                                cfg_path=cfg_path,
                                product_code=cfg.exchange.product_code,
                            )
                            break
                        close_side: Side = "SELL" if exchange_pos_after > 0 else "BUY"
                        close_size = abs(exchange_pos_after)
                        try:
                            await executor.execute(
                                [
                                    Action(
                                        kind="close_market",
                                        side=close_side,
                                        size=close_size,
                                        tag="shutdown",
                                    )
                                ],
                                now=now,
                                board=await ensure_close_board(now),
                            )
                        except Exception as exc:
                            if _is_min_order_size_error(exc):
                                shutdown_dust_blocked = True
                                logger.error(
                                    "SHUTDOWN_CLOSE_DUST residual_position_btc={} min_order_size_btc={} "
                                    "attempt={} err={}",
                                    exchange_pos_after,
                                    min_order_size_btc,
                                    attempt,
                                    exc,
                                )
                                normalized = await _try_shutdown_dust_normalize(
                                    residual_position_btc=exchange_pos_after,
                                    attempt=attempt,
                                )
                                if normalized:
                                    exchange_pos_after, _ = await _fetch_shutdown_position(
                                        context=f"shutdown_dust_err_norm_{attempt}"
                                    )
                                    continue
                                await _send_webhook_alert(
                                    event="SHUTDOWN_CLOSE_DUST",
                                    level="ERROR",
                                    detail=(
                                        f"exchange_pos_after={exchange_pos_after} "
                                        f"min_order_size_btc={min_order_size_btc} "
                                        f"attempt={attempt}"
                                    ),
                                    cfg_path=cfg_path,
                                    product_code=cfg.exchange.product_code,
                                )
                                break
                            if _is_api_limit_error(exc):
                                logger.warning(
                                    "shutdown close_market api-limit detected attempt={} err={}",
                                    attempt,
                                    exc,
                                )
                                await asyncio.sleep(max(close_retry_wait_sec, api_limit_backoff_sec))
                            else:
                                _record_error("shutdown_close_market")
                                logger.exception(
                                    "終了処理の close_market で例外が発生しました。 "
                                    f"attempt={attempt}"
                                )
                        await asyncio.sleep(close_retry_wait_sec)
                        await executor.poll(now=now, force_reconcile=True)
                        exchange_pos_after, _ = await _fetch_shutdown_position(
                            context=f"shutdown_retry_{attempt}"
                        )
                        if exchange_pos_after is None:
                            logger.warning(
                                "SHUTDOWN_CLOSE_RETRY attempt={} residual_position_btc=unknown",
                                attempt,
                            )
                            continue
                        logger.info(
                            "SHUTDOWN_CLOSE_RETRY attempt={} residual_position_btc={}",
                            attempt,
                            exchange_pos_after,
                        )
                        if exchange_pos_after == 0:
                            break
                    if exchange_pos_after is None:
                        _record_error("shutdown_close_position_unknown")
                        logger.error(
                            "SHUTDOWN_CLOSE_POSITION_UNKNOWN close_retry_max={} fetch_retry={}",
                            close_retry_max,
                            position_fetch_retry,
                        )
                        await _send_webhook_alert(
                            event="SHUTDOWN_CLOSE_POSITION_UNKNOWN",
                            level="ERROR",
                            detail=(
                                f"exchange_pos_before={exchange_pos_before} "
                                f"close_retry_max={close_retry_max} "
                                f"position_fetch_retry={position_fetch_retry}"
                            ),
                            cfg_path=cfg_path,
                            product_code=cfg.exchange.product_code,
                        )
                    elif exchange_pos_after != 0:
                        if shutdown_dust_blocked:
                            logger.error(
                                "SHUTDOWN_CLOSE_DUST_RESIDUAL residual_position_btc={}",
                                exchange_pos_after,
                            )
                        else:
                            _record_error("shutdown_close_failed")
                            logger.error(
                                "SHUTDOWN_CLOSE_FAILED residual_position_btc={}",
                                exchange_pos_after,
                            )
                            await _send_webhook_alert(
                                event="SHUTDOWN_CLOSE_FAILED",
                                level="ERROR",
                                detail=(
                                    f"exchange_pos_before={exchange_pos_before} "
                                    f"exchange_pos_after={exchange_pos_after} "
                                    f"close_retry_max={close_retry_max}"
                                ),
                                cfg_path=cfg_path,
                                product_code=cfg.exchange.product_code,
                            )
                elif exchange_pos_before == 0:
                    logger.info("SHUTDOWN_CLOSE_SKIP residual_position_btc=0")
                await executor.poll(now=now, force_reconcile=True)

                promote_actual = _guard_actual(
                    engine=engine,
                    strategy=strategy,
                    executor=executor,
                    error_count=_guard_error_count(time.monotonic()),
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
                    f"exchange_pos_before={exchange_pos_before} "
                    f"exchange_pos_after={exchange_pos_after} "
                    f"open_orders_before={open0} open_orders_after={strategy.open_order_count} "
                    f"active_orders_before={active0} "
                    f"active_orders_after={executor.active_order_count(tag=strategy.tag)} "
                    f"pnl_before={pnl0} pnl_after={pnl1} "
                    f"errors_total={error_count_total} "
                    f"guard_errors={_guard_error_count(time.monotonic())}"
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
    parser.add_argument(
        "--duration-sec",
        type=int,
        default=0,
        help="0 以下で無期限実行（default: 0）",
    )
    parser.add_argument("--confirm", default="")
    parser.add_argument("--eval-interval-ms", type=int, default=50)
    parser.add_argument("--summary-interval-sec", type=int, default=10)
    args = parser.parse_args()

    try:
        if args.mode == "listen":
            asyncio.run(_run_listen(args.override, args.duration_sec))
        else:
            if _env_bool("LIVE_REQUIRE_CONFIRM", True) and args.confirm != "I_UNDERSTAND":
                raise SystemExit(
                    "Refusing to place live orders. Pass --confirm I_UNDERSTAND "
                    "or set LIVE_REQUIRE_CONFIRM=false."
                )
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
    except Exception as exc:
        _send_webhook_alert_best_effort(
            event="RUN_LIVE_FATAL",
            level="ERROR",
            detail=f"{type(exc).__name__}: {exc}",
            cfg_path=args.override,
        )
        raise


if __name__ == "__main__":
    main()
