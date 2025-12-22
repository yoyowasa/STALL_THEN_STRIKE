"""
Raw bitFlyer WS probe (aiohttp): authenticate + subscribe and print messages.

This bypasses pybotters' WS layer so you can see auth/sub responses directly.

Usage:
    .\\.venv\\Scripts\\python -m src.app.ws_raw_probe --mode listen
    .\\.venv\\Scripts\\python -m src.app.ws_raw_probe --mode order --wait-sec 30
"""

import argparse
import asyncio
import hashlib
import hmac
import json
import time
from pathlib import Path
from secrets import token_hex

import aiohttp
from loguru import logger

from src.config.loader import load_app_config
from src.infra.http_bitflyer import BitflyerHttp
from src.infra.pyb_session import PyBotterSession


DEFAULT_CHANNELS = [
    "child_order_events",
    "lightning_ticker_FX_BTC_JPY",
]


def _setup_logging() -> None:
    log_dir = Path("logs") / "runtime"
    log_dir.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    logger.add(log_dir / f"ws-raw-probe-{ts}.log", level="INFO", enqueue=True)


def load_env(env_path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.split("#", 1)[0].strip()
    return env


async def ws_auth(ws: aiohttp.ClientWebSocketResponse, api_key: str, api_secret: str) -> dict:
    ts = int(time.time() * 1000)
    nonce = token_hex(16)
    sig = hmac.new(
        api_secret.encode(),
        f"{ts}{nonce}".encode(),
        digestmod=hashlib.sha256,
    ).hexdigest()
    await ws.send_json(
        {
            "method": "auth",
            "params": {
                "api_key": api_key,
                "timestamp": ts,
                "nonce": nonce,
                "signature": sig,
            },
            "id": "auth",
        }
    )
    while True:
        msg = await ws.receive(timeout=10)
        if msg.type != aiohttp.WSMsgType.TEXT:
            continue
        try:
            data = json.loads(msg.data)
        except Exception:
            continue
        if data.get("id") == "auth":
            return data


async def ws_subscribe(ws: aiohttp.ClientWebSocketResponse, channels: list[str]) -> list[dict]:
    for ch in channels:
        await ws.send_json({"method": "subscribe", "params": {"channel": ch}, "id": f"sub:{ch}"})

    acks: list[dict] = []
    deadline = time.time() + 10
    pending = {f"sub:{ch}" for ch in channels}
    while pending and time.time() < deadline:
        msg = await ws.receive(timeout=10)
        if msg.type != aiohttp.WSMsgType.TEXT:
            continue
        try:
            data = json.loads(msg.data)
        except Exception:
            continue
        if isinstance(data, dict) and data.get("id") in pending:
            pending.remove(data["id"])
            acks.append(data)
    return acks


async def main_async(mode: str, wait_sec: int) -> None:
    cfg = load_app_config("configs/base.yml", "configs/paper.yml")
    _setup_logging()
    env = load_env(Path(".env"))
    api_key = env.get("BF_API_KEY", "")
    api_secret = env.get("BF_API_SECRET", "")

    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(cfg.exchange.ws_url, autoping=True, heartbeat=20.0) as ws:
            auth_res = await ws_auth(ws, api_key, api_secret)
            logger.info(f"auth response: {auth_res}")
            if "error" in auth_res:
                return

            sub_acks = await ws_subscribe(ws, DEFAULT_CHANNELS)
            for ack in sub_acks:
                logger.info(f"sub ack: {ack}")

            if mode == "order":
                async with PyBotterSession(cfg) as sess:
                    http = BitflyerHttp(sess.get_client(), cfg.exchange)
                    board = await http.get_board()
                    mid = int(float(board["mid_price"]))
                    far_buy = max(1, mid - 2_000_000)
                    res = await http.send_limit_order(side="BUY", price=far_buy, size=0.001)
                    oid = res.get("child_order_acceptance_id")
                    logger.info(f"placed BUY @{far_buy} -> {oid}")
                    await asyncio.sleep(2)
                    cancel_res = await http.cancel_order(oid)
                    logger.info(f"cancel -> {cancel_res}")

            logger.info(f"listening {wait_sec}s...")
            end = time.time() + wait_sec
            while time.time() < end:
                msg = await ws.receive(timeout=10)
                if msg.type != aiohttp.WSMsgType.TEXT:
                    continue
                try:
                    data = json.loads(msg.data)
                except Exception:
                    continue
                if isinstance(data, dict) and data.get("method") == "channelMessage":
                    ch = data.get("params", {}).get("channel")
                    if ch in {"child_order_events"}:
                        logger.info(f"child channel msg: {data}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["listen", "order"], default="listen")
    parser.add_argument("--wait-sec", type=int, default=30)
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args.mode, args.wait_sec))
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
