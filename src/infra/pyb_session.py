import asyncio
import json
import os
from pathlib import Path
from typing import Callable, Optional

import aiohttp
import pybotters
from loguru import logger

from src.config.loader import AppConfig


def _load_env_key(name: str, env_path: Path) -> Optional[str]:
    val = os.getenv(name)
    if val:
        return val
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                if k.strip() == name:
                    return v.split("#", 1)[0].strip()
    return None


class PyBotterSession:
    """Wrapper for pybotters Client + bitFlyerDataStore + WS connect."""

    def __init__(
        self,
        cfg: AppConfig,
        *,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        env_path: Path | str = ".env",
        timeout_sec: Optional[int] = None,
    ) -> None:
        self.cfg = cfg
        env_path = Path(env_path)
        self.api_key = api_key or _load_env_key("BF_API_KEY", env_path) or ""
        self.api_secret = api_secret or _load_env_key("BF_API_SECRET", env_path) or ""
        self.timeout_sec = timeout_sec or cfg.pybotters.timeout_sec
        self.client: pybotters.Client | None = None
        self.store: pybotters.bitFlyerDataStore | None = None
        self.app: pybotters.ws.WebSocketApp | None = None

    async def __aenter__(self) -> "PyBotterSession":
        apis = {
            self.cfg.pybotters.api_label: [
                self.api_key,
                self.api_secret,
            ]
        }
        self.client = pybotters.Client(
            apis=apis,
            base_url=self.cfg.exchange.base_url,
            timeout=aiohttp.ClientTimeout(total=self.timeout_sec),
        )
        self.store = pybotters.bitFlyerDataStore()
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    def _make_handlers(
        self,
        store: pybotters.bitFlyerDataStore,
        on_info: Optional[Callable[[dict], None]] = None,
        on_message: Optional[Callable[[dict], None]] = None,
    ):
        def handle(obj, ws):
            if isinstance(obj, dict):
                if "error" in obj:
                    logger.warning(f"WS error: {obj}")
                if on_message:
                    on_message(obj)
            if on_info and isinstance(obj, dict) and "params" not in obj:
                on_info(obj)
            store.onmessage(obj, ws)

        def h_str(text, ws):
            try:
                data = json.loads(text)
            except Exception:
                return
            handle(data, ws)

        def h_bytes(b, ws):
            try:
                text = b.decode()
                data = json.loads(text)
            except Exception:
                return
            handle(data, ws)

        return h_str, h_bytes

    async def connect_ws(
        self,
        channels: list[str],
        *,
        on_info: Optional[Callable[[dict], None]] = None,
        on_message: Optional[Callable[[dict], None]] = None,
    ) -> None:
        if not self.client or not self.store:
            raise RuntimeError("Session not initialized. Use `async with`.")
        h_str, h_bytes = self._make_handlers(self.store, on_info, on_message)
        send_json = [
            {"method": "subscribe", "params": {"channel": ch}, "id": f"sub:{ch}"}
            for ch in channels
        ]
        self.app = self.client.ws_connect(
            self.cfg.exchange.ws_url,
            send_json=send_json,
            hdlr_str=h_str,
            hdlr_bytes=h_bytes,
            autoping=True,
            heartbeat=20.0,
        )
        await self.app  # wait handshake/auth

    async def close(self) -> None:
        if self.app and self.app.current_ws:
            try:
                await asyncio.wait_for(self.app.current_ws.close(), timeout=5)
            except Exception as e:
                logger.warning(f"WS close failed: {e}")
        if self.client:
            await self.client.close()

    def get_client(self) -> pybotters.Client:
        if not self.client:
            raise RuntimeError("Client not ready. Call within async context.")
        return self.client

    def get_store(self) -> pybotters.bitFlyerDataStore:
        if not self.store:
            raise RuntimeError("Store not ready. Call within async context.")
        return self.store
