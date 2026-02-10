from typing import Any, Optional

import pybotters

from src.config.loader import ExchangeConfig


class BitflyerHttp:
    """Thin REST wrapper for bitFlyer endpoints via pybotters.Client."""

    def __init__(self, client: pybotters.Client, exchange_cfg: ExchangeConfig):
        self.client = client
        self.exchange_cfg = exchange_cfg

    async def get_balance(self) -> Any:
        resp = await self.client.get("/v1/me/getbalance")
        return await resp.json(content_type=None)

    async def get_collateral(self) -> Any:
        resp = await self.client.get("/v1/me/getcollateral")
        return await resp.json(content_type=None)

    async def get_positions(self, *, product_code: Optional[str] = None) -> Any:
        params = {"product_code": product_code or self.exchange_cfg.product_code}
        resp = await self.client.get("/v1/me/getpositions", params=params)
        return await resp.json(content_type=None)

    async def get_board(self, product_code: Optional[str] = None) -> Any:
        params = {"product_code": product_code or self.exchange_cfg.product_code}
        resp = await self.client.get("/v1/getboard", params=params)
        return await resp.json(content_type=None)

    async def send_limit_order(
        self,
        *,
        side: str,
        price: float | int,
        size: float,
        product_code: Optional[str] = None,
        ttl_ms: Optional[int] = None,
        tag: Optional[str] = None,
    ) -> Any:
        payload: dict[str, Any] = {
            "product_code": product_code or self.exchange_cfg.product_code,
            "child_order_type": "LIMIT",
            "side": side,
            "price": price,
            "size": size,
        }
        if ttl_ms is not None:
            payload["minute_to_expire"] = int(ttl_ms / 1000 / 60) or 1
        if tag:
            payload["identifier"] = tag
        resp = await self.client.post("/v1/me/sendchildorder", data=payload)
        return await resp.json(content_type=None)

    async def send_market_order(
        self,
        *,
        side: str,
        size: float,
        product_code: Optional[str] = None,
        tag: Optional[str] = None,
    ) -> Any:
        payload: dict[str, Any] = {
            "product_code": product_code or self.exchange_cfg.product_code,
            "child_order_type": "MARKET",
            "side": side,
            "size": size,
        }
        if tag:
            payload["identifier"] = tag
        resp = await self.client.post("/v1/me/sendchildorder", data=payload)
        return await resp.json(content_type=None)

    async def cancel_order(
        self,
        child_order_acceptance_id: str,
        product_code: Optional[str] = None,
    ) -> Any:
        payload = {
            "product_code": product_code or self.exchange_cfg.product_code,
            "child_order_acceptance_id": child_order_acceptance_id,
        }
        resp = await self.client.post("/v1/me/cancelchildorder", data=payload)
        try:
            return await resp.json(content_type=None)
        except Exception:
            return await resp.text()

    async def get_child_orders(
        self,
        *,
        product_code: Optional[str] = None,
        child_order_state: str = "ACTIVE",
        count: int = 20,
    ) -> Any:
        params = {
            "product_code": product_code or self.exchange_cfg.product_code,
            "child_order_state": child_order_state,
            "count": count,
        }
        resp = await self.client.get("/v1/me/getchildorders", params=params)
        return await resp.json(content_type=None)
