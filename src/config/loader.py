import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field


class ExchangeConfig(BaseModel):
    name: str
    product_code: str
    base_url: str
    ws_url: str


class PybottersConfig(BaseModel):
    api_label: str = Field(default="bitflyer")
    timeout_sec: int = Field(default=10)


class WsConfig(BaseModel):
    channels: list[str]


class StallStrategyConfig(BaseModel):
    stall_T_ms: int
    min_spread_tick: int
    ttl_ms: int
    max_reverse_ticks: int
    ca_ratio_win_ms: int = 500  # Cancel/Add比を数える移動窓（ミリ秒）
    ca_threshold: float = 1.3   # C/A比の上限（これ以下のときだけ発注を許可）
    quote_mode: str = Field(default="mid")  # mid|inside
    quote_offset_ticks: int = Field(default=1)
    size_min: float
    max_inventory_btc: float


class RiskConfig(BaseModel):
    daily_pnl_jpy: float
    max_dd_jpy: float


class AppConfig(BaseModel):
    env: str
    exchange: ExchangeConfig
    pybotters: PybottersConfig
    ws: WsConfig
    strategy: StallStrategyConfig
    risk: RiskConfig


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Shallow + nested mapping merge (override wins)."""
    merged: dict[str, Any] = dict(base)
    for key, val in override.items():
        if (
            key in merged
            and isinstance(merged[key], Mapping)
            and isinstance(val, Mapping)
        ):
            merged[key] = _deep_merge(dict(merged[key]), dict(val))
        else:
            merged[key] = val
    return merged


def load_yaml(path: os.PathLike[str] | str) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8")
    return yaml.safe_load(text) or {}


def load_app_config(base_path: str, override_path: Optional[str] = None) -> AppConfig:
    base = load_yaml(base_path)
    merged = base
    if override_path and Path(override_path).exists():
        override = load_yaml(override_path)
        merged = _deep_merge(base, override)
    return AppConfig.model_validate(merged)
