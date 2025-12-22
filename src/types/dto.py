from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional


Side = Literal["BUY", "SELL"]
InventorySide = Literal["flat", "long", "short"]
ActionKind = Literal["place_limit", "cancel_all_stall", "close_market"]


@dataclass
class BoardSnapshot:
    ts: datetime
    best_bid_price: Optional[Decimal]
    best_bid_size: Optional[Decimal]
    best_ask_price: Optional[Decimal]
    best_ask_size: Optional[Decimal]
    best_age_ms: int
    spread_ticks: Optional[int]


@dataclass
class InventoryState:
    side: InventorySide
    size: Decimal
    avg_price: Optional[Decimal]
    pnl_today_jpy: Decimal
    max_drawdown_jpy: Decimal
    last_pnl_jpy: Decimal


@dataclass
class Action:
    kind: ActionKind
    side: Optional[Side] = None
    price: Optional[Decimal] = None
    size: Optional[Decimal] = None
    ttl_ms: Optional[int] = None
    tag: Optional[str] = None
