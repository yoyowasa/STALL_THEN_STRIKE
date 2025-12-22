from datetime import datetime
from decimal import Decimal
from typing import Optional

import pybotters

from src.types.dto import BoardSnapshot


class OrderBookView:
    """Convert pybotters board DataStore into BoardSnapshot with best age + spread."""

    def __init__(
        self,
        store: pybotters.bitFlyerDataStore,
        product_code: str,
        tick_size: Decimal,
    ) -> None:
        self.store = store
        self.product_code = product_code
        self.tick_size = tick_size
        self._last_best_changed_at: Optional[datetime] = None
        self._last_best: tuple[Optional[Decimal], Optional[Decimal]] = (None, None)

    def _best(self) -> tuple[Optional[Decimal], Optional[Decimal], Optional[Decimal], Optional[Decimal]]:
        board = self.store.board.sorted(query={"product_code": self.product_code}, limit=1)
        bids = board.get("bids") or []
        asks = board.get("asks") or []
        bid = Decimal(str(bids[0]["price"])) if bids else None
        bid_size = Decimal(str(bids[0]["size"])) if bids else None
        ask = Decimal(str(asks[0]["price"])) if asks else None
        ask_size = Decimal(str(asks[0]["size"])) if asks else None
        return bid, bid_size, ask, ask_size

    def snapshot(self, now: datetime) -> BoardSnapshot:
        bid, bid_size, ask, ask_size = self._best()
        if (bid, ask) != self._last_best:
            self._last_best = (bid, ask)
            self._last_best_changed_at = now
        if self._last_best_changed_at is None:
            best_age_ms = 0
        else:
            delta = now - self._last_best_changed_at
            best_age_ms = int(delta.total_seconds() * 1000)

        spread_ticks: Optional[int] = None
        if bid is not None and ask is not None and self.tick_size:
            spread_ticks = int((ask - bid) / self.tick_size)

        return BoardSnapshot(
            ts=now,
            best_bid_price=bid,
            best_bid_size=bid_size,
            best_ask_price=ask,
            best_ask_size=ask_size,
            best_age_ms=best_age_ms,
            spread_ticks=spread_ticks,
        )
