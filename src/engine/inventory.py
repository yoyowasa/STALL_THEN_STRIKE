from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Optional

from src.config.loader import RiskConfig
from src.types.dto import InventorySide, InventoryState, Side


@dataclass
class PnLState:
    position_btc: Decimal = Decimal("0")
    avg_price: Optional[Decimal] = None
    realized_pnl_jpy: Decimal = Decimal("0")
    unrealized_pnl_jpy: Decimal = Decimal("0")
    mark_price: Optional[Decimal] = None

    @property
    def total_pnl_jpy(self) -> Decimal:
        return self.realized_pnl_jpy + self.unrealized_pnl_jpy


class PnLTracker:
    """Minimal position + (realized/unrealized) PnL tracker for FX_BTC_JPY."""

    def __init__(self) -> None:
        self.state = PnLState()

    def update_mark(self, mark_price: Decimal) -> PnLState:
        self.state.mark_price = mark_price
        self._recalc_unrealized()
        return self.state

    def apply_execution(self, *, side: str, price: Decimal, size: Decimal) -> PnLState:
        if size <= 0:
            return self.state

        pos = self.state.position_btc
        avg = self.state.avg_price

        trade_sign = Decimal("1") if side == "BUY" else Decimal("-1")
        trade = trade_sign * size

        if pos == 0:
            self.state.position_btc = trade
            self.state.avg_price = price
            self._recalc_unrealized()
            return self.state

        if avg is None:
            self.state.avg_price = price
            avg = price

        same_dir = (pos > 0 and trade > 0) or (pos < 0 and trade < 0)
        if same_dir:
            new_pos = pos + trade
            wavg = (abs(pos) * avg + abs(trade) * price) / abs(new_pos)
            self.state.position_btc = new_pos
            self.state.avg_price = wavg
            self._recalc_unrealized()
            return self.state

        # reducing / flipping
        close_size = min(abs(pos), abs(trade))
        if pos > 0:
            # closing long with SELL
            self.state.realized_pnl_jpy += (price - avg) * close_size
        else:
            # closing short with BUY
            self.state.realized_pnl_jpy += (avg - price) * close_size

        remaining = abs(trade) - close_size
        if remaining == 0:
            self.state.position_btc = Decimal("0")
            self.state.avg_price = None
        else:
            # flipped: open new position with remaining at execution price
            self.state.position_btc = trade_sign * remaining
            self.state.avg_price = price

        self._recalc_unrealized()
        return self.state

    def _recalc_unrealized(self) -> None:
        pos = self.state.position_btc
        avg = self.state.avg_price
        mark = self.state.mark_price
        if pos == 0 or avg is None or mark is None:
            self.state.unrealized_pnl_jpy = Decimal("0")
            return
        if pos > 0:
            self.state.unrealized_pnl_jpy = (mark - avg) * pos
        else:
            self.state.unrealized_pnl_jpy = (avg - mark) * abs(pos)


@dataclass(frozen=True)
class FillEvent:
    ts: datetime
    order_id: str
    side: Side
    price: Decimal
    size: Decimal
    fee: Decimal = Decimal("0")
    tag: Optional[str] = None


class InventoryManager:
    """Position/PnL + risk metrics (daily pnl, drawdown) for strategy decisions."""

    def __init__(self, risk: RiskConfig) -> None:
        self.risk = risk
        self._pnl = PnLTracker()
        self._peak_total_pnl: Decimal = Decimal("0")
        self._max_drawdown: Decimal = Decimal("0")
        self._last_total_pnl: Decimal = Decimal("0")

    @property
    def pnl(self) -> PnLTracker:
        return self._pnl

    @property
    def position_btc(self) -> Decimal:
        return self._pnl.state.position_btc

    @property
    def avg_price(self) -> Optional[Decimal]:
        return self._pnl.state.avg_price

    @property
    def state(self) -> InventoryState:
        pos = self._pnl.state.position_btc
        side: InventorySide
        if pos == 0:
            side = "flat"
        elif pos > 0:
            side = "long"
        else:
            side = "short"
        return InventoryState(
            side=side,
            size=abs(pos),
            avg_price=self._pnl.state.avg_price,
            pnl_today_jpy=self._pnl.state.total_pnl_jpy,
            max_drawdown_jpy=self._max_drawdown,
            last_pnl_jpy=self._last_total_pnl,
        )

    def update_mark(self, mark_price: Decimal) -> InventoryState:
        self._pnl.update_mark(mark_price)
        self._update_risk_metrics()
        return self.state

    def apply_fill(self, fill: FillEvent) -> InventoryState:
        self._pnl.apply_execution(side=fill.side, price=fill.price, size=fill.size)
        self._update_risk_metrics()
        return self.state

    def kill_switch(self) -> bool:
        pnl_limit = Decimal(str(self.risk.daily_pnl_jpy))
        dd_limit = Decimal(str(self.risk.max_dd_jpy))
        st = self.state
        return (st.pnl_today_jpy <= pnl_limit) or (st.max_drawdown_jpy <= dd_limit)

    def _update_risk_metrics(self) -> None:
        total = self._pnl.state.total_pnl_jpy
        if total > self._peak_total_pnl:
            self._peak_total_pnl = total
        dd = total - self._peak_total_pnl
        if dd < self._max_drawdown:
            self._max_drawdown = dd
        self._last_total_pnl = total
