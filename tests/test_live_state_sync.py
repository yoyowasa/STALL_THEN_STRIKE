from datetime import datetime, timezone
from decimal import Decimal

from src.config.loader import RiskConfig, StallStrategyConfig
from src.engine.inventory import FillEvent, InventoryManager
from src.engine.live_executor import LiveExecutor
from src.engine.strategy_stall import StallThenStrikeStrategy
from src.types.dto import BoardSnapshot


def _build_strategy() -> StallThenStrikeStrategy:
    cfg = StallStrategyConfig(
        stall_T_ms=250,
        min_spread_tick=1,
        ttl_ms=2000,
        max_reverse_ticks=4,
        size_min=0.001,
        max_inventory_btc=0.01,
    )
    risk = RiskConfig(daily_pnl_jpy=-9999999, max_dd_jpy=-9999999)
    inventory = InventoryManager(risk)
    return StallThenStrikeStrategy(cfg, tick_size=Decimal("1"), inventory=inventory)


def test_strategy_clears_known_oid_even_without_tag(monkeypatch):
    monkeypatch.delenv("AB_EXPECT_CA_THRESHOLD", raising=False)
    strategy = _build_strategy()
    now = datetime.now(tz=timezone.utc)
    oid = "JRF-TEST-001"

    strategy.on_order_event(
        {
            "event_type": "ORDER",
            "tag": "stall",
            "child_order_acceptance_id": oid,
            "side": "BUY",
            "size": "0.001",
            "price": "100",
        },
        now=now,
    )
    assert strategy.open_order_count == 1

    strategy.on_order_event(
        {
            "event_type": "EXECUTION",
            "tag": "",
            "child_order_acceptance_id": oid,
            "outstanding_size": "0",
        },
        now=now,
    )
    assert strategy.open_order_count == 0


def test_strategy_reconcile_open_orders_replaces_stale(monkeypatch):
    monkeypatch.delenv("AB_EXPECT_CA_THRESHOLD", raising=False)
    strategy = _build_strategy()
    now = datetime.now(tz=timezone.utc)

    strategy.on_order_event(
        {
            "event_type": "ORDER",
            "tag": "stall",
            "child_order_acceptance_id": "JRF-OLD-001",
            "side": "BUY",
            "size": "0.001",
            "price": "101",
        },
        now=now,
    )
    assert strategy.open_order_count == 1

    removed, added = strategy.reconcile_open_orders(
        active_orders=[
            {
                "oid": "JRF-NEW-001",
                "side": "SELL",
                "remaining": "0.002",
                "price": "102",
            }
        ],
        now=now,
    )
    assert removed == 1
    assert added == 1
    assert strategy.open_order_count == 1


def test_live_executor_keeps_tag_for_late_event_annotation():
    executor = LiveExecutor(http=object(), product_code="FX_BTC_JPY")
    oid = "JRF-TEST-EX-001"

    executor.on_child_event(
        {
            "event_type": "ORDER",
            "child_order_acceptance_id": oid,
            "tag": "stall",
            "side": "BUY",
            "size": "0.001",
            "outstanding_size": "0.001",
            "price": "100",
        }
    )
    assert executor.active_order_count(tag="stall") == 1

    executor.on_child_event(
        {
            "event_type": "EXECUTION",
            "child_order_acceptance_id": oid,
            "outstanding_size": "0",
        }
    )
    assert executor.active_order_count(tag="stall") == 0

    enriched = executor.annotate_child_event(
        {
            "event_type": "EXECUTION",
            "child_order_acceptance_id": oid,
            "outstanding_size": "0",
        }
    )
    assert enriched.get("tag") == "stall"


def test_strategy_dust_position_does_not_emit_close_market(monkeypatch):
    monkeypatch.delenv("AB_EXPECT_CA_THRESHOLD", raising=False)
    strategy = _build_strategy()
    now = datetime.now(tz=timezone.utc)

    fill = FillEvent(
        ts=now,
        order_id="JRF-DUST-001",
        side="BUY",
        price=Decimal("100"),
        size=Decimal("0.00077"),
        tag="stall",
    )
    strategy.inventory.apply_fill(fill)
    strategy.on_fill(fill)

    board = BoardSnapshot(
        ts=now,
        best_bid_price=Decimal("90"),
        best_bid_size=Decimal("1"),
        best_ask_price=Decimal("91"),
        best_ask_size=Decimal("1"),
        best_age_ms=300,
        spread_ticks=1,
    )
    actions, meta = strategy.on_board(board, now=now)

    assert meta.reason == "position_dust_below_min"
    assert all(a.kind != "close_market" for a in actions)
