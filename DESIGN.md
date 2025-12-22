# stall_then_strike 全体設計書（pybotters版 / 自炊なし）
bitFlyer Crypto CFD `FX_BTC_JPY` / Python 3.11 / Poetry

## 0. ゴールと前提
- WebSocket 自炊なしで板・約定を扱う。`pybotters` の `Client` / `bitFlyerDataStore` が HTTP・WS・再接続・板差分処理を面倒みる。
- 戦略ロジックは DataStore から見える板の Best・Spread を使い「Best が静止したら両面に一撃置く」シンプルな MM。
- 在庫・PnL の最小限管理、ロット管理、Kill スイッチ、ログ出しを自前で実装する。

## 1. 技術スタック
- 言語: Python 3.11
- パッケージ管理: Poetry
- 主要ライブラリ: `pybotters<2.0`, `pydantic`, `pyyaml`, `orjson`, `loguru`, `rich`
- 取引所: bitFlyer Crypto CFD (`product_code="FX_BTC_JPY"`) — Lightning FX と HTTP/WS 互換

## 2. ディレクトリ構成
```
stall_then_strike/
├─ README.md                  # 戦略概要と使い方
├─ DESIGN.md                  # 本ドキュメント
├─ .env.example               # API キー雛形（.env は git ignore を想定）
├─ pyproject.toml             # Poetry 設定
├─ configs/
│  ├─ base.yml                # 共通設定
│  ├─ live.yml                # 本番用上書き
│  └─ paper.yml               # ペーパー用上書き
├─ src/
│  ├─ config/
│  │  └─ loader.py            # YAML → Pydantic モデル化
│  ├─ infra/
│  │  ├─ pyb_session.py       # pybotters.Client & DataStore 管理
│  │  ├─ orderbook_view.py    # DataStore → BoardSnapshot 変換
│  │  ├─ http_bitflyer.py     # REST 発注/取消ラッパ
│  │  ├─ logging_runtime.py   # loguru 設定（人間向け runtime ログ）
│  │  └─ logging_structured.py# trades/orders/decisions 用ロガー
│  ├─ engine/
│  │  ├─ inventory.py         # 在庫・PnL 管理
│  │  ├─ strategy_stall.py    # 戦略ロジック（静止→一撃）
│  │  └─ executor.py          # Action → HTTP 注文
│  ├─ app/
│  │  ├─ run_live.py          # 本番起動エントリ
│  │  └─ run_paper.py         # ペーパー起動エントリ
│  └─ types/
│     └─ dto.py               # BoardSnapshot / Decision / Action / LogRow 型
├─ logs/
│  ├─ runtime/                # app 全体ログ（テキスト）
│  ├─ trades/                 # 約定ごとの構造化ログ
│  ├─ orders/                 # 発注/取消/失効の構造化ログ
│  └─ decisions/              # 戦略意思決定ログ
└─ data/
   ├─ raw/                    # 必要ならテープ保存
   └─ results/                # 分析結果
```

## 3. 設定モデル（`src/config/loader.py`）
### 3.1 YAML 例（`configs/base.yml`）
```yaml
env: paper

exchange:
  name: bitflyer
  product_code: FX_BTC_JPY
  base_url: "https://api.bitflyer.com"
  ws_url: "wss://ws.lightstream.bitflyer.com/json-rpc"

pybotters:
  api_label: "bitflyer"
  timeout_sec: 10

ws:
  channels:
    - "lightning_board_snapshot_FX_BTC_JPY"
    - "lightning_board_FX_BTC_JPY"

strategy:
  stall_T_ms: 250
  min_spread_tick: 1
  ttl_ms: 800
  max_reverse_ticks: 2
  size_min: 0.001
  max_inventory_btc: 0.2

risk:
  daily_pnl_jpy: -30000
  max_dd_jpy: -20000
```

### 3.2 Pydantic モデル
- `ExchangeConfig`: `name`, `product_code`, `base_url`, `ws_url`
- `PybottersConfig`: `api_label`, `timeout_sec`
- `WsConfig`: `channels`
- `StallStrategyConfig`: `stall_T_ms`, `min_spread_tick`, `ttl_ms`, `max_reverse_ticks`, `size_min`, `max_inventory_btc`
- `RiskConfig`: `daily_pnl_jpy`, `max_dd_jpy`
- `AppConfig`: 上記を束ねる

### 3.3 ローダー
`load_app_config(base_path: str, override_path: str | None) -> AppConfig`
- `base.yml` を読み込み、`live.yml` もしくは `paper.yml` で上書きマージ。
- `.env` から API キーを読むのはローダー外（pyb_session 内）で実施。

## 4. pybotters インフラ
### 4.1 セッション管理（`src/infra/pyb_session.py`）
`PyBotterSession` が `pybotters.Client` と `bitFlyerDataStore` をまとめる。
- `__aenter__ / __aexit__` で API キーを `.env` から読み `apis = {"bitflyer": [key, secret]}` をセット。
- `pybotters.Client(apis=apis, base_url=cfg.exchange.base_url, timeout=cfg.pybotters.timeout_sec)` を生成。
- `self.store = pybotters.bitFlyerDataStore()` を用意。
- `connect_ws()` で `cfg.ws.channels` から `send_json` を組み立て `client.ws_connect(..., hdlr_json=self.store.onmessage)` を貼る。
- `get_store()` / `get_client()` で戦略や executor に渡す。

### 4.2 Board ビュー（`src/infra/orderbook_view.py`）
`OrderBookView` が DataStore の板を `BoardSnapshot` に変換。
- `snapshot(now)`:
  - `store.board.sorted(limit=1)` から BestBid/Ask を取得。
  - 前回 Best と違えば `_last_best_changed_at = now` を更新。
  - `best_age_ms = (now - _last_best_changed_at)` を算出。
  - `spread_ticks = (best_ask - best_bid) / tick_size`。
- アプリ側では `with store.board.watch() as stream: async for change in stream:` で板更新を監視し、毎回 `snapshot` を戦略へ渡す。

### 4.3 HTTP ラッパ（`src/infra/http_bitflyer.py`）
`BitflyerHttp` が pybotters.Client 経由で REST 呼び出し。
- `get_balance()` → `/v1/me/getbalance`
- `send_limit_order(product_code, side, price, size, ttl_ms, tag)` → `/v1/me/sendchildorder` (`child_order_type="LIMIT"`)
- `cancel_order(child_order_acceptance_id)` → `/v1/me/cancelchildorder`

## 5. 戦略ロジック（`src/engine/strategy_stall.py`）
### 5.1 状態モデル（`src/types/dto.py`）
- `BoardSnapshot`: `ts`, `best_bid/ask_price/size`, `best_age_ms`, `spread_ticks`
- `InventoryState`: `side("flat|long|short")`, `size`, `avg_price`, `pnl_today_jpy`, `max_drawdown_jpy`
- `Action`: `kind("place_limit"|"cancel_all_stall"|"close_market")`, `side`, `price`, `size`, `ttl_ms`, `tag`

### 5.2 戦略クラス
`StallThenStrikeStrategy(cfg: StallStrategyConfig, tick_size: Decimal, inventory: InventoryManager)`
- `on_board(board, now) -> tuple[list[Action], DecisionMeta]`
  - Kill スイッチ判定（PnL/DD）
  - TTL 失効チェックで `cancel_all_stall`
  - 逆行チェックで `close_market`
  - Best が `stall_T_ms` 以上静止し `spread_ticks >= min_spread_tick` 且つ 在庫上限未満ならミッド±1tick に `place_limit`（tag="stall", ttl_ms=cfg.ttl_ms, size=cfg.size_min）を両面。
- `on_fill(fill)` / `on_order_event(event)` で在庫とオープン注文を更新。

## 6. 在庫・注文実行
### 6.1 InventoryManager（`src/engine/inventory.py`）
- `apply_fill(fill)` でポジション・平均建値・日次 PnL・DD を更新。
- `pnl_today_jpy`, `max_drawdown_jpy`, `last_pnl_jpy` を保持し、戦略とトレードログに渡す。

### 6.2 OrderExecutor（`src/engine/executor.py`）
- `execute(actions: list[Action]) -> list[FillEvent]`
  - `place_limit` → `BitflyerHttp.send_limit_order`
  - `cancel_all_stall` → tag="stall" で管理する注文を cancel 連発
  - `close_market` → 反対サイドの MARKET もしくは Best±tick の IOC
- 発注/取消結果を `OrderLogger` に書き、返ってきた fill を InventoryManager に渡す。

## 7. ログ設計
### 7.1 ログ種別と目的
- **runtime** (`logs/runtime/app.log` ほか): loguru テキスト。起動/終了、WS 状態、例外、Kill 発火、発注要約。人間が `tail -f` で見る。
- **trades** (`logs/trades/trades-YYYYMMDD.csv`): 1 fill = 1 行。`ts, order_id, side, price, size, fee, strategy, tag, pnl_jpy, pnl_cum_jpy, inventory_after`。PnL 集計・分析用。
- **orders** (`logs/orders/orders-YYYYMMDD.csv`): 1 イベント = 1 行。`ts, action(place/cancel/expire), order_id, client_order_id, side, price, size, tif, ttl_ms, tag, reason`。発注のトレース用。
- **decisions** (`logs/decisions/decisions-YYYYMMDD.csv`): 1 意思決定 = 1 行。`ts, best_age_ms, spread_ticks, inventory_btc, decision_type, entry_bid_px, entry_ask_px, actions, reason`。ロジック検証用。

### 7.2 ロガー実装ポイント
- `logging_runtime.py`: `setup_runtime_logging(log_dir)` で loguru を設定。`app.log` は INFO 以上、必要なら `ws.log` を DEBUG で分離。rotation/retention は 10MB / 7days など。
- `logging_structured.py`: `TradeLogger` / `OrderLogger` / `DecisionLogger`
  - 日付ごとに `*-YYYYMMDD.csv` を開き、ヘッダ付きで追記。
  - `types/dto.py` の `TradeLogRow` / `OrderLogRow` / `DecisionLogRow` を受け取り CSV 1 行を書き込む。
  - 日付が変わったらファイルを切り替える簡易実装で十分。

### 7.3 各レイヤーからの書き込みタイミング
- **TradeLogger**: `executor.execute` → fill 取得 → `InventoryManager.apply_fill` → その結果を `log_trade`。
- **OrderLogger**: `executor.execute` で HTTP を叩いた直後に `log_order`（place/cancel/expire）。
- **DecisionLogger**: `strategy.on_board` の最後で、出した/出さなかったアクションと理由をまとめて `log_decision`。
- **runtime logger**: 起動/終了、WS 接続イベント、例外、Kill スイッチ発火、発注概要。

## 8. アプリ起動フロー
`src/app/run_live.py` / `run_paper.py`（擬似コード）
```python
async def main():
    cfg = load_app_config("configs/base.yml", "configs/live.yml")
    log_root = Path("logs")

    setup_runtime_logging(log_root / "runtime")
    trade_logger = TradeLogger(log_root / "trades", "stall_then_strike")
    order_logger = OrderLogger(log_root / "orders")
    decision_logger = DecisionLogger(log_root / "decisions", "stall_then_strike")

    async with PyBotterSession(cfg, logger) as session:
        await session.connect_ws()
        store = session.get_store()
        client = session.get_client()

        http = BitflyerHttp(client, cfg.exchange)
        ob_view = OrderBookView(store, cfg.exchange.product_code, tick_size=Decimal("1"))
        inventory = InventoryManager(cfg.risk)
        strategy = StallThenStrikeStrategy(cfg.strategy, tick_size=Decimal("1"), inventory=inventory)
        executor = OrderExecutor(http, cfg.exchange, order_logger)

        with store.board.watch() as stream:
            async for change in stream:
                now = datetime.utcnow()
                board = ob_view.snapshot(now)
                actions, decision_meta = strategy.on_board(board, now)

                decision_logger.log_decision(... decision_meta ...)
                fills = await executor.execute(actions)

                for fill in fills:
                    inventory.apply_fill(fill)
                    trade_logger.log_trade(... inventory state ...)
```
- `run_paper.py` では executor をモックに差し替え、実注文なしでロジック検証する。

## 9. テスト方針
- **pybotters 結線テスト**: 実 WS で `store.board.watch()` からイベントが流れてくるか確認。
- **OrderBookView テスト**: ダミー板差分を流し、`best_age_ms` / `spread_ticks` が期待通りか検証。
- **戦略ユニットテスト**: 連続した `BoardSnapshot` を与え、静止→一撃、Best 変更時キャンセル、逆行時ストップが出るかを確認。
- **E2E（紙トレ）**: `run_paper.py` で実板を見つつ、`logs/decisions` と `logs/orders` を確認して挙動をチェック。

## 10. まとめ
- 板・WS・再接続はすべて `pybotters` に任せ、戦略は DataStore が見せる Best/Spread/BestAge を元に最初の一撃だけを狙う。
- ログは用途ごとに runtime（人間向け）、trades / orders / decisions（分析向け）に分離し、後処理しやすい CSV を主軸にする。
- このドキュメント通りに骨組みを実装すれば、即座に開発フェーズに入れる構成になっている。
