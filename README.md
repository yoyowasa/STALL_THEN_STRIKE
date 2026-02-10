# stall_then_strike (pybotters版)

bitFlyer Crypto CFD `FX_BTC_JPY` 向けの「Best が静止したら両面で一撃だけ取りに行く」シンプルな MM 戦略用リポジトリです。板・WS 周りはすべて `pybotters` の `Client` / `bitFlyerDataStore` に任せ、こちら側は DataStore が見せる板情報を使って意思決定・発注・ログ記録に集中します。

## 使い方（イメージ）
- `poetry install` で依存を入れる（Python 3.11 前提）。
- `.env` を `.env.example` から複製し API キーをセット。
- `configs/base.yml` を基礎に `configs/live.yml` / `configs/paper.yml` で環境を上書き。
- ペーパー: `python -m src.app.run_paper --override configs/paper.yml`
- リアル口座: `python -m src.app.run_live --override configs/live.yml --duration-sec 0`
- ペーパー運用ラッパ: `powershell -File .\\run_paper_guard_ok.ps1`
- リアル運用ラッパ: `powershell -File .\\run_live_guard_ok.ps1`

### 補足（live）
- `run_live` は `--duration-sec 0`（既定値）で無期限実行。
- `run_live` の `mode=trade` は確認トークン必須（既定）。`--confirm I_UNDERSTAND` を付与するか、`LIVE_REQUIRE_CONFIRM=false` を設定。
- 起動時に `ACTIVE` 注文を回収してキャンセルする（既定）。無効化する場合は `LIVE_CANCEL_ACTIVE_ON_START=false` を設定。
- 終了時の成行クローズは再試行する（既定3回）。`LIVE_CLOSE_MAX_RETRY` / `LIVE_CLOSE_RETRY_WAIT_SEC` で調整可能。
- `run_live_guard_ok.ps1` は `RUN_LIVE_DURATION_SEC` / `RUN_LIVE_OVERRIDE` / `RUN_LIVE_CONFIRM` で起動引数を上書き可能（`RUN_LIVE_CONFIRM` 未指定時は `I_UNDERSTAND` を既定使用）。既定で二重起動防止を行い、必要な場合のみ `RUN_LIVE_ALLOW_MULTI=true` で無効化。
- liveログはローテーション/保持を有効化（既定: `LIVE_LOG_ROTATION=200 MB`, `LIVE_LOG_RETENTION=14 days`, `LIVE_LOG_COMPRESSION=zip`）。

## ディレクトリ
- `configs/` … YAML 設定
- `src/` … 設定ローダー、pybotters セッション、戦略、実行器、型定義
- `logs/` … runtime（人間向けテキスト）/ trades / orders / decisions の構造化ログ
- `data/` … 生データや結果を残したい場合の置き場

詳細なアーキテクチャとログ設計は `DESIGN.md` を参照してください。
