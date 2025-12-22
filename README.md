# stall_then_strike (pybotters版)

bitFlyer Crypto CFD `FX_BTC_JPY` 向けの「Best が静止したら両面で一撃だけ取りに行く」シンプルな MM 戦略用リポジトリです。板・WS 周りはすべて `pybotters` の `Client` / `bitFlyerDataStore` に任せ、こちら側は DataStore が見せる板情報を使って意思決定・発注・ログ記録に集中します。

## 使い方（イメージ）
- `poetry install` で依存を入れる（Python 3.11 前提）。
- `.env` を `.env.example` から複製し API キーをセット。
- `configs/base.yml` を基礎に `configs/live.yml` / `configs/paper.yml` で環境を上書き。
- リアル口座: `python -m src.app.run_live`（予定実装）
- ペーパー: `python -m src.app.run_paper`（予定実装）

## ディレクトリ
- `configs/` … YAML 設定
- `src/` … 設定ローダー、pybotters セッション、戦略、実行器、型定義
- `logs/` … runtime（人間向けテキスト）/ trades / orders / decisions の構造化ログ
- `data/` … 生データや結果を残したい場合の置き場

詳細なアーキテクチャとログ設計は `DESIGN.md` を参照してください。
# STALL_THEN_STRIKE
