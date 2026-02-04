# このスクリプトが何をするか: ENTRY_GUARD の require 環境変数をセットして paper を起動する
if (-not $env:ENTRY_GUARD_REQUIRE_CONSISTENCY_OK_CONSECUTIVE) { $env:ENTRY_GUARD_REQUIRE_CONSISTENCY_OK_CONSECUTIVE="0" }
$env:ENTRY_GUARD_REQUIRE_COOLDOWN_REMAINING_SEC="999999"
$env:ENTRY_GUARD_REQUIRE_EVENT_LATENCY_MS_P99="999999"
$env:ENTRY_GUARD_REQUIRE_QUEUE_DEPTH_MAX="999999999"
$env:ENTRY_GUARD_REQUIRE_CA_GATE_BLOCK_RATE="1.0"
$env:ENTRY_GUARD_REQUIRE_ACTIVE_COUNT="0"
$env:ENTRY_GUARD_REQUIRE_ERRORS="0"

# このスクリプトが何をするか: ログ探索の基点と、今回runの開始時刻を決める
$logs_root="C:\BOT\stall_then_strike\logs"
$start=Get-Date

# このスクリプトが何をするか: paper を起動する
python run_paper.py

# このスクリプトが何をするか: 今回の実行で更新された paper 本体ログを再帰検索で拾う（無ければ全体の最新にフォールバック）
$log = Get-ChildItem -Path $logs_root -Recurse -File -ErrorAction SilentlyContinue |
  Where-Object { $_.Name -match '^paper-\d{8}-\d{6}\.log$' -and $_.LastWriteTime -ge $start } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

if (-not $log) {
  $log = Get-ChildItem -Path $logs_root -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -match '^paper-\d{8}-\d{6}\.log$' } |
    Sort-Object LastWriteTime -Descending |
    Select-Object -First 1
}

# このスクリプトが何をするか: ログが見つからない場合は明示して異常終了する（Path=null を防ぐ）
if (-not $log) {
  "LOG_NOT_FOUND logs_root=$logs_root"
  exit 3
}

# このスクリプトが何をするか: 最新の判定結果を表示して、pass=false を見逃さない
"LOG="+$log.FullName
Select-String -Path $log.FullName -Pattern "PROMOTE_GUARD_RESULT|ENTRY_GUARD_ENFORCE_SUMMARY" | ForEach-Object { $_.Line }

# このスクリプトが何をするか: pass=false のときは exit 2 で異常終了させる
$pr = Select-String -Path $log.FullName -Pattern "PROMOTE_GUARD_RESULT" | Select-Object -Last 1
if ($pr -and ($pr.Line -match 'pass=false')) { exit 2 }
