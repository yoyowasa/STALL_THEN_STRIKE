$ErrorActionPreference = "Stop"

# Set relaxed guard thresholds for this wrapper run.
if (-not $env:ENTRY_GUARD_REQUIRE_CONSISTENCY_OK_CONSECUTIVE) {
  $env:ENTRY_GUARD_REQUIRE_CONSISTENCY_OK_CONSECUTIVE = "0"
}
$env:ENTRY_GUARD_REQUIRE_COOLDOWN_REMAINING_SEC = "999999"
$env:ENTRY_GUARD_REQUIRE_EVENT_LATENCY_MS_P99 = "999999"
$env:ENTRY_GUARD_REQUIRE_QUEUE_DEPTH_MAX = "999999999"
$env:ENTRY_GUARD_REQUIRE_CA_GATE_BLOCK_RATE = "1.0"
$env:ENTRY_GUARD_REQUIRE_ACTIVE_COUNT = "0"
$env:ENTRY_GUARD_REQUIRE_ERRORS = "0"

# Capture start time before running paper, then only accept logs newer than this.
$logsRoot = "C:\BOT\stall_then_strike\logs"
$start = Get-Date

# Run paper with fixed venv python to avoid system python dependency issues.
$pythonExe = "C:\BOT\stall_then_strike\.venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
  "PYTHON_NOT_FOUND pythonExe=$pythonExe"
  exit 4
}

$proc = Start-Process -FilePath $pythonExe `
  -ArgumentList @("-m", "src.app.run_paper") `
  -WorkingDirectory "C:\BOT\stall_then_strike" `
  -NoNewWindow -Wait -PassThru
[int]$pyExit = $proc.ExitCode
if ($pyExit -ne 0) {
  "PYTHON_EXIT_CODE=$pyExit"
  exit $pyExit
}

# Find the paper log created in this run only. Do not fall back to old logs.
$log = Get-ChildItem -Path $logsRoot -Recurse -File -ErrorAction SilentlyContinue |
  Where-Object {
    $_.Name -match '^paper-\d{8}-\d{6}\.log$' -and
    $_.LastWriteTime -ge $start
  } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

if (-not $log) {
  "NEW_LOG_NOT_FOUND logs_root=$logsRoot start=$start"
  exit 5
}

# Print guard lines from this run log.
"LOG=$($log.FullName)"
Select-String -Path $log.FullName -Pattern "PROMOTE_GUARD_RESULT|ENTRY_GUARD_ENFORCE_SUMMARY" |
  ForEach-Object { $_.Line }

# Fail when promote guard reports pass=false.
$pr = Select-String -Path $log.FullName -Pattern "PROMOTE_GUARD_RESULT" | Select-Object -Last 1
if ($pr -and ($pr.Line -match "pass=false")) {
  exit 2
}

exit 0
