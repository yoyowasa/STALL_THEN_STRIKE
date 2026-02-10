$ErrorActionPreference = "Stop"

# Set guard defaults only when values are not already provided.
if (-not $env:ENTRY_GUARD_REQUIRE_CONSISTENCY_OK_CONSECUTIVE) { $env:ENTRY_GUARD_REQUIRE_CONSISTENCY_OK_CONSECUTIVE = "30" }
if (-not $env:ENTRY_GUARD_REQUIRE_COOLDOWN_REMAINING_SEC) { $env:ENTRY_GUARD_REQUIRE_COOLDOWN_REMAINING_SEC = "0" }
if (-not $env:ENTRY_GUARD_REQUIRE_EVENT_LATENCY_MS_P99) { $env:ENTRY_GUARD_REQUIRE_EVENT_LATENCY_MS_P99 = "800" }
if (-not $env:ENTRY_GUARD_REQUIRE_QUEUE_DEPTH_MAX) { $env:ENTRY_GUARD_REQUIRE_QUEUE_DEPTH_MAX = "2000" }
if (-not $env:ENTRY_GUARD_REQUIRE_CA_GATE_BLOCK_RATE) { $env:ENTRY_GUARD_REQUIRE_CA_GATE_BLOCK_RATE = "0.8" }
if (-not $env:ENTRY_GUARD_REQUIRE_ACTIVE_COUNT) { $env:ENTRY_GUARD_REQUIRE_ACTIVE_COUNT = "0" }
if (-not $env:ENTRY_GUARD_REQUIRE_ERRORS) { $env:ENTRY_GUARD_REQUIRE_ERRORS = "0" }

# Prevent duplicate live trader processes unless explicitly allowed.
$allowMulti = ($env:RUN_LIVE_ALLOW_MULTI -as [string])
if ($allowMulti -notin @("1", "true", "TRUE", "yes", "YES")) {
  $existing = Get-CimInstance Win32_Process -Filter "Name='python.exe'" -ErrorAction SilentlyContinue |
    Where-Object {
      ($_.CommandLine -match "C:\\BOT\\stall_then_strike") -and
      ($_.CommandLine -match "src\.app\.run_live")
    }
  if ($existing -and $existing.Count -gt 0) {
    "ALREADY_RUNNING count=$($existing.Count)"
    exit 6
  }
}

# Record run start time and only accept logs created by this run.
$logsRoot = "C:\BOT\stall_then_strike\logs"
$start = Get-Date

# Run live entrypoint with fixed venv python.
$pythonExe = "C:\BOT\stall_then_strike\.venv\Scripts\python.exe"
if (-not (Test-Path $pythonExe)) {
  "PYTHON_NOT_FOUND pythonExe=$pythonExe"
  exit 4
}

$override = if ($env:RUN_LIVE_OVERRIDE) { $env:RUN_LIVE_OVERRIDE } else { "configs/live.yml" }
$durationSec = if ($env:RUN_LIVE_DURATION_SEC) { $env:RUN_LIVE_DURATION_SEC } else { "0" }
$confirm = if ($env:RUN_LIVE_CONFIRM) { $env:RUN_LIVE_CONFIRM } else { "I_UNDERSTAND" }

$proc = Start-Process -FilePath $pythonExe `
  -ArgumentList @("-m", "src.app.run_live", "--override", $override, "--duration-sec", $durationSec, "--confirm", $confirm) `
  -WorkingDirectory "C:\BOT\stall_then_strike" `
  -NoNewWindow -Wait -PassThru
[int]$pyExit = $proc.ExitCode
if ($pyExit -ne 0) {
  "PYTHON_EXIT_CODE=$pyExit"
  exit $pyExit
}

# Search only for logs created in this run.
$log = Get-ChildItem -Path $logsRoot -Recurse -File -ErrorAction SilentlyContinue |
  Where-Object {
    $_.Name -match '^live-\d{8}-\d{6}\.log$' -and
    $_.LastWriteTime -ge $start
  } |
  Sort-Object LastWriteTime -Descending |
  Select-Object -First 1

if (-not $log) {
  "NEW_LOG_NOT_FOUND logs_root=$logsRoot start=$start"
  exit 5
}

# Print key guard lines.
"LOG=$($log.FullName)"
Select-String -Path $log.FullName -Pattern "PROMOTE_GUARD_RESULT|ENTRY_GUARD_ENFORCE_SUMMARY" |
  ForEach-Object { $_.Line }

# Exit non-zero when promote guard fails.
$pr = Select-String -Path $log.FullName -Pattern "PROMOTE_GUARD_RESULT" | Select-Object -Last 1
if ($pr -and ($pr.Line -match "pass=false")) {
  exit 2
}

exit 0
