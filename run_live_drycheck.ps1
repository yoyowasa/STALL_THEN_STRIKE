param(
  [int]$DurationSec = 20,
  [string]$Override = "configs/live.yml"
)

$ErrorActionPreference = "Stop"

$root = "C:\BOT\stall_then_strike"
$wrapper = Join-Path $root "run_live_guard_ok.ps1"
if (-not (Test-Path $wrapper)) {
  "WRAPPER_NOT_FOUND path=$wrapper"
  exit 4
}

# Keep caller environment clean by restoring previous values after the check.
$original = @{}
$overrides = @{
  "RUN_LIVE_DURATION_SEC" = [string]$DurationSec
  "RUN_LIVE_OVERRIDE" = $Override
  "RUN_LIVE_CONFIRM" = "I_UNDERSTAND"
  "LIVE_REQUIRE_CONFIRM" = "true"
  "LIVE_ALERT_ENABLED" = "false"
  "LIVE_CANCEL_ACTIVE_ON_START" = "false"
  "ENTRY_GUARD_REQUIRE_CONSISTENCY_OK_CONSECUTIVE" = "999999"
  "ENTRY_GUARD_REQUIRE_COOLDOWN_REMAINING_SEC" = "0"
  "ENTRY_GUARD_REQUIRE_EVENT_LATENCY_MS_P99" = "999999"
  "ENTRY_GUARD_REQUIRE_QUEUE_DEPTH_MAX" = "999999999"
  "ENTRY_GUARD_REQUIRE_CA_GATE_BLOCK_RATE" = "1.0"
  "ENTRY_GUARD_REQUIRE_ACTIVE_COUNT" = "0"
  "ENTRY_GUARD_REQUIRE_ERRORS" = "0"
}

foreach ($k in $overrides.Keys) {
  $original[$k] = [Environment]::GetEnvironmentVariable($k, "Process")
  [Environment]::SetEnvironmentVariable($k, $overrides[$k], "Process")
}

try {
  & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $wrapper
  [int]$code = $LASTEXITCODE
  "DRYCHECK_EXIT_CODE=$code"

  if ($code -eq 2) {
    "DRYCHECK_OK expected guard block observed (pass=false)."
    exit 0
  }
  if ($code -eq 0) {
    "DRYCHECK_UNEXPECTED_PASS guard passed. review thresholds/logs."
    exit 7
  }

  "DRYCHECK_FAILED wrapper_exit=$code"
  exit $code
}
finally {
  foreach ($k in $overrides.Keys) {
    $prev = $original[$k]
    if ($null -eq $prev) {
      Remove-Item "Env:$k" -ErrorAction SilentlyContinue
    } else {
      [Environment]::SetEnvironmentVariable($k, $prev, "Process")
    }
  }
}
