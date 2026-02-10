param(
  [string]$Override = "configs/live.yml",
  [int]$DurationSec = 0,
  [string]$AlertWebhookUrl = ""
)

$ErrorActionPreference = "Stop"

$root = "C:\BOT\stall_then_strike"
$wrapper = Join-Path $root "run_live_guard_ok.ps1"
if (-not (Test-Path $wrapper)) {
  "WRAPPER_NOT_FOUND path=$wrapper"
  exit 4
}

# Keep caller environment clean by restoring previous values after execution.
$original = @{}
$overrides = @{
  "RUN_LIVE_OVERRIDE" = $Override
  "RUN_LIVE_DURATION_SEC" = [string]$DurationSec
  "RUN_LIVE_CONFIRM" = "I_UNDERSTAND"
  "LIVE_REQUIRE_CONFIRM" = "true"
  "LIVE_CANCEL_ACTIVE_ON_START" = "true"
}

if ($AlertWebhookUrl) {
  $overrides["RUN_LIVE_ALERT_WEBHOOK_URL"] = $AlertWebhookUrl
} elseif ($env:RUN_LIVE_ALERT_WEBHOOK_URL) {
  $overrides["RUN_LIVE_ALERT_WEBHOOK_URL"] = $env:RUN_LIVE_ALERT_WEBHOOK_URL
} elseif ($env:LIVE_ALERT_WEBHOOK_URL) {
  $overrides["RUN_LIVE_ALERT_WEBHOOK_URL"] = $env:LIVE_ALERT_WEBHOOK_URL
}

foreach ($k in $overrides.Keys) {
  $original[$k] = [Environment]::GetEnvironmentVariable($k, "Process")
  [Environment]::SetEnvironmentVariable($k, $overrides[$k], "Process")
}

try {
  $webhookState = if ($overrides.ContainsKey("RUN_LIVE_ALERT_WEBHOOK_URL")) { "enabled" } else { "disabled" }
  "RUN_LIVE_START override=$Override duration_sec=$DurationSec webhook=$webhookState"
  & powershell.exe -NoProfile -ExecutionPolicy Bypass -File $wrapper
  [int]$code = $LASTEXITCODE
  "RUN_LIVE_WRAPPER_EXIT_CODE=$code"
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

