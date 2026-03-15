#Requires -Version 5.1
<#
.SYNOPSIS
  Start or stop the MCP dashboard server.

.DESCRIPTION
  --startup   Loads .env, checks required env vars, then starts server.py in the background.
  --shutdown  Stops the server process recorded at last startup.

.EXAMPLE
  .\start-stop.ps1 --startup
  .\start-stop.ps1 --shutdown
#>

param(
    [switch] $startup,
    [switch] $shutdown
)

$ErrorActionPreference = "Stop"
$ProjectRoot = (Split-Path -Parent $PSScriptRoot)
$PidFile = Join-Path $ProjectRoot ".mcp-dashboard.pid"
$ServerScript = Join-Path $ProjectRoot "server.py"
$EnvFile = Join-Path $ProjectRoot ".env"

function Load-DotEnv {
    if (-not (Test-Path $EnvFile)) { return }
    Get-Content $EnvFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -and -not $line.StartsWith("#")) {
            $i = $line.IndexOf("=")
            if ($i -gt 0) {
                $name = $line.Substring(0, $i).Trim()
                $value = $line.Substring($i + 1).Trim()
                if ($value -match '^["''](.*)["'']$') { $value = $matches[1] }
                [System.Environment]::SetEnvironmentVariable($name, $value, "Process")
            }
        }
    }
}

function Test-RequiredEnv {
    Load-DotEnv
    $token = [System.Environment]::GetEnvironmentVariable("TE_TOKEN", "Process")
    if (-not $token -or ($token -eq "")) {
        Write-Error "TE_TOKEN is not set. Set it in the environment or in a .env file in the project root."
    }
}

function Start-Dashboard {
    if (-not (Test-Path $ServerScript)) {
        Write-Error "server.py not found at $ServerScript"
    }
    Test-RequiredEnv
    $port = [System.Environment]::GetEnvironmentVariable("PORT", "Process")
    if (-not $port) { $port = "8000" }
    if (Test-Path $PidFile) {
        $oldPid = Get-Content $PidFile -Raw
        $proc = Get-Process -Id $oldPid -ErrorAction SilentlyContinue
        if ($proc) {
            Write-Error "Dashboard may already be running (PID $oldPid). Use --shutdown first, or remove $PidFile"
        }
        Remove-Item $PidFile -Force
    }
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "python"
    $psi.Arguments = "server.py"
    $psi.WorkingDirectory = $ProjectRoot
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $p = [System.Diagnostics.Process]::Start($psi)
    $p.Id | Set-Content $PidFile -NoNewline
    Write-Host "MCP dashboard started in the background (PID $($p.Id))."
    Write-Host "URL: http://127.0.0.1:${port}/"
    Write-Host "To stop: $PSCommandPath --shutdown"

    $baseUrl = "http://127.0.0.1:${port}"
    $maxWait = 30
    $waited = 0
    while ($waited -lt $maxWait) {
        try {
            $r = Invoke-WebRequest -Uri "$baseUrl/" -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
            if ($r.StatusCode -eq 200) { break }
        } catch {}
        Start-Sleep -Seconds 1
        $waited++
    }
    if ($waited -ge $maxWait) {
        Write-Host "Server did not respond within ${maxWait}s. Check .mcp-dashboard.log in project root."
        return
    }

    Write-Host "Waiting for initial data load..."
    $lastMessage = ""
    while ($true) {
        try {
            $status = Invoke-RestMethod -Uri "$baseUrl/api/refresh-status" -Method Get -TimeoutSec 5
        } catch {
            $status = @{ phase = ""; message = ""; current = 0; total = 0; error = "" }
        }
        $msg = $status.message
        if ($msg -and $msg -ne $lastMessage) {
            if ($status.total -and [int]$status.total -gt 0 -and $null -ne $status.current) {
                Write-Host "  [$($status.phase)] $msg ($($status.current)/$($status.total))"
            } else {
                Write-Host "  [$($status.phase)] $msg"
            }
            $lastMessage = $msg
        }
        switch ($status.phase) {
            "done"   { Write-Host "Dashboard ready."; return }
            "error"  { Write-Host "Initial load failed: $($status.error)"; return }
        }
        Start-Sleep -Seconds 2
    }
}

function Stop-Dashboard {
    if (-not (Test-Path $PidFile)) {
        Write-Host "No PID file found. Dashboard may not be running."
        return
    }
    $pidVal = [int](Get-Content $PidFile -Raw)
    $proc = Get-Process -Id $pidVal -ErrorAction SilentlyContinue
    if ($proc) {
        $proc.Kill()
        Write-Host "Stopped MCP dashboard (PID $pidVal)."
    } else {
        Write-Host "Process $pidVal was not running."
    }
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
}

if ($startup) {
    Start-Dashboard
} elseif ($shutdown) {
    Stop-Dashboard
} else {
    Write-Host "Usage: $PSCommandPath --startup | --shutdown"
    exit 1
}
