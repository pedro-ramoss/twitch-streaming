#!/usr/bin/env bash
set -euo pipefail
powershell.exe -NoProfile -Command 'Disable-ScheduledTask -TaskName "TwitchStreaming-6h" | Out-Null; Write-Host "Agendamento pausado."'
