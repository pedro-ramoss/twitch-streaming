param(
    [Parameter(Mandatory=$true)][string]$Distro,
    [Parameter(Mandatory=$true)][string]$LinuxUser,
    [Parameter(Mandatory=$true)][string]$ProjectPath
)
$ErrorActionPreference = 'Stop'
$taskName = 'TwitchStreaming-6h'
$exe = Join-Path $env:WINDIR 'System32\wsl.exe'
$script = "$($ProjectPath.TrimEnd('/'))/scripts/run_scheduled_pipeline.sh"
$args = "--distribution `"$Distro`" --user `"$LinuxUser`" --exec /bin/bash `"$script`""
$action = New-ScheduledTaskAction -Execute $exe -Argument $args
# Quatro execucoes por dia, seguindo o horario local do Windows.
$trigger = New-ScheduledTaskTrigger -Once -At ([DateTime]::Today) -RepetitionInterval (New-TimeSpan -Hours 6) -RepetitionDuration (New-TimeSpan -Days 365)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Hours 3)
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Description 'Twitch Streaming: coleta + PySpark + PostgreSQL a cada 6 horas pelo WSL2' -Force | Out-Null
Write-Host "Agendamento criado: $taskName (00:00, 06:00, 12:00, 18:00; hora do Windows)."
Write-Host "WSL: $Distro | usuario: $LinuxUser | projeto: $ProjectPath"
Write-Host "Para confirmar: Get-ScheduledTask -TaskName '$taskName'"
