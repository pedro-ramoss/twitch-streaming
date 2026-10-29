#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DISTRO="${WSL_DISTRO_NAME:-Ubuntu}"
LINUX_USER="$(id -un)"
if ! command -v powershell.exe >/dev/null || ! command -v wslpath >/dev/null; then
  echo "Este instalador deve ser executado no Ubuntu WSL2 com interoperabilidade do Windows habilitada." >&2
  exit 1
fi
WINDOWS_PS1="$(wslpath -w "$ROOT/scripts/register_windows_task.ps1")"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$WINDOWS_PS1" -Distro "$DISTRO" -LinuxUser "$LINUX_USER" -ProjectPath "$ROOT"
