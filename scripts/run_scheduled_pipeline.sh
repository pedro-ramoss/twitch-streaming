#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export PATH="$ROOT/venv/bin:$PATH"
exec "$ROOT/venv/bin/python" -m scripts.run_pipeline_once
