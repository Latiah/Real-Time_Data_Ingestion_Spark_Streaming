#!/usr/bin/env bash
# Runs the Python event generator continuously.
# Usage: ./scripts/generate_events.sh
set -euo pipefail
cd "$(dirname "$0")/.."
source .venv/bin/activate 2>/dev/null || true
python -m src.generator.data_generator
