#!/usr/bin/env bash
# scripts/probe_markers_stream.sh
#
# Convenience wrapper: starts the live MJPEG viewer that
# decodes carousel DataMatrix markers in real time and overlays
# the fused carousel angle on the camera stream.
#
# Usage:
#   ./scripts/probe_markers_stream.sh                 # default port 8765
#   ./scripts/probe_markers_stream.sh --port 9000
#   ./scripts/probe_markers_stream.sh --device /dev/video1
#   ./scripts/probe_markers_stream.sh --width 1920 --height 1080
#   ./scripts/probe_markers_stream.sh --record ~/sway_runs/cam_log
#
# Open in a browser:
#   http://<rpi-host>:8765/
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_PY="$PROJECT_DIR/.venv/bin/python3"

if [ -x "$VENV_PY" ]; then
    PY="$VENV_PY"
else
    PY="python3"
fi

exec "$PY" "$SCRIPT_DIR/probe_markers_stream.py" "$@"
