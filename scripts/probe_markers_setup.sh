#!/usr/bin/env bash
# scripts/probe_markers_setup.sh
#
# One-shot installer for the carousel-marker bring-up tooling.
# Installs the libdmtx C library + CLI, the python wrapper, and
# OpenCV (for live camera capture). Safe to re-run.
#
# Usage:
#   ./scripts/probe_markers_setup.sh
#
# Targets:
#   - Raspberry Pi OS (bookworm/bullseye) — uses apt
#   - Falls back to pip-only if apt is unavailable
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_DIR="$PROJECT_DIR/.venv"

echo "==> probe_markers_setup: installing libdmtx + python deps"

# ---- system packages (libdmtx native lib + dmtxread CLI) ----
if command -v apt-get >/dev/null 2>&1; then
    if [ "$(id -u)" -ne 0 ]; then
        SUDO="sudo"
    else
        SUDO=""
    fi
    echo "  - apt: libdmtx0b libdmtx-utils python3-opencv"
    $SUDO apt-get update -qq
    $SUDO apt-get install -y --no-install-recommends \
        libdmtx0b libdmtx-utils python3-opencv
else
    echo "  ! apt-get not found -- skipping system packages"
    echo "    install libdmtx + dmtxread manually for your distro"
fi

# ---- python venv (reuse the project's if present) ----
if [ -d "$VENV_DIR" ]; then
    echo "  - using existing venv: $VENV_DIR"
    PIP="$VENV_DIR/bin/pip"
else
    echo "  - using system python3 (--user)"
    PIP="pip3 install --user"
fi

echo "  - pip: pylibdmtx numpy"
$PIP install --quiet pylibdmtx numpy

echo
echo "==> done. Try:"
echo
echo "    # Live overlay on a browser:"
echo "    ./scripts/probe_markers_stream.sh"
echo "    # then open http://<rpi-host>:8765/ from your laptop"
echo
echo "    # Decode one saved image (CLI from libdmtx-utils):"
echo "    dmtxread \"/home/min/Downloads/Image from iOS.jpg\""
