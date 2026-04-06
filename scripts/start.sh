#!/usr/bin/env bash
# Start the Ultra RPi service.
#
# Usage:
#   ./scripts/start.sh              # real hardware (RPi)
#   ./scripts/start.sh --mock       # mock hardware (dev/test)
#   ./scripts/start.sh --systemd    # via systemd (production RPi)
#
# Environment variables:
#   ULTRA_MOCK=1        force mock mode (no STM32/reader hw)
#   ULTRA_CONFIG=<path> override config file
#   ULTRA_LOG_LEVEL=DEBUG  set log level
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# --------------- parse flags ---------------
MODE="foreground"
for arg in "$@"; do
    case "$arg" in
        --mock)
            export ULTRA_MOCK=1
            ;;
        --systemd)
            MODE="systemd"
            ;;
        -h|--help)
            echo "Usage: $0 [--mock] [--systemd]"
            echo ""
            echo "  --mock      Run with mock hardware (no STM32/reader)"
            echo "  --systemd   Start via systemd service"
            echo ""
            echo "Environment:"
            echo "  ULTRA_MOCK=1         Force mock mode"
            echo "  ULTRA_CONFIG=<path>  Override config file"
            exit 0
            ;;
    esac
done

# --------------- systemd mode ---------------
if [ "$MODE" = "systemd" ]; then
    echo "Starting ultra-rpi via systemd..."
    sudo systemctl start ultra-rpi
    sleep 1
    if systemctl is-active --quiet ultra-rpi; then
        echo "ultra-rpi is running."
        echo "  Logs: journalctl -u ultra-rpi -f"
        HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
        echo "  GUI:  http://${HOST_IP:-localhost}:8080"
    else
        echo "Failed to start. Check: journalctl -u ultra-rpi -e"
        exit 1
    fi
    exit 0
fi

# --------------- foreground mode ---------------

VENV_DIR="$PROJECT_DIR/.venv"

# Create venv if it doesn't exist
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    SYSPYTHON=""
    if command -v python3 &>/dev/null; then
        SYSPYTHON="python3"
    elif command -v python &>/dev/null; then
        SYSPYTHON="python"
    else
        echo "ERROR: python3 not found. Install Python >=3.11."
        exit 1
    fi
    "$SYSPYTHON" -m venv "$VENV_DIR"
    echo "Upgrading pip & setuptools..."
    "$VENV_DIR/bin/pip" install --quiet --upgrade pip setuptools wheel
fi

PYTHON="$VENV_DIR/bin/python"

# Install / update the package
if ! "$PYTHON" -c "import ultra" 2>/dev/null; then
    echo "Installing ultra-rpi in dev mode..."
    "$VENV_DIR/bin/pip" install --quiet -e "$PROJECT_DIR"
fi

if [ "${ULTRA_MOCK:-}" = "1" ]; then
    echo "Starting ultra-rpi (MOCK mode)..."
else
    echo "Starting ultra-rpi..."
fi

HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
echo "  GUI will be at http://${HOST_IP:-localhost}:8080"
echo "  Press Ctrl+C to stop."
echo ""

exec "$PYTHON" -m ultra.app
