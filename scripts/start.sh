#!/usr/bin/env bash
# Start the Ultra RPi service.
#
# Usage:
#   ./scripts/start.sh              # setup, enable on boot, start
#   ./scripts/start.sh --mock       # same but with mock hardware
#   ./scripts/start.sh --fg         # foreground only (no systemd)
#
# Environment variables:
#   ULTRA_MOCK=1        force mock mode (no STM32/reader hw)
#   ULTRA_CONFIG=<path> override config file
#   ULTRA_LOG_LEVEL=DEBUG  set log level
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
VENV_DIR="$PROJECT_DIR/.venv"
SERVICE_FILE="ultra-rpi.service"

# --------------- parse flags ---------------
MODE="service"
for arg in "$@"; do
    case "$arg" in
        --mock)    export ULTRA_MOCK=1 ;;
        --fg)      MODE="foreground" ;;
        -h|--help)
            echo "Usage: $0 [--mock] [--fg]"
            echo ""
            echo "  (default)   Setup venv, install systemd, enable"
            echo "              on boot, and start the service now"
            echo "  --mock      Use mock hardware (no STM32/reader)"
            echo "  --fg        Run in foreground only (no systemd)"
            echo ""
            echo "Environment:"
            echo "  ULTRA_MOCK=1         Force mock mode"
            echo "  ULTRA_CONFIG=<path>  Override config file"
            exit 0
            ;;
    esac
done

# --------------- ensure venv & deps ---------------
setup_venv() {
    if [ ! -d "$VENV_DIR" ]; then
        echo "Creating virtual environment..."
        SYSPYTHON=""
        if command -v python3 &>/dev/null; then
            SYSPYTHON="python3"
        elif command -v python &>/dev/null; then
            SYSPYTHON="python"
        else
            echo "ERROR: python3 not found."
            exit 1
        fi
        echo "Using $SYSPYTHON ($("$SYSPYTHON" --version))"
        "$SYSPYTHON" -m venv "$VENV_DIR"
        echo "Upgrading pip & setuptools..."
        PIP_CONFIG_FILE="$PROJECT_DIR/pip.conf" \
            "$VENV_DIR/bin/pip" install --quiet \
            --upgrade pip setuptools wheel
    fi

    if ! "$VENV_DIR/bin/python" -c "import ultra" 2>/dev/null; then
        echo "Installing ultra-rpi..."
        PIP_CONFIG_FILE="$PROJECT_DIR/pip.conf" \
            "$VENV_DIR/bin/pip" install --quiet \
            --ignore-requires-python -e "$PROJECT_DIR"
    fi

    if ! "$VENV_DIR/bin/python" -c \
        "import siphox.analysis_tools" 2>/dev/null; then
        echo "Installing analysis-tools (reader pipeline)..."
        PIP_CONFIG_FILE="$PROJECT_DIR/pip.conf" \
            "$VENV_DIR/bin/pip" install --quiet \
            --ignore-requires-python \
            "analysis-tools @ git+ssh://git@github.com/siphox-inc/sway.git@main#subdirectory=analysis_tools" \
            || echo "WARNING: analysis-tools install failed." \
               " Peak detection will be disabled."
    fi
}

setup_venv

# --------------- service mode (default) ---------------
if [ "$MODE" = "service" ]; then
    UNIT_SRC="$PROJECT_DIR/deploy/$SERVICE_FILE"
    if [ ! -f "$UNIT_SRC" ]; then
        echo "ERROR: $UNIT_SRC not found."
        exit 1
    fi

    echo "Installing systemd service..."
    sudo cp "$UNIT_SRC" "/etc/systemd/system/$SERVICE_FILE"
    sudo systemctl daemon-reload
    sudo systemctl enable "$SERVICE_FILE"
    sudo systemctl restart "$SERVICE_FILE"

    sleep 1
    if systemctl is-active --quiet ultra-rpi; then
        echo ""
        echo "ultra-rpi is running and will start on boot."
        echo "  Logs: journalctl -u ultra-rpi -f"
        HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
        echo "  GUI:  http://${HOST_IP:-localhost}:8080"
    else
        echo "Service installed but failed to start."
        echo "  Check: journalctl -u ultra-rpi -e"
        exit 1
    fi
    exit 0
fi

# --------------- foreground mode (--fg) ---------------
PYTHON="$VENV_DIR/bin/python"

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
