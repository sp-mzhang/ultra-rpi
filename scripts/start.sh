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

# --------------- ensure uv ---------------
ensure_uv() {
    if command -v uv &>/dev/null; then
        return
    fi
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.local/bin:$PATH"
    if ! command -v uv &>/dev/null; then
        echo "ERROR: uv install failed."
        exit 1
    fi
    echo "uv $(uv --version) installed"
}

# --------------- sync environment ---------------
setup_env() {
    ensure_uv
    echo "Syncing environment with uv..."
    cd "$PROJECT_DIR"
    UV_NO_PROGRESS=1 NO_COLOR=1 \
        uv sync \
        --upgrade-package analysis-tools \
        --upgrade-package dollopclient 2>&1

    # OpenCV: prefer system apt package, fall back to pip
    if ! "$VENV_DIR/bin/python" -c "import cv2" 2>/dev/null; then
        echo "Installing OpenCV into venv..."
        SITE_CV2=$(python3 -c \
            "import cv2; print(cv2.__file__)" 2>/dev/null || true)
        if [ -n "$SITE_CV2" ]; then
            SITE_DIR=$(dirname "$SITE_CV2")
            VENV_SP=$("$VENV_DIR/bin/python" -c \
                "import site; print(site.getsitepackages()[0])")
            ln -sfn "$SITE_DIR" "$VENV_SP/cv2"
            echo "Linked system cv2 -> $VENV_SP/cv2"
        else
            "$VENV_DIR/bin/pip" install \
                opencv-python-headless 2>&1
        fi
    fi
    echo "Environment ready."
}

setup_env

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
