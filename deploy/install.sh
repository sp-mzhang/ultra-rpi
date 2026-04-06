#!/usr/bin/env bash
# Ultra RPi deployment script for Raspberry Pi.
# Sets up the venv, installs dependencies, and enables the
# systemd service so ultra-rpi starts automatically on boot.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SERVICE_FILE="ultra-rpi.service"
VENV_DIR="$PROJECT_DIR/.venv"

echo "=== Ultra RPi Install ==="

# Create virtual environment if needed
if [ ! -d "$VENV_DIR" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
    echo "Upgrading pip & setuptools..."
    PIP_CONFIG_FILE="$PROJECT_DIR/pip.conf" \
        "$VENV_DIR/bin/pip" install --quiet \
        --upgrade pip setuptools wheel
fi

# Install package
echo "Installing ultra-rpi..."
PIP_CONFIG_FILE="$PROJECT_DIR/pip.conf" \
    "$VENV_DIR/bin/pip" install --quiet \
    --ignore-requires-python -e "$PROJECT_DIR"

# Install systemd service
echo "Installing systemd service..."
sudo cp "$PROJECT_DIR/deploy/$SERVICE_FILE" \
    "/etc/systemd/system/$SERVICE_FILE"
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_FILE"

echo ""
echo "=== Installation complete ==="
echo ""
echo "The service will start automatically on next boot."
echo ""
echo "Start now:"
echo "  sudo systemctl start ultra-rpi"
echo ""
echo "View logs:"
echo "  journalctl -u ultra-rpi -f"
echo ""
HOST_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
echo "Access GUI:"
echo "  http://${HOST_IP:-localhost}:8080"
