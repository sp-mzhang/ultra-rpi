#!/usr/bin/env bash
# Ultra RPi deployment script for Raspberry Pi.
# Installs the application to /opt/ultra-rpi with a
# virtual environment and enables the systemd service.
set -euo pipefail

INSTALL_DIR="/opt/ultra-rpi"
SERVICE_FILE="ultra-rpi.service"

echo "=== Ultra RPi Install ==="

# Create install directory
sudo mkdir -p "$INSTALL_DIR"
sudo chown pi:pi "$INSTALL_DIR"

# Copy source
rsync -a --exclude='.git' --exclude='__pycache__' \
    --exclude='.venv' --exclude='*.pyc' \
    "$(dirname "$0")/../" "$INSTALL_DIR/"

# Create virtual environment
if [ ! -d "$INSTALL_DIR/.venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv "$INSTALL_DIR/.venv"
fi

# Install package
echo "Installing ultra-rpi..."
"$INSTALL_DIR/.venv/bin/pip" install --quiet --upgrade pip
"$INSTALL_DIR/.venv/bin/pip" install --quiet -e "$INSTALL_DIR"

# Install systemd service
echo "Installing systemd service..."
sudo cp "$INSTALL_DIR/deploy/$SERVICE_FILE" \
    "/etc/systemd/system/$SERVICE_FILE"
sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_FILE"

echo "=== Installation complete ==="
echo ""
echo "Start the service:"
echo "  sudo systemctl start ultra-rpi"
echo ""
echo "View logs:"
echo "  journalctl -u ultra-rpi -f"
echo ""
echo "Access GUI:"
echo "  http://$(hostname -I | awk '{print $1}'):8080"
