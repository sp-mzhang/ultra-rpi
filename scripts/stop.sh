#!/usr/bin/env bash
# Stop the Ultra RPi service and disable start-on-boot.
#
# Usage:
#   ./scripts/stop.sh
set -euo pipefail

STOPPED=0

# --------------- systemd: stop & disable ---------------
if systemctl is-active --quiet ultra-rpi 2>/dev/null; then
    echo "Stopping ultra-rpi systemd service..."
    sudo systemctl stop ultra-rpi
    STOPPED=1
fi

if systemctl is-enabled --quiet ultra-rpi 2>/dev/null; then
    echo "Disabling ultra-rpi boot service..."
    sudo systemctl disable ultra-rpi
fi

# --------------- foreground processes ---------------
PIDS=$(pgrep -f 'python.*ultra\.app' 2>/dev/null || true)
if [ -n "$PIDS" ]; then
    echo "Stopping ultra-rpi process(es): $PIDS"
    kill -SIGTERM $PIDS 2>/dev/null || true
    sleep 2
    REMAINING=$(pgrep -f 'python.*ultra\.app' 2>/dev/null || true)
    if [ -n "$REMAINING" ]; then
        echo "Force-killing remaining: $REMAINING"
        kill -9 $REMAINING 2>/dev/null || true
    fi
    STOPPED=1
fi

if [ "$STOPPED" -eq 1 ]; then
    echo "ultra-rpi stopped and boot service disabled."
else
    echo "ultra-rpi is not running."
fi
