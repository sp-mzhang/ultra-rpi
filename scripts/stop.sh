#!/usr/bin/env bash
# Stop the Ultra RPi service.
#
# Usage:
#   ./scripts/stop.sh               # stop any running instance
#   ./scripts/stop.sh --systemd     # stop via systemd only
set -euo pipefail

for arg in "$@"; do
    case "$arg" in
        -h|--help)
            echo "Usage: $0 [--systemd]"
            echo ""
            echo "  --systemd   Only stop the systemd service"
            exit 0
            ;;
    esac
done

STOPPED=0

# --------------- systemd service ---------------
if systemctl is-active --quiet ultra-rpi 2>/dev/null; then
    echo "Stopping ultra-rpi systemd service..."
    sudo systemctl stop ultra-rpi
    STOPPED=1
fi

# If --systemd flag, stop here
for arg in "$@"; do
    if [ "$arg" = "--systemd" ]; then
        if [ "$STOPPED" -eq 1 ]; then
            echo "ultra-rpi service stopped."
        else
            echo "ultra-rpi service was not running."
        fi
        exit 0
    fi
done

# --------------- foreground processes ---------------
PIDS=$(pgrep -f 'python.*ultra\.app' 2>/dev/null || true)
if [ -n "$PIDS" ]; then
    echo "Stopping ultra-rpi process(es): $PIDS"
    kill -SIGTERM $PIDS 2>/dev/null || true
    sleep 2
    # Force-kill any remaining
    REMAINING=$(pgrep -f 'python.*ultra\.app' 2>/dev/null || true)
    if [ -n "$REMAINING" ]; then
        echo "Force-killing remaining: $REMAINING"
        kill -9 $REMAINING 2>/dev/null || true
    fi
    STOPPED=1
fi

if [ "$STOPPED" -eq 1 ]; then
    echo "ultra-rpi stopped."
else
    echo "ultra-rpi is not running."
fi
