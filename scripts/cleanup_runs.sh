#!/usr/bin/env bash
# Delete all run data and egress database to free disk space.
#
# Usage:
#   ./scripts/cleanup_runs.sh              # prompt before deleting
#   ./scripts/cleanup_runs.sh --yes        # skip confirmation
#   ./scripts/cleanup_runs.sh --restart    # restart service after cleanup
#   ./scripts/cleanup_runs.sh --data-dir /path/to/runs
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$HOME/sway_runs"
YES=0
RESTART=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --yes|-y)       YES=1; shift ;;
        --restart|-r)   RESTART=1; shift ;;
        --data-dir)     DATA_DIR="$2"; shift 2 ;;
        *)
            echo "Unknown option: $1" >&2
            echo "Usage: $0 [--yes] [--restart] [--data-dir PATH]" >&2
            exit 1
            ;;
    esac
done

DATA_DIR="${DATA_DIR/#\~/$HOME}"

if [ ! -d "$DATA_DIR" ]; then
    echo "Data directory does not exist: $DATA_DIR"
    echo "Nothing to clean up."
    exit 0
fi

BEFORE=$(du -sh "$DATA_DIR" 2>/dev/null | cut -f1)
echo "Data directory: $DATA_DIR"
echo "Current size:   $BEFORE"
echo "Disk usage:"
df -h "$DATA_DIR" | tail -1 | awk '{printf "  Used: %s / %s (%s)  Available: %s\n", $3, $2, $5, $4}'
echo ""

if [ "$YES" -eq 0 ]; then
    echo "This will DELETE all run data and the egress database."
    read -rp "Continue? [y/N] " confirm
    if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
        echo "Aborted."
        exit 0
    fi
fi

echo ""
echo "Stopping ultra-rpi..."
"$SCRIPT_DIR/stop.sh" 2>/dev/null || true
sleep 1

echo "Removing all contents of $DATA_DIR..."
rm -rf "${DATA_DIR:?}"/*
rm -rf "${DATA_DIR:?}"/.*  2>/dev/null || true

echo "Recreating empty directory..."
mkdir -p "$DATA_DIR"

AFTER=$(du -sh "$DATA_DIR" 2>/dev/null | cut -f1)
echo ""
echo "Done."
echo "  Before: $BEFORE"
echo "  After:  $AFTER"
echo "Disk usage:"
df -h "$DATA_DIR" | tail -1 | awk '{printf "  Used: %s / %s (%s)  Available: %s\n", $3, $2, $5, $4}'

if [ "$RESTART" -eq 1 ]; then
    echo ""
    echo "Restarting ultra-rpi..."
    "$SCRIPT_DIR/start.sh"
fi
