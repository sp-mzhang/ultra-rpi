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
    if timeout 180 env UV_NO_PROGRESS=1 NO_COLOR=1 \
        uv sync \
        --upgrade-package analysis-tools \
        --upgrade-package dollopclient 2>&1; then
        echo "uv sync succeeded."
    else
        echo "WARNING: uv sync failed or timed out — using existing venv."
    fi

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

    # bluezero BLE provisioning needs python3-dbus + python3-gi,
    # which are apt-only C-extension packages. `uv sync` will
    # install the pure-python bluezero wheel, but importing it
    # fails inside the venv because the dbus/gi bindings live in
    # the system site-packages. Install the apt deps (idempotent)
    # and symlink them into the venv, matching the cv2 approach.
    need_apt=""
    for pkg in python3-dbus python3-gi bluez; do
        if ! dpkg -s "$pkg" >/dev/null 2>&1; then
            need_apt+=" $pkg"
        fi
    done
    if [ -n "$need_apt" ]; then
        echo "Installing missing apt packages for BLE:$need_apt"
        sudo apt-get update -qq
        # shellcheck disable=SC2086
        sudo apt-get install -y $need_apt
    fi

    VENV_SP=$("$VENV_DIR/bin/python" -c \
        "import site; print(site.getsitepackages()[0])")
    link_system_module() {
        local modname="$1"
        if "$VENV_DIR/bin/python" -c "import $modname" 2>/dev/null; then
            return
        fi
        local src
        src=$(python3 -c \
            "import $modname, os; p=$modname.__file__; print(os.path.dirname(p) if os.path.basename(p)=='__init__.py' else p)" \
            2>/dev/null || true)
        if [ -n "$src" ] && [ -e "$src" ]; then
            ln -sfn "$src" "$VENV_SP/$(basename "$src")"
            echo "Linked system $modname -> $VENV_SP/$(basename "$src")"
        else
            echo "WARNING: $modname not found in system python; BLE may be unavailable."
        fi
    }
    link_system_module dbus
    # gi is PyGObject; also pull _dbus_bindings / _dbus_glib_bindings
    link_system_module gi
    for mod in _dbus_bindings _dbus_glib_bindings dbus_python-*.dist-info; do
        for f in /usr/lib/python3/dist-packages/$mod*; do
            [ -e "$f" ] || continue
            ln -sfn "$f" "$VENV_SP/$(basename "$f")"
        done
    done

    # Final sanity check -- surface any remaining problem loudly.
    if ! "$VENV_DIR/bin/python" -c \
        "from bluezero import adapter, peripheral" 2>/dev/null; then
        echo "WARNING: bluezero still not importable from venv."
        echo "  Reproduce with:"
        echo "    $VENV_DIR/bin/python -c 'from bluezero import adapter, peripheral'"
    else
        echo "BLE stack imports OK (bluezero + dbus)."
    fi

    echo "Environment ready."
}

setup_env

# --------------- sync analysis-model-store vendored code ---------------
AMS_REPO="ssh://git@github.com/siphox-inc/analysis-model-store.git"
AMS_CACHE="$PROJECT_DIR/.cache/analysis-model-store"
AMS_SRC="$AMS_CACHE/src/analyses/assay_3rd_party_validation/src"
AMS_DST="$PROJECT_DIR/lib/assay_validation"
AMS_FILES=(demo demo_helpers analysis analysis_plots validation_lib
           fitting_lib helpers errors_lib compat_lib uihelpers)

sync_ams() {
    echo "Syncing analysis validation files from ams..."
    if [ -d "$AMS_CACHE/.git" ]; then
        git -C "$AMS_CACHE" fetch --depth 1 origin main 2>&1 \
            && git -C "$AMS_CACHE" reset --hard origin/main 2>&1 \
            || echo "WARNING: ams git pull failed — using cached copy."
    else
        mkdir -p "$(dirname "$AMS_CACHE")"
        git clone --depth 1 --branch main "$AMS_REPO" "$AMS_CACHE" 2>&1 || {
            echo "WARNING: Failed to clone analysis-model-store."
            return
        }
    fi
    mkdir -p "$AMS_DST"
    for f in "${AMS_FILES[@]}"; do
        cp "$AMS_SRC/${f}.py" "$AMS_DST/" 2>/dev/null || true
    done
    echo "Analysis validation files synced."
}

sync_ams

# --------------- service mode (default) ---------------
if [ "$MODE" = "service" ]; then
    CURRENT_USER="$(whoami)"

    echo "Installing systemd service (User=$CURRENT_USER)..."
    sudo tee "/etc/systemd/system/$SERVICE_FILE" > /dev/null << UNIT
[Unit]
Description=Ultra RPi Controller
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$CURRENT_USER
WorkingDirectory=$PROJECT_DIR
Environment=ULTRA_CONFIG=/etc/ultra/machine.yaml
Environment=PYTHONPATH=$PROJECT_DIR/lib/assay_validation
ExecStart=$VENV_DIR/bin/python -m ultra.app
Restart=on-failure
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
UNIT
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

export PYTHONPATH="$PROJECT_DIR/lib/assay_validation${PYTHONPATH:+:$PYTHONPATH}"
exec "$PYTHON" -m ultra.app
