#!/usr/bin/env bash
# Seed a single machine's settings in S3 and configure the local device.
#
# Usage:
#   ./scripts/seed_machine_settings.sh <machine_name>
#
# Example:
#   ./scripts/seed_machine_settings.sh ultra2
#
# What it does:
#   1. Generates machines/<machine_name>/machine_settings.yaml in S3
#      with device_sn and machine_name updated to match the argument.
#   2. Writes /etc/ultra/machine.yaml on this device so the app picks
#      up the correct device_sn and machine_name at boot.
#
# NOTE: Currently device_sn is set to the machine name (e.g. "ultra2").
#       In the future device_sn will be replaced by a real hardware UID.
#       When that happens, update this script to read the UID from the
#       hardware and use it for device_sn (the S3 key will change too).
#
# Environment:
#   ULTRA_CONFIG_BUCKET  (default: siphox-ultra-config)
#   AWS_DEFAULT_REGION   (default: us-east-2)
set -euo pipefail

# --- Install required packages if missing ---
install_if_missing() {
  if ! command -v "$1" &>/dev/null; then
    echo "$1 not found — installing ..."
    sudo apt-get update -qq
    sudo apt-get install -y -qq "$2"
  fi
}

install_if_missing python3 python3
install_if_missing aws awscli
install_if_missing pip3 python3-pip

# boto3 is needed by the app at runtime
if ! python3 -c "import boto3" &>/dev/null; then
  echo "boto3 not found — installing ..."
  if apt-cache show python3-boto3 &>/dev/null 2>&1; then
    sudo apt-get install -y -qq python3-boto3
  else
    pip3 install --quiet --break-system-packages boto3
  fi
fi

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <machine_name>  (e.g. ultra2)" >&2
  exit 1
fi

MACHINE="$1"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUCKET="${ULTRA_CONFIG_BUCKET:-siphox-ultra-config}"
REGION="${AWS_DEFAULT_REGION:-us-east-2}"
SRC="${ROOT}/config/ultra_default.yaml"
TMP="/tmp/machine_settings_${MACHINE}.yaml"
LOCAL_CFG_DIR="/etc/ultra"
LOCAL_CFG="${LOCAL_CFG_DIR}/machine.yaml"

if [[ ! -f "$SRC" ]]; then
  echo "Missing $SRC" >&2
  exit 1
fi

# Derive a human-friendly display name: ultra2 -> Ultra 2
DISPLAY=$(echo "$MACHINE" | sed -E 's/^ultra([0-9]+)$/Ultra \1/')

# --- Generate the full machine_settings.yaml for S3 ---
python3 - "$SRC" "$MACHINE" "$DISPLAY" "$TMP" << 'PY'
import sys
from pathlib import Path

src, machine, display, out_path = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

lines = Path(src).read_text(encoding="utf-8").splitlines()
skip_comment = "# device_sn: use the real hardware UID in production"

out = [
    "# machine_settings.yaml — per-machine config.",
    f"# device_sn {machine} matches S3 path machines/{machine}/.",
    "# NOTE: device_sn will be a real hardware UID in the future.",
    "",
]
for line in lines:
    s = line.strip()
    if s.startswith(skip_comment):
        continue
    if s.startswith("device_sn:"):
        out.append(f"device_sn: {machine}")
    elif s.startswith("machine_name:"):
        out.append(f"machine_name: {display}")
    else:
        out.append(line)

Path(out_path).write_text("\n".join(out) + "\n", encoding="utf-8")
PY

# --- Upload to S3 ---
echo "Uploading ${TMP} -> s3://${BUCKET}/machines/${MACHINE}/machine_settings.yaml (region ${REGION})"
aws s3 cp "$TMP" \
  "s3://${BUCKET}/machines/${MACHINE}/machine_settings.yaml" \
  --region "$REGION"
echo "S3 seed complete."

# --- Write local /etc/ultra/machine.yaml ---
echo "Writing local config ${LOCAL_CFG} ..."
sudo mkdir -p "$LOCAL_CFG_DIR"
sudo tee "$LOCAL_CFG" > /dev/null << EOF
# Local machine identity — read via ULTRA_CONFIG=/etc/ultra/machine.yaml
# NOTE: device_sn will be a real hardware UID in the future.
device_sn: ${MACHINE}
machine_name: ${DISPLAY}
EOF

# --- Ensure the systemd service picks up ULTRA_CONFIG ---
SERVICE_FILE="/etc/systemd/system/ultra-rpi.service"
if [[ -f "$SERVICE_FILE" ]]; then
  if ! grep -q "ULTRA_CONFIG=" "$SERVICE_FILE"; then
    echo "Adding ULTRA_CONFIG to ${SERVICE_FILE} ..."
    sudo sed -i "/^\[Service\]/a Environment=ULTRA_CONFIG=${LOCAL_CFG}" "$SERVICE_FILE"
    sudo systemctl daemon-reload
    echo "Service updated and daemon reloaded."
  else
    echo "ULTRA_CONFIG already set in ${SERVICE_FILE}."
  fi
else
  echo "No ${SERVICE_FILE} found — add Environment=ULTRA_CONFIG=${LOCAL_CFG} to your service manually."
fi

echo ""
echo "Done: ${MACHINE} seeded in S3 and configured locally."
