#!/usr/bin/env bash
# Provision a new Ultra device: credentials, IoT certs, machine identity.
#
# Run this directly on the target Raspberry Pi; it needs sudo for the
# /etc/ultra/* writes and systemd patch, so the same shell session must
# be able to sudo without a password prompt interrupting the script.
#
# Usage:
#   ./scripts/seed_machine_settings.sh <device_sn> [machine_name]
#
# Examples:
#   # Fleet-provisioning UID (preferred for new units):
#   ./scripts/seed_machine_settings.sh \
#     DEV-1e10e696-cfec-4e5c-ac06-b2951a20aa8a
#
#   # Legacy short-name (back-compat for existing fleet):
#   ./scripts/seed_machine_settings.sh ultra4
#
#   # Override the human display label:
#   ./scripts/seed_machine_settings.sh \
#     DEV-1e10e696-cfec-4e5c-ac06-b2951a20aa8a "Ultra 4 (dev)"
#
# device_sn rules:
#   * any string; becomes the S3 key at s3://${BUCKET}/machines/<device_sn>/.
#   * preferred format: the fleet-provisioning UID (DEV-<uuid>) baked
#     into the hardware, so the S3 path tracks the unit for its lifetime.
#   * legacy ultraN values still work; machine_name auto-derives to
#     "Ultra N" unless the second positional arg overrides it.
#
# What it does (in order):
#   1. Installs required packages (python3, awscli, boto3).
#   2. Writes ~/.aws/credentials and ~/.aws/config if missing.
#   3. Copies IoT fleet-provisioning certs to /etc/ultra/certs/.
#   4. Generates machines/<device_sn>/machine_settings.yaml in S3.
#   5. Writes /etc/ultra/machine.yaml for local identity.
#   6. Uploads calibration data from config/calibration_data/ to S3.
#   7. Patches the systemd service if needed.
#
# Environment:
#   ULTRA_CONFIG_BUCKET  (default: siphox-ultra-config)
#   AWS_DEFAULT_REGION   (default: us-east-2)
#   DEVICE_SN            (alternative to positional arg)
#   MACHINE_NAME         (alternative to second positional arg)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"

# ------------------------------------------------------------------ #
# 1. Install required packages if missing                            #
# ------------------------------------------------------------------ #
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

if ! python3 -c "import boto3" &>/dev/null; then
  echo "boto3 not found — installing ..."
  if apt-cache show python3-boto3 &>/dev/null 2>&1; then
    sudo apt-get install -y -qq python3-boto3
  else
    pip3 install --quiet --break-system-packages boto3
  fi
fi

# ------------------------------------------------------------------ #
# 2. AWS credentials (~/.aws)                                        #
# ------------------------------------------------------------------ #
AWS_DIR="${HOME}/.aws"
AWS_CREDS="${AWS_DIR}/credentials"
AWS_CONFIG="${AWS_DIR}/config"

PROV_AWS_CREDS="${ROOT}/provisioning/aws_credentials"
PROV_AWS_CONFIG="${ROOT}/provisioning/aws_config"

if [[ -f "$AWS_CREDS" ]]; then
  echo "AWS credentials already present at ${AWS_CREDS} — skipping."
else
  echo "=== AWS Credentials Setup ==="

  if [[ -f "$PROV_AWS_CREDS" ]]; then
    echo "Using cached credentials from ${PROV_AWS_CREDS}"
  else
    echo "No cached credentials found."
    echo "Enter AWS credentials (get these from an existing machine or your admin):"
    read -rp "  AWS Access Key ID: " AWS_KEY
    read -rp "  AWS Secret Access Key: " AWS_SECRET
    if [[ -z "$AWS_KEY" || -z "$AWS_SECRET" ]]; then
      echo "[ERROR] Both key and secret are required."
      exit 1
    fi
    mkdir -p "$(dirname "$PROV_AWS_CREDS")"
    cat > "${PROV_AWS_CREDS}" << GENCREDS
[default]
aws_access_key_id = ${AWS_KEY}
aws_secret_access_key = ${AWS_SECRET}
GENCREDS
    chmod 600 "${PROV_AWS_CREDS}"
    echo "[OK] Saved to ${PROV_AWS_CREDS} (gitignored, reused on next run)."
  fi

  mkdir -p "${AWS_DIR}"
  chmod 700 "${AWS_DIR}"

  cp "${PROV_AWS_CREDS}" "${AWS_CREDS}"
  chmod 600 "${AWS_CREDS}"

  if [[ -f "$PROV_AWS_CONFIG" ]]; then
    cp "${PROV_AWS_CONFIG}" "${AWS_CONFIG}"
  else
    cat > "${AWS_CONFIG}" << 'AWSCONFIG'
[default]
region = us-east-2
AWSCONFIG
  fi
  chmod 600 "${AWS_CONFIG}"

  echo "[OK] AWS credentials written to ${AWS_CREDS}"
fi

# ------------------------------------------------------------------ #
# 3. IoT fleet-provisioning certificates -> /etc/ultra/certs/        #
# ------------------------------------------------------------------ #
CERTS_DIR="${ROOT}/provisioning/certs"
IOT_DEST="/etc/ultra/certs"
CURRENT_USER="$(logname 2>/dev/null || whoami)"

REQUIRED_CERTS=(
  "claim.cert.pem"
  "claim.private.key"
  "root-CA.crt"
)

ALL_CERTS_PRESENT=true
for f in "${REQUIRED_CERTS[@]}"; do
  if [[ ! -f "${CERTS_DIR}/${f}" ]]; then
    ALL_CERTS_PRESENT=false
    break
  fi
done

if $ALL_CERTS_PRESENT; then
  echo "=== IoT Provisioning Certs ==="
  sudo mkdir -p "${IOT_DEST}"
  for f in "${REQUIRED_CERTS[@]}"; do
    sudo cp "${CERTS_DIR}/${f}" "${IOT_DEST}/${f}"
  done
  sudo chmod 644 "${IOT_DEST}/claim.cert.pem"
  sudo chmod 600 "${IOT_DEST}/claim.private.key"
  sudo chmod 644 "${IOT_DEST}/root-CA.crt"
  sudo chown -R "${CURRENT_USER}:${CURRENT_USER}" "${IOT_DEST}"
  echo "[OK] Certs installed to ${IOT_DEST}"
else
  echo "IoT certs not found in ${CERTS_DIR} — skipping."
  echo "  Place claim.cert.pem, claim.private.key, root-CA.crt there and re-run."
fi

# ------------------------------------------------------------------ #
# 4-6. Machine identity (requires <device_sn> argument)              #
# ------------------------------------------------------------------ #
#
# Accept device_sn from $1 or $DEVICE_SN; machine_name from $2 or
# $MACHINE_NAME; otherwise auto-derive. Auto-derivation:
#   * ultraN  -> "Ultra N"          (legacy fleet convention)
#   * DEV-*   -> "DEV <first UUID chunk>"  (readable from logs)
#   * anything else -> the device_sn itself (operator can override)

DEVICE_SN="${1:-${DEVICE_SN:-}}"
MACHINE_NAME_ARG="${2:-${MACHINE_NAME:-}}"

if [[ -z "$DEVICE_SN" ]]; then
  echo ""
  echo "Usage: $0 <device_sn> [machine_name]" >&2
  echo "  e.g. $0 DEV-1e10e696-cfec-4e5c-ac06-b2951a20aa8a" >&2
  echo "  or   $0 ultra4" >&2
  echo "  (Steps 1-3 completed above; pass a device_sn to continue.)" >&2
  exit 1
fi

BUCKET="${ULTRA_CONFIG_BUCKET:-siphox-ultra-config}"
REGION="${AWS_DEFAULT_REGION:-us-east-2}"
SRC="${ROOT}/config/ultra_default.yaml"
TMP="/tmp/machine_settings_${DEVICE_SN}.yaml"
LOCAL_CFG_DIR="/etc/ultra"
LOCAL_CFG="${LOCAL_CFG_DIR}/machine.yaml"

if [[ ! -f "$SRC" ]]; then
  echo "Missing $SRC" >&2
  exit 1
fi

# Derive the human-readable machine_name if the operator didn't
# supply one. Keep the derivation cheap / offline so no one has to
# ship a lookup table.
if [[ -n "$MACHINE_NAME_ARG" ]]; then
  MACHINE_NAME="$MACHINE_NAME_ARG"
elif [[ "$DEVICE_SN" =~ ^ultra([0-9]+)$ ]]; then
  MACHINE_NAME="Ultra ${BASH_REMATCH[1]}"
elif [[ "$DEVICE_SN" =~ ^DEV-([0-9a-fA-F]{8}) ]]; then
  MACHINE_NAME="DEV ${BASH_REMATCH[1]}"
else
  MACHINE_NAME="$DEVICE_SN"
fi

echo "=== Machine Identity ==="
echo "  device_sn    = ${DEVICE_SN}"
echo "  machine_name = ${MACHINE_NAME}"
echo "  S3 prefix    = s3://${BUCKET}/machines/${DEVICE_SN}/"

# --- Generate the full machine_settings.yaml for S3 ---
python3 - "$SRC" "$DEVICE_SN" "$MACHINE_NAME" "$TMP" << 'PY'
import sys
from pathlib import Path

src, device_sn, display, out_path = (
    sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4],
)

lines = Path(src).read_text(encoding="utf-8").splitlines()
# Strip any placeholder comment the defaults file used to use for
# device_sn -- harmless if absent on current configs.
skip_comment = "# device_sn: use the real hardware UID in production"

out = [
    "# machine_settings.yaml -- per-machine config.",
    f"# device_sn {device_sn} matches S3 path machines/{device_sn}/.",
    "# Seeded by scripts/seed_machine_settings.sh; safe to edit.",
    "",
]
saw_device_sn = False
saw_machine_name = False
for line in lines:
    s = line.strip()
    if s.startswith(skip_comment):
        continue
    if s.startswith("device_sn:"):
        out.append(f"device_sn: {device_sn}")
        saw_device_sn = True
    elif s.startswith("machine_name:"):
        out.append(f"machine_name: {display}")
        saw_machine_name = True
    else:
        out.append(line)

# If the defaults file didn't already have device_sn / machine_name,
# prepend them so the S3 copy is self-contained.
prepend = []
if not saw_device_sn:
    prepend.append(f"device_sn: {device_sn}")
if not saw_machine_name:
    prepend.append(f"machine_name: {display}")
if prepend:
    out = out[:4] + prepend + [""] + out[4:]

Path(out_path).write_text("\n".join(out) + "\n", encoding="utf-8")
PY

# --- Upload to S3 ---
echo "=== S3 Machine Settings ==="
echo "Uploading ${TMP} -> s3://${BUCKET}/machines/${DEVICE_SN}/machine_settings.yaml"
aws s3 cp "$TMP" \
  "s3://${BUCKET}/machines/${DEVICE_SN}/machine_settings.yaml" \
  --region "$REGION"
echo "[OK] S3 seed complete."

# --- Write local /etc/ultra/machine.yaml ---
echo "=== Local Machine Identity ==="
echo "Writing ${LOCAL_CFG} ..."
sudo mkdir -p "$LOCAL_CFG_DIR"
sudo tee "$LOCAL_CFG" > /dev/null << EOF
# Local machine identity -- read via ULTRA_CONFIG=/etc/ultra/machine.yaml
# device_sn is the stable S3 / fleet identifier for this unit.
device_sn: ${DEVICE_SN}
machine_name: ${MACHINE_NAME}
EOF
sudo chown -R "${CURRENT_USER}:${CURRENT_USER}" "$LOCAL_CFG_DIR"
echo "[OK] ${LOCAL_CFG} written (owned by ${CURRENT_USER})."

# --- Upload calibration data to S3 ---
CALIB_ROOT="${ROOT}/config/calibration_data"
if [[ -d "$CALIB_ROOT" ]]; then
  echo "=== S3 Calibration Data ==="
  for ASSAY_DIR in "${CALIB_ROOT}"/*/; do
    ASSAY="$(basename "$ASSAY_DIR")"
    for VER_DIR in "${ASSAY_DIR}"*/; do
      [[ -d "$VER_DIR" ]] || continue
      VER="$(basename "$VER_DIR")"
      PREFIX="calibration_data/${ASSAY}/${VER}"
      echo "  Syncing ${ASSAY}/${VER} -> s3://${BUCKET}/${PREFIX}/"
      aws s3 sync "${VER_DIR}" "s3://${BUCKET}/${PREFIX}/" \
        --region "$REGION" \
        --exclude ".*"
    done
  done
  echo "[OK] Calibration data uploaded."
else
  echo "No calibration data found at ${CALIB_ROOT} — skipping."
fi

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
  echo "No ${SERVICE_FILE} found — run ./scripts/start.sh after this to create it."
fi

echo ""
echo "=== Done ==="
echo "Device ${DEVICE_SN} (${MACHINE_NAME}) fully provisioned:"
echo "  - AWS creds, IoT certs, local identity"
echo "  - S3 machine_settings at s3://${BUCKET}/machines/${DEVICE_SN}/"
echo "  - Calibration data (if present)"
echo "Next: ./scripts/start.sh"
