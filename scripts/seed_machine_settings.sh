#!/usr/bin/env bash
# Provision a new Ultra device: credentials, IoT certs, machine identity.
#
# Usage:
#   ./scripts/seed_machine_settings.sh <machine_name>
#
# Example:
#   ./scripts/seed_machine_settings.sh ultra4
#
# What it does (in order):
#   1. Installs required packages (python3, awscli, boto3).
#   2. Writes ~/.aws/credentials and ~/.aws/config if missing.
#   3. Copies IoT fleet-provisioning certs to /etc/siphox/.
#   4. Generates machines/<machine_name>/machine_settings.yaml in S3.
#   5. Writes /etc/ultra/machine.yaml for local identity.
#   6. Patches the systemd service if needed.
#
# NOTE: Currently device_sn is set to the machine name (e.g. "ultra4").
#       In the future device_sn will be replaced by a real hardware UID.
#       When that happens, update this script to read the UID from the
#       hardware and use it for device_sn (the S3 key will change too).
#
# Environment:
#   ULTRA_CONFIG_BUCKET  (default: siphox-ultra-config)
#   AWS_DEFAULT_REGION   (default: us-east-2)
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
# 3. IoT fleet-provisioning certificates -> /etc/siphox/             #
# ------------------------------------------------------------------ #
CERTS_DIR="${ROOT}/provisioning/certs"
IOT_DEST="/etc/siphox"
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
  sudo chown "${CURRENT_USER}:${CURRENT_USER}" "${IOT_DEST}"/*
  echo "[OK] Certs installed to ${IOT_DEST}"
else
  echo "IoT certs not found in ${CERTS_DIR} — skipping."
  echo "  Place claim.cert.pem, claim.private.key, root-CA.crt there and re-run."
fi

# ------------------------------------------------------------------ #
# 4-6. Machine identity (requires <machine_name> argument)           #
# ------------------------------------------------------------------ #
if [[ $# -lt 1 ]]; then
  echo ""
  echo "Usage: $0 <machine_name>  (e.g. ultra4)" >&2
  echo "  (Steps 1-3 completed above; pass a machine name to continue.)"
  exit 1
fi

MACHINE="$1"
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
echo "=== S3 Machine Settings ==="
echo "Uploading ${TMP} -> s3://${BUCKET}/machines/${MACHINE}/machine_settings.yaml"
aws s3 cp "$TMP" \
  "s3://${BUCKET}/machines/${MACHINE}/machine_settings.yaml" \
  --region "$REGION"
echo "[OK] S3 seed complete."

# --- Write local /etc/ultra/machine.yaml ---
echo "=== Local Machine Identity ==="
echo "Writing ${LOCAL_CFG} ..."
sudo mkdir -p "$LOCAL_CFG_DIR"
sudo tee "$LOCAL_CFG" > /dev/null << EOF
# Local machine identity — read via ULTRA_CONFIG=/etc/ultra/machine.yaml
# NOTE: device_sn will be a real hardware UID in the future.
device_sn: ${MACHINE}
machine_name: ${DISPLAY}
EOF
echo "[OK] ${LOCAL_CFG} written."

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
echo "${MACHINE} fully provisioned: AWS creds, IoT certs, S3 settings, local identity."
echo "Next: ./scripts/start.sh"
