#!/usr/bin/env bash
# Download fleet-provisioning claim certificates from S3 to the local RPi.
#
# Usage (run on the RPi):
#   ./scripts/fetch_certs.sh [device_sn]
#
# If device_sn is omitted it is read from /etc/ultra/machine.yaml,
# then falls back to config/ultra_default.yaml.
#
# Downloads from:
#   s3://${BUCKET}/machines/${DEVICE_SN}/certs/
# Into:
#   /etc/ultra/certs/
#
# These are claim certs used for fleet provisioning. The provisioner
# uses them to generate unique device certs (device.pem.crt, etc.).
#
# Environment:
#   ULTRA_CONFIG_BUCKET  (default: siphox-ultra-config)
#   ULTRA_CERT_DIR       (default: /etc/ultra/certs)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUCKET="${ULTRA_CONFIG_BUCKET:-siphox-ultra-config}"
CERT_DIR="${ULTRA_CERT_DIR:-/etc/ultra/certs}"

# --- Resolve device_sn ---------------------------------------------------
if [[ $# -ge 1 ]]; then
  DEVICE_SN="$1"
else
  # Try local machine identity first, then default config.
  for cfg in /etc/ultra/machine.yaml "${ROOT}/config/ultra_default.yaml"; do
    if [[ -f "$cfg" ]]; then
      DEVICE_SN="$(grep '^device_sn:' "$cfg" | head -1 | awk '{print $2}')"
      [[ -n "$DEVICE_SN" ]] && break
    fi
  done
  if [[ -z "${DEVICE_SN:-}" ]]; then
    echo "Could not determine device_sn. Pass it as an argument." >&2
    exit 1
  fi
fi

S3_PREFIX="machines/${DEVICE_SN}/certs"

echo "=== Fetch IoT Certs ==="
echo "  device_sn : ${DEVICE_SN}"
echo "  source    : s3://${BUCKET}/${S3_PREFIX}/"
echo "  dest      : ${CERT_DIR}/"
echo ""

# --- Download to temp, then copy as root ----------------------------------
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "${TMP_DIR}"' EXIT

aws s3 sync "s3://${BUCKET}/${S3_PREFIX}/" "${TMP_DIR}/"

DL_COUNT="$(find "${TMP_DIR}" -type f | wc -l)"
if [[ "$DL_COUNT" -eq 0 ]]; then
  echo "No files downloaded from S3. Check the path and credentials." >&2
  exit 1
fi

sudo mkdir -p "${CERT_DIR}"
sudo cp "${TMP_DIR}"/* "${CERT_DIR}/"
echo "  downloaded ${DL_COUNT} file(s)"

# --- Lock down permissions ------------------------------------------------
CURRENT_USER="$(logname 2>/dev/null || whoami)"
sudo chmod 644 "${CERT_DIR}"/*.pem "${CERT_DIR}"/*.crt 2>/dev/null || true
sudo chmod 600 "${CERT_DIR}"/*.key 2>/dev/null || true
sudo chown "${CURRENT_USER}:${CURRENT_USER}" "${CERT_DIR}"/* 2>/dev/null || true

echo ""
echo "[OK] Certs installed to ${CERT_DIR}/"
ls -la "${CERT_DIR}/"
