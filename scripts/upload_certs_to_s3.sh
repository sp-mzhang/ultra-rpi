#!/usr/bin/env bash
# Upload device IoT certificates to S3 for a specific machine.
#
# Usage:
#   ./scripts/upload_certs_to_s3.sh [device_sn]
#
# If device_sn is omitted it is read from config/ultra_default.yaml.
#
# Uploads all .pem / .key / .crt files from provisioning/certs/ to:
#   s3://${BUCKET}/machines/${DEVICE_SN}/certs/
#
# Each machine has its own folder so certs stay unique per device.
#
# Environment:
#   ULTRA_CONFIG_BUCKET  (default: siphox-ultra-config)
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUCKET="${ULTRA_CONFIG_BUCKET:-siphox-ultra-config}"
CERTS_DIR="${ROOT}/provisioning/certs"

if [[ $# -ge 1 ]]; then
  DEVICE_SN="$1"
else
  DEVICE_SN="$(grep '^device_sn:' "${ROOT}/config/ultra_default.yaml" \
    | head -1 | awk '{print $2}')"
  if [[ -z "$DEVICE_SN" ]]; then
    echo "Could not read device_sn from config. Pass it as an argument." >&2
    exit 1
  fi
fi

if [[ ! -d "$CERTS_DIR" ]]; then
  echo "Certs directory not found: ${CERTS_DIR}" >&2
  exit 1
fi

S3_PREFIX="machines/${DEVICE_SN}/certs"

echo "=== Upload IoT Certs ==="
echo "  device_sn : ${DEVICE_SN}"
echo "  source    : ${CERTS_DIR}"
echo "  dest      : s3://${BUCKET}/${S3_PREFIX}/"
echo ""

count=0
for f in "${CERTS_DIR}"/*.pem "${CERTS_DIR}"/*.key "${CERTS_DIR}"/*.crt; do
  [[ -f "$f" ]] || continue
  name="$(basename "$f")"
  echo "  uploading ${name} ..."
  aws s3 cp "$f" "s3://${BUCKET}/${S3_PREFIX}/${name}" --quiet
  count=$((count + 1))
done

if [[ $count -eq 0 ]]; then
  echo "No cert files (.pem/.key/.crt) found in ${CERTS_DIR}" >&2
  exit 1
fi

echo ""
echo "[OK] ${count} file(s) uploaded to s3://${BUCKET}/${S3_PREFIX}/"
