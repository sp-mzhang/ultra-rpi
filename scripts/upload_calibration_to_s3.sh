#!/usr/bin/env bash
# Upload calibration data to S3.
#
# Usage:
#   ./scripts/upload_calibration_to_s3.sh [assay] [version]
#
# Defaults:
#   assay   = crp
#   version = v1.0
#
# Source:  config/calibration_data/{assay}/{version}/   (in this repo)
# Dest:   s3://siphox-ultra-config/calibration_data/{assay}/{version}/
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

ASSAY="${1:-crp}"
VERSION="${2:-v1.0}"
BUCKET="${ULTRA_CONFIG_BUCKET:-siphox-ultra-config}"
REGION="${AWS_DEFAULT_REGION:-us-east-2}"

SRC="${REPO_ROOT}/config/calibration_data/${ASSAY}/${VERSION}"
PREFIX="calibration_data/${ASSAY}/${VERSION}"

if [[ ! -d "$SRC" ]]; then
  echo "[ERROR] Source directory not found: ${SRC}" >&2
  exit 1
fi

echo "=== Upload Calibration Data ==="
echo "  assay   : ${ASSAY}"
echo "  version : ${VERSION}"
echo "  source  : ${SRC}/"
echo "  dest    : s3://${BUCKET}/${PREFIX}/"

aws s3 sync "${SRC}/" "s3://${BUCKET}/${PREFIX}/" \
  --region "${REGION}" \
  --exclude ".*"

echo "[OK] Calibration data uploaded."
