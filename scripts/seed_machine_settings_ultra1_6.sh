#!/usr/bin/env bash
# Upload full machine_settings.yaml for machines ultra1 … ultra6 from
# config/ultra_default.yaml (same keys), with device_sn ultraN and
# machine_name "Ultra N". Requires aws CLI and credentials.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BUCKET="${ULTRA_CONFIG_BUCKET:-siphox-ultra-config}"
REGION="${AWS_DEFAULT_REGION:-us-east-2}"
SRC="${ROOT}/config/ultra_default.yaml"

if [[ ! -f "$SRC" ]]; then
  echo "Missing $SRC" >&2
  exit 1
fi

python3 << PY
from pathlib import Path

root = Path("${ROOT}")
src = root / "config" / "ultra_default.yaml"
lines = src.read_text(encoding="utf-8").splitlines()
repo_dev_comment = "# device_sn: use the real hardware UID in production"

for n in range(1, 7):
    out = []
    out.append(
        "# machine_settings.yaml — full per-machine config for testing."
    )
    out.append(
        "# device_sn: use the real hardware UID in production; "
        f"ultra{n} matches S3 path machines/ultra{n}/ for now."
    )
    out.append("")
    for line in lines:
        s = line.strip()
        if s.startswith(repo_dev_comment):
            continue
        if s.startswith("device_sn:"):
            out.append(f"device_sn: ultra{n}")
        elif s.startswith("machine_name:"):
            out.append(f"machine_name: Ultra {n}")
        else:
            out.append(line)
    path = Path("/tmp") / f"machine_settings_ultra{n}.yaml"
    path.write_text("\n".join(out) + "\n", encoding="utf-8")
    print(path)
PY

echo "Uploading to s3://${BUCKET}/ (region ${REGION})"
for i in 1 2 3 4 5 6; do
  aws s3 cp "/tmp/machine_settings_ultra${i}.yaml" \
    "s3://${BUCKET}/machines/ultra${i}/machine_settings.yaml" \
    --region "${REGION}"
done
echo "Done."
