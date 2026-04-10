#!/usr/bin/env bash
# Upload packaged protocol YAMLs to the global S3 recipe catalog.
# Requires: aws CLI, credentials with s3:PutObject on the bucket.
# See docs/recipe_s3.md for bucket layout and IAM.
set -euo pipefail

BUCKET="${ULTRA_CONFIG_BUCKET:-siphox-ultra-config}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RECIPES="${ROOT}/src/ultra/protocol/recipes"

if [[ ! -d "$RECIPES" ]]; then
  echo "Expected recipes dir: $RECIPES" >&2
  exit 1
fi

echo "Uploading to s3://${BUCKET}/"

aws s3 cp "${RECIPES}/_common.yaml" \
  "s3://${BUCKET}/recipes/_shared/_common.yaml"

for slug in crp_ultra quick_demo salt_ultra tsh_ultra; do
  aws s3 cp "${RECIPES}/${slug}.yaml" \
    "s3://${BUCKET}/recipes/${slug}/recipe.yaml"
done

echo "Done."
