# Recipe and machine config (S3)

Ultra can load **global protocol recipes** and **per-machine settings** from an S3 bucket (`siphox-ultra-config` by default). S3 **versioning** should be enabled on that bucket for history.

## Bucket setup (AWS console or CLI)

1. Create bucket (e.g. `siphox-ultra-config`) in the region you will use for API calls (must match **`AWS_DEFAULT_REGION`** on the RPi). Example deployment: **`us-east-2`**.
2. Block public access; use SSE-S3 or KMS.
3. Enable **Bucket versioning**: bucket → Properties → Versioning → Enable.

## Key layout

| Prefix | Purpose |
|--------|---------|
| `machines/{device_sn}/machine_settings.yaml` | Per-unit calibration (offsets, angles, Z defaults). |
| `recipes/{slug}/recipe.yaml` | Global recipe (same for all instruments). |
| `recipes/_shared/_common.yaml` | Shared includes for `include: _common.yaml#section`. |

`device_sn` comes from local config (`config/ultra_default.yaml` or `ULTRA_CONFIG`).

## IAM policy (RPi / operator)

Grant `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`, `s3:ListObjectVersions` on `arn:aws:s3:::siphox-ultra-config` and `arn:aws:s3:::siphox-ultra-config/*`.

## Environment

| Variable | Default | Meaning |
|----------|---------|---------|
| `ULTRA_CONFIG_BUCKET` | `siphox-ultra-config` | Bucket name |
| `AWS_DEFAULT_REGION` | often `us-east-1` in code | **Must match the bucket region** (e.g. `us-east-2` if the bucket is there). |
| `ULTRA_CONFIG_CACHE` | `/tmp/ultra_config_cache` | Local cache for recipe YAML |

## Config merge order

1. `config/ultra_default.yaml`
2. `ULTRA_CONFIG` file (if set)
3. S3 `machines/{device_sn}/machine_settings.yaml` (if present and download succeeds)

## Seeding the bucket from the repo

After editing packaged YAMLs under `src/ultra/protocol/recipes/`, upload the
global catalog (shared `_common` + one object per recipe slug):

```bash
chmod +x scripts/upload_recipes_to_s3.sh
export AWS_PROFILE=...   # or rely on instance role on the RPi
./scripts/upload_recipes_to_s3.sh
```

Override the bucket with `ULTRA_CONFIG_BUCKET` if it differs from the default.

## Recipe resolution

1. Try S3 (cached under `ULTRA_CONFIG_CACHE`) for `recipes/{slug}/recipe.yaml`.
2. Fall back to packaged files in `src/ultra/protocol/recipes/`.

## Time skew

If AWS returns `RequestTimeTooSkewed`, sync the RPi clock (`timedatectl set-ntp true`).

## Seeding recipes

Use `scripts/upload_recipes_to_s3.sh` (see script header) after configuring AWS credentials.
