# Recipe and machine config (S3)

Ultra can load **global protocol recipes** and **per-machine settings** from an S3 bucket (`siphox-ultra-config` by default). S3 **versioning** should be enabled on that bucket for history.

## Bucket setup (AWS console or CLI)

1. Create bucket (e.g. `siphox-ultra-config`) in the region you will use for API calls (must match **`AWS_DEFAULT_REGION`** on the RPi). Example deployment: **`us-east-2`**.
2. Block public access; use SSE-S3 or KMS.
3. Enable **Bucket versioning**: bucket → Properties → Versioning → Enable.

## Key layout

| Prefix | Purpose |
|--------|---------|
| `machines/{device_sn}/machine_settings.yaml` | Full per-machine YAML (same shape as `ultra_default.yaml`), merged over defaults. |
| `recipes/{slug}/recipe.yaml` | Global recipe (same for all instruments). |
| `recipes/_shared/_common.yaml` | Shared includes for `include: _common.yaml#section`. |

`device_sn` comes from local config (`config/ultra_default.yaml` or `ULTRA_CONFIG`).

## IAM policy (RPi / operator)

Grant `s3:GetObject`, `s3:PutObject`, `s3:ListBucket`, `s3:ListObjectVersions` on `arn:aws:s3:::siphox-ultra-config` and `arn:aws:s3:::siphox-ultra-config/*`.

## Environment

| Variable | Default | Meaning |
|----------|---------|---------|
| `ULTRA_CONFIG_BUCKET` | `siphox-ultra-config` | Bucket name |
| `AWS_DEFAULT_REGION` | `us-east-2` | **Must match the bucket region**. |
| `ULTRA_CONFIG_CACHE` | `/tmp/ultra_config_cache` | Local cache for recipe YAML |

## Config merge order

1. `config/ultra_default.yaml`
2. `ULTRA_CONFIG` file (if set)
3. S3 `machines/{device_sn}/machine_settings.yaml` (if present and download succeeds)

`GET /api/machine-settings` returns the **raw** YAML from S3 when
`machines/{device_sn}/machine_settings.yaml` exists and is non-empty (so the
editor matches the bucket, including comments). If there is no object yet, it
returns a draft built from the full effective merged in-memory config.

Use **`GET /api/machine-settings?apply=1`** (the **Reload** button) to
re-download from S3 **and** deep-merge that YAML into `app.config` for the next
run — **no process restart**.

**`PUT` or `POST /api/machine-settings`** uploads to S3 and merges into
`app.config` immediately (same as Reload with `apply=1`, but from your edited
text).

`GET /api/recipes/{slug}/yaml` returns `source`: `s3` when
`recipes/{slug}/recipe.yaml` exists in the bucket (raw downloaded text), else
`packaged` from the repo. **`PUT` or `POST`** saves to S3 and updates the local
cache file used by `load_recipe`.

## Seeding the bucket from the repo

After editing packaged YAMLs under `src/ultra/protocol/recipes/`, upload the
global catalog (shared `_common` + one object per recipe slug):

```bash
chmod +x scripts/upload_recipes_to_s3.sh
export AWS_PROFILE=...   # or rely on instance role on the RPi
./scripts/upload_recipes_to_s3.sh
```

Override the bucket with `ULTRA_CONFIG_BUCKET` if it differs from the default.

## Seeding machine settings (ultra1–ultra6)

To push **full** `machine_settings.yaml` copies derived from
`config/ultra_default.yaml` (with `device_sn: ultraN` and `machine_name: Ultra N`):

```bash
chmod +x scripts/seed_machine_settings_ultra1_6.sh
export AWS_DEFAULT_REGION=us-east-2   # match bucket
./scripts/seed_machine_settings_ultra1_6.sh
```

## Recipe resolution

1. Try S3 (cached under `ULTRA_CONFIG_CACHE`) for `recipes/{slug}/recipe.yaml`.
2. Fall back to packaged files in `src/ultra/protocol/recipes/`.

## Time skew

If AWS returns `RequestTimeTooSkewed`, sync the RPi clock (`timedatectl set-ntp true`).

## Setting up a new unit

To bring a new Ultra RPi (e.g. `ultra2`) online with its own machine-specific
settings and recipes:

1. **Set `device_sn`** on the device. Either:
   - Edit `config/ultra_default.yaml` and set `device_sn: ultra2`, or
   - Create a local override file and point to it:
     ```bash
     echo 'device_sn: ultra2' > /etc/ultra/machine.yaml
     export ULTRA_CONFIG=/etc/ultra/machine.yaml
     ```
   The `device_sn` determines the S3 key `machines/ultra2/machine_settings.yaml`.

2. **Seed machine settings in S3.** Upload a starting `machine_settings.yaml`:
   ```bash
   export AWS_DEFAULT_REGION=us-east-2
   aws s3 cp machine_settings.yaml \
     s3://siphox-ultra-config/machines/ultra2/machine_settings.yaml
   ```
   Or use the seeding script for a batch (see "Seeding machine settings" above).

3. **Ensure recipes exist.** Global recipes are shared across all machines.
   Run `scripts/upload_recipes_to_s3.sh` if the bucket has no recipes yet.

4. **Configure AWS credentials** on the RPi (`~/.aws/credentials` or
   instance role). Grant the IAM policy described above.

5. **Start the application.** On first boot the app will:
   - Read `device_sn` from config (`ultra2`).
   - Download `machines/ultra2/machine_settings.yaml` from S3.
   - Deep-merge the S3 YAML over the defaults + `ULTRA_CONFIG`.
   - Fetch global recipes to the local cache.

6. **Edit via the GUI.** Open the "Config & recipes" tab to view/edit
   machine settings and recipes. Save uploads to S3 and applies changes
   immediately (no restart needed).

### `ULTRA_CONFIG` override pattern

For production, prefer a per-device file outside the repo:

```
/etc/ultra/machine.yaml   # device_sn, machine_name, any local overrides
export ULTRA_CONFIG=/etc/ultra/machine.yaml
```

This keeps the repo `config/ultra_default.yaml` identical on every RPi
while each unit picks up its own `device_sn` at boot.

## Seeding recipes

Use `scripts/upload_recipes_to_s3.sh` (see script header) after configuring AWS credentials.
