'''ultra.services.config_store -- S3-backed global recipes and machine YAML.

Bucket layout (see docs/recipe_s3.md):
  machines/{device_sn}/machine_settings.yaml
  recipes/{slug}/recipe.yaml
  recipes/_shared/_common.yaml
'''
from __future__ import annotations

import logging
import os
from typing import Any

LOG = logging.getLogger(__name__)

DEFAULT_BUCKET = 'siphox-ultra-config'
DEFAULT_REGION = 'us-east-1'
DEFAULT_CACHE = '/tmp/ultra_config_cache'

_s3_client: Any = None


def config_bucket() -> str:
    return os.environ.get(
        'ULTRA_CONFIG_BUCKET', DEFAULT_BUCKET,
    )


def cache_root() -> str:
    return os.environ.get(
        'ULTRA_CONFIG_CACHE', DEFAULT_CACHE,
    )


def _get_s3():
    global _s3_client
    if _s3_client is None:
        import boto3
        from botocore.config import Config
        cfg = Config(
            region_name=os.environ.get(
                'AWS_DEFAULT_REGION', DEFAULT_REGION,
            ),
            signature_version='s3v4',
            retries={'max_attempts': 3, 'mode': 'standard'},
        )
        _s3_client = boto3.client('s3', config=cfg)
    return _s3_client


def machine_settings_key(device_sn: str) -> str:
    return f'machines/{device_sn}/machine_settings.yaml'


def recipe_object_key(slug: str) -> str:
    return f'recipes/{slug}/recipe.yaml'


def shared_common_key() -> str:
    return 'recipes/_shared/_common.yaml'


def _ensure_dir(path: str) -> None:
    os.makedirs(path, mode=0o755, exist_ok=True)


def cache_path_for_key(key: str) -> str:
    '''Local filesystem path mirroring the object key.'''
    return os.path.join(cache_root(), key)


def fetch_object_bytes(key: str) -> bytes | None:
    '''Download object body or None if missing / error.'''
    try:
        resp = _get_s3().get_object(
            Bucket=config_bucket(), Key=key,
        )
        return resp['Body'].read()
    except Exception as exc:
        LOG.debug('S3 get %s: %s', key, exc)
        return None


def put_object_bytes(
        key: str,
        body: bytes,
        content_type: str = 'application/x-yaml',
) -> None:
    _get_s3().put_object(
        Bucket=config_bucket(),
        Key=key,
        Body=body,
        ContentType=content_type,
    )


def machine_settings_object_exists(device_sn: str) -> bool:
    '''True if ``machines/{device_sn}/machine_settings.yaml`` exists in S3.'''
    key = machine_settings_key(device_sn)
    try:
        _get_s3().head_object(
            Bucket=config_bucket(),
            Key=key,
        )
        return True
    except Exception as exc:
        try:
            from botocore.exceptions import ClientError
            if isinstance(exc, ClientError):
                code = exc.response.get('Error', {}).get(
                    'Code', '',
                )
                if code in ('404', 'NoSuchKey', 'NotFound'):
                    return False
        except Exception:
            pass
        LOG.debug('S3 head %s: %s', key, exc)
        return False


def fetch_machine_settings_yaml(device_sn: str) -> str | None:
    '''Return machine settings YAML text from S3, or None.'''
    key = machine_settings_key(device_sn)
    raw = fetch_object_bytes(key)
    if raw is None:
        return None
    text = raw.decode('utf-8')
    path = cache_path_for_key(key)
    _ensure_dir(os.path.dirname(path))
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(text)
    return text


def put_machine_settings_yaml(
        device_sn: str, yaml_text: str,
) -> None:
    put_object_bytes(
        machine_settings_key(device_sn),
        yaml_text.encode('utf-8'),
    )
    key = machine_settings_key(device_sn)
    path = cache_path_for_key(key)
    _ensure_dir(os.path.dirname(path))
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(yaml_text)


def list_recipe_slugs() -> list[str]:
    '''List recipe slugs that have recipes/{slug}/recipe.yaml in S3.'''
    slugs: set[str] = set()
    try:
        paginator = _get_s3().get_paginator('list_objects_v2')
        for page in paginator.paginate(
                Bucket=config_bucket(), Prefix='recipes/',
        ):
            for obj in page.get('Contents', []):
                key = obj['Key']
                parts = key.split('/')
                if (
                    len(parts) >= 3
                    and parts[0] == 'recipes'
                    and parts[2] == 'recipe.yaml'
                    and parts[1] not in ('', '_shared')
                ):
                    slugs.add(parts[1])
    except Exception as exc:
        LOG.warning('list_recipe_slugs: %s', exc)
    return sorted(slugs)


def fetch_recipe_to_cache(slug: str) -> str | None:
    '''Download recipes/{slug}/recipe.yaml to cache; return path or None.'''
    key = recipe_object_key(slug)
    raw = fetch_object_bytes(key)
    if raw is None:
        return None
    path = cache_path_for_key(key)
    _ensure_dir(os.path.dirname(path))
    with open(path, 'wb') as fh:
        fh.write(raw)
    return path


def fetch_shared_common_to_cache() -> str | None:
    key = shared_common_key()
    raw = fetch_object_bytes(key)
    if raw is None:
        return None
    path = cache_path_for_key(key)
    _ensure_dir(os.path.dirname(path))
    with open(path, 'wb') as fh:
        fh.write(raw)
    return path


def put_recipe_yaml(slug: str, yaml_text: str) -> None:
    key = recipe_object_key(slug)
    put_object_bytes(key, yaml_text.encode('utf-8'))
    path = cache_path_for_key(key)
    _ensure_dir(os.path.dirname(path))
    with open(path, 'w', encoding='utf-8') as fh:
        fh.write(yaml_text)


def sync_recipes_and_shared_from_s3() -> None:
    '''Best-effort download of all recipe.yaml and _shared/_common.'''
    slugs = list_recipe_slugs()
    for s in slugs:
        fetch_recipe_to_cache(s)
    fetch_shared_common_to_cache()


def object_version_ids(key: str, max_items: int = 20) -> list[dict]:
    '''Return recent version metadata for a key (newest first).'''
    try:
        resp = _get_s3().list_object_versions(
            Bucket=config_bucket(),
            Prefix=key,
        )
        out = []
        for v in resp.get('Versions', [])[:max_items]:
            if v['Key'] != key:
                continue
            out.append({
                'VersionId': v['VersionId'],
                'LastModified': v['LastModified'].isoformat(),
                'IsLatest': v['IsLatest'],
            })
        return out
    except Exception as exc:
        LOG.warning('object_version_ids: %s', exc)
        return []
