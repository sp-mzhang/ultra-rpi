'''ultra.services.config_store -- S3-backed global recipes and machine YAML.

Bucket layout (see docs/recipe_s3.md):
  machines/{device_sn}/machine_settings.yaml
  recipes/{slug}/recipe.yaml
  recipes/_shared/_common.yaml
'''
from __future__ import annotations

import logging
import os
import time
from typing import Any

LOG = logging.getLogger(__name__)

DEFAULT_BUCKET = 'siphox-ultra-config'
DEFAULT_REGION = 'us-east-2'
DEFAULT_CACHE = '/tmp/ultra_config_cache'
CACHE_MAX_AGE_S = 60

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


def _cache_fresh(path: str, max_age_s: float = CACHE_MAX_AGE_S) -> bool:
    '''True if *path* exists and was written less than *max_age_s* ago.'''
    try:
        age = time.time() - os.path.getmtime(path)
        return age < max_age_s
    except OSError:
        return False


def _write_cache(key: str, data: bytes | str) -> str:
    '''Write *data* to the cache path for *key*; return the path.'''
    path = cache_path_for_key(key)
    _ensure_dir(os.path.dirname(path))
    if isinstance(data, bytes):
        with open(path, 'wb') as fh:
            fh.write(data)
    else:
        with open(path, 'w', encoding='utf-8') as fh:
            fh.write(data)
    return path


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


def fetch_machine_settings_yaml(
        device_sn: str,
        force: bool = False,
) -> str | None:
    '''Return machine settings YAML text, preferring cache when fresh.'''
    key = machine_settings_key(device_sn)
    path = cache_path_for_key(key)
    if not force and _cache_fresh(path):
        with open(path, 'r', encoding='utf-8') as fh:
            return fh.read()
    raw = fetch_object_bytes(key)
    if raw is None:
        return None
    text = raw.decode('utf-8')
    _write_cache(key, text)
    return text


def put_machine_settings_yaml(
        device_sn: str, yaml_text: str,
) -> None:
    put_object_bytes(
        machine_settings_key(device_sn),
        yaml_text.encode('utf-8'),
    )
    _write_cache(machine_settings_key(device_sn), yaml_text)


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


def fetch_recipe_to_cache(
        slug: str,
        force: bool = False,
) -> str | None:
    '''Download recipes/{slug}/recipe.yaml to cache; return path or None.

    Skips S3 when the cache file is younger than ``CACHE_MAX_AGE_S``
    unless *force* is True.
    '''
    key = recipe_object_key(slug)
    path = cache_path_for_key(key)
    if not force and _cache_fresh(path):
        return path
    raw = fetch_object_bytes(key)
    if raw is None:
        return None
    _write_cache(key, raw)
    return path


def fetch_shared_common_to_cache(
        force: bool = False,
) -> str | None:
    key = shared_common_key()
    path = cache_path_for_key(key)
    if not force and _cache_fresh(path):
        return path
    raw = fetch_object_bytes(key)
    if raw is None:
        return None
    _write_cache(key, raw)
    return path


def put_recipe_yaml(slug: str, yaml_text: str) -> None:
    key = recipe_object_key(slug)
    put_object_bytes(key, yaml_text.encode('utf-8'))
    _write_cache(key, yaml_text)


def put_shared_common_yaml(yaml_text: str) -> None:
    key = shared_common_key()
    put_object_bytes(key, yaml_text.encode('utf-8'))
    _write_cache(key, yaml_text)


def sync_recipes_and_shared_from_s3() -> None:
    '''Best-effort download of all recipe.yaml and _shared/_common.'''
    slugs = list_recipe_slugs()
    for s in slugs:
        fetch_recipe_to_cache(s, force=True)
    fetch_shared_common_to_cache(force=True)
