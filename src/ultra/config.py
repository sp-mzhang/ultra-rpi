'''ultra.config -- YAML configuration loader.

Loads the default config from config/ultra_default.yaml and
optionally merges an override file specified via the
ULTRA_CONFIG environment variable.
'''
from __future__ import annotations

import logging
import os
import os.path as op
from typing import Any

import yaml

LOG = logging.getLogger(__name__)

_PROJECT_ROOT = op.dirname(
    op.dirname(op.dirname(op.abspath(__file__))),
)
DEFAULT_CONFIG_PATH = op.join(
    _PROJECT_ROOT, 'config', 'ultra_default.yaml',
)


def _deep_merge(
        base: dict[str, Any],
        override: dict[str, Any],
) -> dict[str, Any]:
    '''Recursively merge *override* into *base*.

    Returns a new dict; neither input is mutated.
    '''
    merged = dict(base)
    for key, val in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(val, dict)
        ):
            merged[key] = _deep_merge(merged[key], val)
        else:
            merged[key] = val
    return merged


def load_config(
        path: str | None = None,
) -> dict[str, Any]:
    '''Load and merge configuration from YAML files.

    First loads the built-in default config, then merges any
    override file. The override path is resolved in order:
      1. *path* argument (if given)
      2. ULTRA_CONFIG environment variable
      3. No override -- defaults only

    Args:
        path: Optional path to an override YAML file.

    Returns:
        Merged configuration dictionary.
    '''
    with open(DEFAULT_CONFIG_PATH, 'r') as fh:
        config: dict[str, Any] = yaml.safe_load(fh) or {}
    LOG.debug('Loaded default config from %s', DEFAULT_CONFIG_PATH)

    override_path = path or os.environ.get('ULTRA_CONFIG')
    if override_path and op.isfile(override_path):
        with open(override_path, 'r') as fh:
            override: dict[str, Any] = yaml.safe_load(fh) or {}
        config = _deep_merge(config, override)
        LOG.info('Merged override config from %s', override_path)

    device_sn = config.get('device_sn', '')
    if device_sn:
        try:
            from ultra.services import config_store
            yaml_text = config_store.fetch_machine_settings_yaml(
                device_sn,
            )
            if yaml_text:
                s3_overlay: dict[str, Any] = (
                    yaml.safe_load(yaml_text) or {}
                )
                config = _deep_merge(config, s3_overlay)
                LOG.info(
                    'Merged S3 machine settings for %s',
                    device_sn,
                )
        except Exception as exc:
            LOG.warning(
                'S3 machine settings not merged: %s', exc,
            )

    return config
