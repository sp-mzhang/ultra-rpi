'''ultra.protocol.recipe_loader -- YAML recipe loader.

Loads, validates, and resolves YAML recipe files into
Recipe objects. Handles include references to shared
phase definitions in _common.yaml.

Recipes may be loaded from the S3 cache (synced from the
global config bucket), then packaged files under recipes/.
'''
from __future__ import annotations

import logging
import os
import os.path as op
from dataclasses import replace
from typing import Any

import yaml

from ultra.protocol.models import (
    PhaseDef,
    Recipe,
    StepDef,
)
from ultra.protocol.steps import STEP_REGISTRY

LOG = logging.getLogger(__name__)

RECIPES_DIR = op.join(
    op.dirname(op.abspath(__file__)), 'recipes',
)

# Keys that belong in per-machine S3 overlay, not global recipes.
FORBIDDEN_IN_GLOBAL_RECIPE: frozenset[str] = frozenset({
    'loc_offset_x_um',
    'loc_offset_y_um',
    'loc_offset_z_um',
    'angle_open_initial_deg',
    'default_cartridge_z_mm',
})

MACHINE_CALIBRATION_KEYS: frozenset[str] = frozenset({
    'loc_offset_x_um',
    'loc_offset_y_um',
    'loc_offset_z_um',
    'angle_open_initial_deg',
    'default_cartridge_z_mm',
})


def _resolve_recipe_path(name: str) -> str:
    '''Prefer S3 cache, then packaged recipes/.'''
    if op.isfile(name):
        return name
    try:
        from ultra.services import config_store
        cached = config_store.fetch_recipe_to_cache(name)
        if cached and op.isfile(cached):
            LOG.info('Using S3-cached recipe: %s', name)
            return cached
    except Exception as exc:
        LOG.debug('S3 recipe fetch %s: %s', name, exc)
    path = op.join(RECIPES_DIR, f'{name}.yaml')
    if op.isfile(path):
        return path
    raise FileNotFoundError(
        f'Recipe not found: {name} '
        f'(not in S3 cache or {RECIPES_DIR})',
    )


def _load_common() -> dict[str, Any]:
    '''Load _common.yaml from S3 cache first, then disk.'''
    try:
        from ultra.services import config_store
        p = config_store.fetch_shared_common_to_cache()
        if p and op.isfile(p):
            with open(p, 'r', encoding='utf-8') as fh:
                return yaml.safe_load(fh) or {}
    except Exception as exc:
        LOG.debug('_load_common S3: %s', exc)
    path = op.join(RECIPES_DIR, '_common.yaml')
    if not op.isfile(path):
        return {}
    with open(path, 'r', encoding='utf-8') as fh:
        return yaml.safe_load(fh) or {}


def recipe_from_raw_dict(raw: dict[str, Any], name: str) -> Recipe:
    '''Build a Recipe from parsed YAML dict (used by API validation).'''
    recipe_name = raw.get('name', name)
    description = raw.get('description', '')
    wells_raw = raw.get('wells', {})
    phases_raw = raw.get('phases', [])

    common = _load_common()

    defaults = common.get('defaults', {})
    constants = {**defaults, **raw.get('constants', {})}

    phases: list[PhaseDef] = []
    total_steps = 0

    for phase_raw in phases_raw:
        include = phase_raw.get('include')
        if include:
            resolved_steps = _resolve_include(
                include, common,
            )
        else:
            resolved_steps = _parse_steps(
                phase_raw.get('steps', []),
            )

        phase = PhaseDef(
            name=phase_raw.get('name', ''),
            label=phase_raw.get('label', ''),
            steps=resolved_steps,
        )
        total_steps += len(phase.steps)
        phases.append(phase)

    reader = dict(raw.get('reader', {}))
    peak_detect = dict(raw.get('peak_detect', {}))

    return Recipe(
        name=recipe_name,
        description=description,
        constants=constants,
        wells=wells_raw,
        phases=phases,
        total_steps=total_steps,
        reader=reader,
        peak_detect=peak_detect,
    )


def load_recipe(name: str) -> Recipe:
    '''Load and validate a YAML recipe file.

    Resolves include references to _common.yaml, validates
    step types exist in STEP_REGISTRY, validates well
    references, and computes total_steps.

    Args:
        name: Recipe name (without .yaml extension) or
            full path to a YAML file.

    Returns:
        Fully loaded and validated Recipe object.

    Raises:
        FileNotFoundError: If the recipe file does not exist.
        ValueError: If the recipe fails validation.
    '''
    path = _resolve_recipe_path(name)
    with open(path, 'r', encoding='utf-8') as fh:
        raw: dict = yaml.safe_load(fh) or {}

    recipe = recipe_from_raw_dict(raw, name)
    _validate_recipe(recipe)
    lint_global_recipe_keys(recipe)

    LOG.info(
        f'Loaded recipe "{recipe.name}": '
        f'{recipe.total_steps} steps, '
        f'{len(recipe.wells)} wells, '
        f'{len(recipe.phases)} phases',
    )
    return recipe


def apply_machine_calibration(
        config: dict[str, Any],
        recipe: Recipe,
) -> Recipe:
    '''Overlay per-machine calibration from config onto constants.

    Uses ``calibration:`` map and/or top-level calibration keys
    merged from S3 machine_settings.yaml.
    '''
    cal = config.get('calibration')
    if not isinstance(cal, dict):
        cal = {}
    cal = dict(cal)
    for k in MACHINE_CALIBRATION_KEYS:
        if k in config and k not in cal:
            cal[k] = config[k]
    if not cal:
        return recipe
    new_constants = {**recipe.constants, **cal}
    return replace(recipe, constants=new_constants)


def merge_protocol_config(
        base: dict[str, Any],
        recipe: Recipe,
) -> dict[str, Any]:
    '''Merge recipe reader/peak_detect over app config for one run.'''
    from ultra.config import _deep_merge

    patch: dict[str, Any] = {}
    if recipe.reader:
        patch['reader'] = recipe.reader
    if recipe.peak_detect:
        patch['peak_detect'] = recipe.peak_detect
    if not patch:
        return dict(base)
    return _deep_merge(dict(base), patch)


def lint_global_recipe_keys(recipe: Recipe) -> None:
    '''Raise ValueError if recipe constants contain machine-only keys.'''
    bad = FORBIDDEN_IN_GLOBAL_RECIPE & set(recipe.constants.keys())
    if bad:
        raise ValueError(
            f'Recipe must not set machine-specific keys in '
            f'constants (use machine S3 settings): {sorted(bad)}',
        )


def list_recipes() -> list[dict[str, str]]:
    '''Discover packaged recipes and S3-backed recipes.

    S3 entries override the same ``file`` slug when both exist.
    '''
    seen: dict[str, dict[str, str]] = {}

    if op.isdir(RECIPES_DIR):
        for fname in sorted(os.listdir(RECIPES_DIR)):
            if (
                fname.endswith('.yaml')
                and not fname.startswith('_')
            ):
                path = op.join(RECIPES_DIR, fname)
                try:
                    with open(path, 'r', encoding='utf-8') as fh:
                        raw = yaml.safe_load(fh) or {}
                    slug = fname.replace('.yaml', '')
                    seen[slug] = {
                        'name': raw.get('name', slug),
                        'file': slug,
                        'description': raw.get(
                            'description', '',
                        ),
                        'source': 'packaged',
                    }
                except Exception as err:
                    LOG.warning(
                        'Failed to read recipe %s: %s',
                        fname, err,
                    )

    try:
        from ultra.services import config_store
        for slug in config_store.list_recipe_slugs():
            path = config_store.fetch_recipe_to_cache(slug)
            if path and op.isfile(path):
                with open(path, 'r', encoding='utf-8') as fh:
                    raw = yaml.safe_load(fh) or {}
                seen[slug] = {
                    'name': raw.get('name', slug),
                    'file': slug,
                    'description': raw.get(
                        'description', '',
                    ),
                    'source': 's3',
                }
    except Exception as exc:
        LOG.warning('list_recipes S3: %s', exc)

    return sorted(seen.values(), key=lambda r: r['file'])


def _resolve_include(
        include: str,
        common: dict[str, Any],
) -> list[StepDef]:
    '''Resolve an include reference.

    Format: "_common.yaml#section_name"

    Args:
        include: Include reference string.
        common: Parsed _common.yaml contents.

    Returns:
        List of StepDef from the included section.

    Raises:
        ValueError: If the reference cannot be resolved.
    '''
    if '#' not in include:
        raise ValueError(
            f'Invalid include format: {include} '
            f'(expected file.yaml#section)',
        )

    _, section = include.split('#', 1)
    section_data = common.get(section)
    if section_data is None:
        raise ValueError(
            f'Include section not found: {section}',
        )

    steps_raw = section_data.get('steps', [])
    return _parse_steps(steps_raw)


def _parse_steps(
        steps_raw: list[dict[str, Any]],
) -> list[StepDef]:
    '''Parse a list of raw step dicts into StepDef objects.

    Args:
        steps_raw: List of step dicts from YAML.

    Returns:
        List of StepDef objects.
    '''
    steps = []
    for raw in steps_raw:
        step = StepDef(
            type=raw.get('type', ''),
            label=raw.get('label', ''),
            params=dict(raw),
        )
        steps.append(step)
    return steps


def _validate_recipe(recipe: Recipe) -> None:
    '''Validate a loaded recipe.

    Checks:
    - All step types are registered.
    - Well references in steps match the wells map.
    - Required step parameters are present.

    Args:
        recipe: Recipe to validate.

    Raises:
        ValueError: On validation failure.
    '''
    well_names = set(recipe.wells.keys())

    for phase in recipe.phases:
        for step in phase.steps:
            if step.type not in STEP_REGISTRY:
                raise ValueError(
                    f'Unknown step type "{step.type}" '
                    f'in step "{step.label}"',
                )

            _well_refs = ['source', 'target', 'dest',
                          'well']
            for ref_key in _well_refs:
                ref = step.params.get(ref_key)
                if ref and ref not in well_names:
                    raise ValueError(
                        f'Step "{step.label}" references '
                        f'unknown well "{ref}" '
                        f'(available: {well_names})',
                    )
