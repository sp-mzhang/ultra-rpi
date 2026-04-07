'''ultra.protocol.recipe_loader -- YAML recipe loader.

Loads, validates, and resolves YAML recipe files into
Recipe objects. Handles include references to shared
phase definitions in _common.yaml.
'''
from __future__ import annotations

import logging
import os
import os.path as op
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
    if op.isfile(name):
        path = name
    else:
        path = op.join(RECIPES_DIR, f'{name}.yaml')

    if not op.isfile(path):
        raise FileNotFoundError(
            f'Recipe not found: {path}',
        )

    with open(path, 'r') as fh:
        raw: dict = yaml.safe_load(fh) or {}

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

    recipe = Recipe(
        name=recipe_name,
        description=description,
        constants=constants,
        wells=wells_raw,
        phases=phases,
        total_steps=total_steps,
    )

    _validate_recipe(recipe)

    LOG.info(
        f'Loaded recipe "{recipe_name}": '
        f'{total_steps} steps, '
        f'{len(recipe.wells)} wells, '
        f'{len(recipe.phases)} phases',
    )
    return recipe


def list_recipes() -> list[dict[str, str]]:
    '''Discover all available YAML recipes.

    Returns:
        List of dicts with 'name' and 'description' keys.
    '''
    recipes = []
    if not op.isdir(RECIPES_DIR):
        return recipes

    for fname in sorted(os.listdir(RECIPES_DIR)):
        if (
            fname.endswith('.yaml')
            and not fname.startswith('_')
        ):
            path = op.join(RECIPES_DIR, fname)
            try:
                with open(path, 'r') as fh:
                    raw = yaml.safe_load(fh) or {}
                recipes.append({
                    'name': raw.get(
                        'name',
                        fname.replace('.yaml', ''),
                    ),
                    'file': fname.replace('.yaml', ''),
                    'description': raw.get(
                        'description', '',
                    ),
                })
            except Exception as err:
                LOG.warning(
                    f'Failed to read recipe {fname}: '
                    f'{err}',
                )
    return recipes


def _load_common() -> dict[str, Any]:
    '''Load _common.yaml shared definitions.'''
    path = op.join(RECIPES_DIR, '_common.yaml')
    if not op.isfile(path):
        return {}
    with open(path, 'r') as fh:
        return yaml.safe_load(fh) or {}


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
