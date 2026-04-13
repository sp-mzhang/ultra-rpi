'''Machine settings, recipes, and protocol builder metadata endpoints.

Handles /machine-settings, /config/sync-recipes, /recipes/{slug}/yaml,
/recipes/{slug} DELETE, /common-protocol/yaml, /protocol/step-types,
and /protocol/step-schemas.
'''
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, HTTPException, Query, UploadFile
from fastapi.responses import Response
from pydantic import BaseModel

if TYPE_CHECKING:
    from ultra.app import Application

LOG = logging.getLogger(__name__)


class YamlTextBody(BaseModel):
    '''YAML payload for machine settings or recipe save.'''
    yaml_text: str = ''


def _machine_settings_effective_yaml(
        cfg: dict[str, Any],
) -> str:
    '''Serialize the full effective in-memory config as YAML.'''
    import yaml

    header = (
        '# machine_settings.yaml — full effective config.\n'
        '# Edit any keys and Save to S3.\n\n'
    )
    try:
        body = yaml.dump(
            cfg,
            default_flow_style=False,
            allow_unicode=True,
            sort_keys=False,
        )
    except Exception:
        LOG.exception('Cannot serialize config to YAML')
        return header + '# Error: see server log.\n'
    return header + body


def _parse_and_merge_machine_yaml(
        yaml_text: str,
        app_config: dict[str, Any],
) -> dict[str, Any]:
    '''Parse *yaml_text*, deep-merge into *app_config*.

    Raises ``ValueError`` when the YAML is not a mapping.
    '''
    import yaml
    from ultra.config import merge_config

    try:
        parsed = yaml.safe_load(yaml_text)
    except yaml.YAMLError as exc:
        raise ValueError(f'Invalid YAML: {exc}') from exc
    if parsed is None:
        parsed = {}
    if not isinstance(parsed, dict):
        raise ValueError(
            'Machine settings must be a YAML mapping, '
            'not a list or scalar.',
        )
    return merge_config(app_config, parsed)


def _read_yaml_cached(
        cache_fetcher,
        fallback_dir: str,
        filename: str,
) -> tuple[str, str]:
    '''Read YAML from S3 cache or packaged fallback.

    Args:
        cache_fetcher: callable returning a cache path or None.
        fallback_dir: directory containing packaged fallback files.
        filename: the YAML filename (e.g. 'crp_ultra.yaml').

    Returns:
        (yaml_text, source) where source is 's3' or 'packaged'.

    Raises:
        FileNotFoundError if neither source has the file.
    '''
    import os.path as op

    path = cache_fetcher()
    if path and op.isfile(path):
        with open(path, encoding='utf-8') as fh:
            return fh.read(), 's3'
    pack = op.join(fallback_dir, filename)
    if op.isfile(pack):
        with open(pack, encoding='utf-8') as fh:
            return fh.read(), 'packaged'
    raise FileNotFoundError(filename)


LOCAL_MACHINE_YAML = '/etc/ultra/machine.yaml'


def _write_local_machine_yaml(yaml_text: str) -> None:
    '''Persist the machine settings to /etc/ultra/machine.yaml.

    Creates the directory if needed.  Silently logs on failure
    (e.g. when running without write access to /etc).
    '''
    import os
    try:
        os.makedirs(os.path.dirname(LOCAL_MACHINE_YAML), exist_ok=True)
        with open(LOCAL_MACHINE_YAML, 'w', encoding='utf-8') as fh:
            fh.write(yaml_text)
        LOG.info('Wrote %s', LOCAL_MACHINE_YAML)
    except OSError as exc:
        LOG.warning(
            'Could not write %s: %s', LOCAL_MACHINE_YAML, exc,
        )


def create_config_router(app: 'Application') -> APIRouter:
    router = APIRouter()

    @router.get('/machine-settings')
    async def machine_settings_get(
            apply: bool = Query(
                False,
                description='Reload from S3 and merge into app.config.',
            ),
    ):
        '''Return YAML for the machine settings editor.'''
        from ultra.services import config_store
        ds = app.config.get('device_sn', '')
        if not ds:
            raise HTTPException(
                status_code=400,
                detail='device_sn not set in config',
            )
        loop = asyncio.get_running_loop()

        def _load() -> tuple[str, str, bool]:
            raw = config_store.fetch_machine_settings_yaml(
                ds, force=apply,
            )
            if raw and raw.strip():
                applied = False
                if apply:
                    try:
                        app.config = _parse_and_merge_machine_yaml(
                            raw, app.config,
                        )
                        applied = True
                    except ValueError as exc:
                        LOG.warning('apply machine_settings: %s', exc)
                return raw, 's3', applied
            return (
                _machine_settings_effective_yaml(app.config),
                'defaults',
                False,
            )

        yaml_text, source, applied = await loop.run_in_executor(
            None, _load,
        )
        return {
            'device_sn': ds,
            'yaml_text': yaml_text,
            'source': source,
            'applied': applied,
        }

    @router.put('/machine-settings')
    @router.post('/machine-settings')
    async def machine_settings_put(req: YamlTextBody):
        '''Save machine_settings.yaml to S3, local disk, and app.config.'''
        from ultra.services import config_store
        ds = app.config.get('device_sn', '')
        if not ds:
            raise HTTPException(
                status_code=400,
                detail='device_sn not set in config',
            )
        loop = asyncio.get_running_loop()

        def _save_and_apply() -> None:
            app.config = _parse_and_merge_machine_yaml(
                req.yaml_text, app.config,
            )
            config_store.put_machine_settings_yaml(
                ds, req.yaml_text,
            )
            _write_local_machine_yaml(req.yaml_text)

        try:
            await loop.run_in_executor(
                None, _save_and_apply,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=str(exc),
            ) from exc
        except Exception as exc:
            LOG.exception('S3 put machine_settings failed')
            raise HTTPException(
                status_code=502,
                detail=f'S3 upload failed: {exc}',
            ) from exc
        return {
            'ok': True,
            'message': (
                'Saved to S3, /etc/ultra/machine.yaml,'
                ' and applied (no restart needed).'
            ),
        }

    @router.post('/config/sync-recipes')
    async def config_sync_recipes():
        '''Download global recipes and _shared/_common from S3.'''
        from ultra.services import config_store
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None,
            config_store.sync_recipes_and_shared_from_s3,
        )
        return {'ok': True}

    @router.get('/recipes/{slug}/yaml')
    async def recipe_yaml_get(slug: str):
        '''Return raw YAML for a recipe.'''
        from ultra.protocol import recipe_loader as rl
        from ultra.services import config_store
        loop = asyncio.get_running_loop()

        def _read() -> tuple[str, str]:
            return _read_yaml_cached(
                lambda: config_store.fetch_recipe_to_cache(slug),
                rl.RECIPES_DIR,
                f'{slug}.yaml',
            )

        try:
            text, source = await loop.run_in_executor(
                None, _read,
            )
        except FileNotFoundError:
            raise HTTPException(
                status_code=404, detail='Recipe not found',
            )
        return {
            'slug': slug,
            'yaml_text': text,
            'source': source,
        }

    @router.put('/recipes/{slug}/yaml')
    @router.post('/recipes/{slug}/yaml')
    async def recipe_yaml_put(slug: str, req: YamlTextBody):
        '''Validate and save a global recipe to S3.'''
        import yaml
        from ultra.protocol import recipe_loader as rl
        from ultra.services import config_store
        loop = asyncio.get_running_loop()

        def _validate_and_save() -> None:
            raw = yaml.safe_load(req.yaml_text) or {}
            recipe = rl.recipe_from_raw_dict(raw, slug)
            rl.validate_recipe(recipe)
            rl.lint_global_recipe_keys(recipe)
            config_store.put_recipe_yaml(slug, req.yaml_text)

        try:
            await loop.run_in_executor(
                None, _validate_and_save,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=str(exc),
            )
        return {
            'ok': True,
            'slug': slug,
            'message': f'Recipe "{slug}" saved to S3.',
        }

    @router.delete('/recipes/{slug}')
    async def recipe_delete(slug: str):
        '''Delete a recipe from S3.'''
        from ultra.services import config_store
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None, config_store.delete_recipe, slug,
            )
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f'Delete failed: {exc}',
            )
        return {
            'ok': True,
            'message': f'Recipe "{slug}" deleted.',
        }

    @router.get('/common-protocol/yaml')
    async def common_protocol_get():
        '''Return raw YAML for _common.yaml.'''
        from ultra.protocol import recipe_loader as rl
        from ultra.services import config_store
        loop = asyncio.get_running_loop()

        def _read() -> tuple[str, str]:
            return _read_yaml_cached(
                config_store.fetch_shared_common_to_cache,
                rl.RECIPES_DIR,
                '_common.yaml',
            )

        try:
            text, source = await loop.run_in_executor(
                None, _read,
            )
        except FileNotFoundError:
            raise HTTPException(
                status_code=404,
                detail='_common.yaml not found',
            )
        return {'yaml_text': text, 'source': source}

    @router.put('/common-protocol/yaml')
    @router.post('/common-protocol/yaml')
    async def common_protocol_put(req: YamlTextBody):
        '''Save _common.yaml to S3.'''
        import yaml
        from ultra.services import config_store
        loop = asyncio.get_running_loop()

        def _validate_and_save() -> None:
            raw = yaml.safe_load(req.yaml_text)
            if not isinstance(raw, dict):
                raise ValueError(
                    '_common.yaml must be a YAML mapping',
                )
            config_store.put_shared_common_yaml(req.yaml_text)

        try:
            await loop.run_in_executor(
                None, _validate_and_save,
            )
        except ValueError as exc:
            raise HTTPException(
                status_code=400, detail=str(exc),
            )
        return {
            'ok': True,
            'message': 'Common protocol saved to S3.',
        }

    @router.get('/protocol/step-types')
    async def protocol_step_types():
        '''List registered protocol step type names.'''
        from ultra.protocol.steps import STEP_REGISTRY
        return {'step_types': sorted(STEP_REGISTRY.keys())}

    @router.get('/protocol/step-schemas')
    async def protocol_step_schemas():
        '''Return step types with parameter schemas for the GUI builder.'''
        from ultra.protocol.steps import (
            STEP_DESCRIPTIONS,
            STEP_SCHEMAS,
        )
        return {
            'schemas': STEP_SCHEMAS,
            'descriptions': STEP_DESCRIPTIONS,
        }

    # -------------------------------------------------------------- #
    # Calibration data endpoints                                      #
    # -------------------------------------------------------------- #

    @router.get('/calibration')
    async def calibration_list():
        '''List all assays and their calibration versions.'''
        from ultra.services import config_store
        loop = asyncio.get_running_loop()
        tree = await loop.run_in_executor(
            None, config_store.list_calibration_tree,
        )
        default_assay = app.config.get('analysis', {}).get(
            'default_assay', '',
        )
        default_version = app.config.get('analysis', {}).get(
            'default_calibration_version', '',
        )
        return {
            'assays': tree,
            'default_assay': default_assay,
            'default_version': default_version,
        }

    @router.get('/calibration/{assay}/{version}/config')
    async def calibration_config_get(assay: str, version: str):
        '''Return analysis_config.yaml text for a version.'''
        from ultra.services import config_store
        loop = asyncio.get_running_loop()

        def _read() -> str | None:
            p = config_store.fetch_calibration_file(
                assay, version, 'analysis_config.yaml',
            )
            if p:
                import os.path as op
                if op.isfile(p):
                    with open(p, encoding='utf-8') as fh:
                        return fh.read()
            return None

        text = await loop.run_in_executor(None, _read)
        if text is None:
            raise HTTPException(
                404,
                detail=f'Config not found: {assay}/{version}',
            )
        return {
            'assay': assay,
            'version': version,
            'yaml_text': text,
        }

    @router.post('/calibration/{assay}/{version}/config')
    @router.put('/calibration/{assay}/{version}/config')
    async def calibration_config_put(
            assay: str, version: str, req: YamlTextBody,
    ):
        '''Save analysis_config.yaml for a version.'''
        import yaml
        from ultra.services import config_store
        loop = asyncio.get_running_loop()

        def _validate_and_save() -> None:
            parsed = yaml.safe_load(req.yaml_text)
            if not isinstance(parsed, dict):
                raise ValueError(
                    'analysis_config.yaml must be a YAML mapping',
                )
            config_store.put_calibration_file(
                assay, version,
                'analysis_config.yaml',
                req.yaml_text.encode('utf-8'),
                content_type='application/x-yaml',
            )

        try:
            await loop.run_in_executor(
                None, _validate_and_save,
            )
        except ValueError as exc:
            raise HTTPException(400, detail=str(exc))
        return {
            'ok': True,
            'message': (
                f'Saved {assay}/{version}/analysis_config.yaml'
            ),
        }

    @router.get(
        '/calibration/{assay}/{version}/file/{filename}',
    )
    async def calibration_file_download(
            assay: str, version: str, filename: str,
    ):
        '''Download an Excel calibration file.'''
        allowed = {
            'fitting_protocol_sheet.xlsx',
            'validation_rules_sheet.xlsx',
        }
        if filename not in allowed:
            raise HTTPException(
                400,
                detail=f'Invalid filename: {filename}',
            )
        from ultra.services import config_store
        loop = asyncio.get_running_loop()
        data = await loop.run_in_executor(
            None,
            config_store.fetch_calibration_file_bytes,
            assay, version, filename,
        )
        if data is None:
            raise HTTPException(
                404,
                detail=f'{assay}/{version}/{filename} not found',
            )
        ct = (
            'application/vnd.openxmlformats-'
            'officedocument.spreadsheetml.sheet'
        )
        return Response(
            content=data,
            media_type=ct,
            headers={
                'Content-Disposition': (
                    f'attachment; filename="{filename}"'
                ),
            },
        )

    @router.post(
        '/calibration/{assay}/{version}/file/{filename}',
    )
    async def calibration_file_upload(
            assay: str,
            version: str,
            filename: str,
            file: UploadFile,
    ):
        '''Upload an Excel calibration file.'''
        allowed = {
            'fitting_protocol_sheet.xlsx',
            'validation_rules_sheet.xlsx',
        }
        if filename not in allowed:
            raise HTTPException(
                400,
                detail=f'Invalid filename: {filename}',
            )
        from ultra.services import config_store
        loop = asyncio.get_running_loop()
        data = await file.read()
        ct = (
            'application/vnd.openxmlformats-'
            'officedocument.spreadsheetml.sheet'
        )
        await loop.run_in_executor(
            None,
            config_store.put_calibration_file,
            assay, version, filename, data, ct,
        )
        return {
            'ok': True,
            'message': (
                f'Uploaded {assay}/{version}/{filename}'
            ),
        }

    @router.delete('/calibration/{assay}/{version}')
    async def calibration_version_delete(
            assay: str, version: str,
    ):
        '''Delete all files for a calibration version.'''
        from ultra.services import config_store
        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(
                None,
                config_store.delete_calibration_version,
                assay, version,
            )
        except Exception as exc:
            raise HTTPException(
                500, detail=f'Delete failed: {exc}',
            )
        return {
            'ok': True,
            'message': (
                f'Calibration {assay}/{version} deleted.'
            ),
        }

    return router
