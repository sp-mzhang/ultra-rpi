'''ultra.services.dollop_client -- Dollop REST API helpers.

Ported from sway.utils.dollop_helpers. Provides the subset
of Dollop API calls required for egress: create/update
RunGroups and Runs, and submit analysis templates.

Uses the ``siphox.dollopclient`` package (same as sway)
so that all API payloads are wire-compatible.
'''
from __future__ import annotations

import json
import logging
import os.path as op
from typing import Any

LOG = logging.getLogger(__name__)

DOLLOP_DEFAULT_ID = -1
DOLLOP_SKIP_ID = -2

_HOST: str = ''
_PORT: int = 8080


def configure(host: str, port: int) -> None:
    '''Set the Dollop API endpoint.

    Args:
        host: Dollop API host (IP or hostname).
        port: Dollop API port.
    '''
    global _HOST, _PORT
    _HOST = host
    _PORT = port
    LOG.info('Dollop endpoint: %s:%d', host, port)


def _client() -> Any:
    '''Create a DollopClient instance.'''
    from siphox.dollopclient.client import (
        dollop_client as dc,
    )
    return dc.DollopClient(
        host=_HOST, port=_PORT, logger=LOG,
    )


def _runs_api() -> Any:
    '''Create a RunsAPI client.'''
    from siphox.dollopclient.api import runs as r
    return r.RunsAPI(client=_client())


_dollop_err_logged = False


def _safe_call(func: Any, **kwargs: Any) -> Any:
    '''Call a dollopclient method, return {} on error.'''
    global _dollop_err_logged
    try:
        result = func(**kwargs)
        _dollop_err_logged = False
        return result
    except Exception as err:
        if not _dollop_err_logged:
            LOG.warning('Dollop API unreachable: %s', err)
            _dollop_err_logged = True
        else:
            LOG.debug('Dollop API error: %s -- %s', func, err)
        return {}


# ----------------------------------------------------------
# Reader device config
# ----------------------------------------------------------

def fetch_reader_config(
        reader_name: str = 'reader7',
) -> dict[str, Any]:
    '''Fetch reader calibration parameters from Dollop.

    Looks up the device by name (e.g. ``reader7``), then
    retrieves its config dict containing tia_gain, fsr_nm,
    rth thresholds, wavemeter parameters, etc.

    Args:
        reader_name: Dollop device name for the reader.

    Returns:
        Config dict with numeric values coerced to float,
        or empty dict on failure.
    '''
    from siphox.dollopclient.api import (
        devices as ddev,
    )

    if not reader_name.startswith('reader'):
        reader_name = f'reader{reader_name}'

    dev_api = ddev.DevicesAPI(client=_client())
    dev_list = _safe_call(
        dev_api.get_devices,
        filters=[{
            'col': 'name', 'opr': 'eq',
            'value': reader_name,
        }],
        page_size=1,
    )
    if not dev_list:
        LOG.warning(
            'Reader device not found on Dollop: %s',
            reader_name,
        )
        return {}

    device_uuid = dev_list[0]['id']
    config = _safe_call(
        dev_api.get_device_config,
        device_id=device_uuid,
    )
    if not config:
        LOG.warning(
            'No config for reader %s (id=%s)',
            reader_name, device_uuid,
        )
        return {}

    for k, v in list(config.items()):
        try:
            config[k] = float(v)
        except (ValueError, TypeError):
            pass

    LOG.info(
        'Fetched reader config for %s: %d keys',
        reader_name, len(config),
    )
    return config


# ----------------------------------------------------------
# RunGroup
# ----------------------------------------------------------

def create_rungroup(
        rg_dict: dict[str, Any],
) -> tuple[int, str]:
    '''Create a RunGroup on Dollop.

    Args:
        rg_dict: Full rungroup.json-compatible dict with
            keys matching sway's RunGroup schema.

    Returns:
        (rungroup_id, run_state) or
        (DOLLOP_DEFAULT_ID, '') on failure.
    '''
    api = _runs_api()
    rg = _safe_call(
        api.create_rungroup,
        rungroup={
            'uuid': rg_dict['rungroup_uuid'],
            'rungroup_name': rg_dict['rungroup_name'],
            'description': rg_dict.get(
                'rungroup_description', '',
            ),
            'station_id': rg_dict.get('station_id', 1),
            'rungroup_protocol': rg_dict.get(
                'rungroup_protocol', {},
            ),
            'well_plate': rg_dict.get('well_plate', {}),
            'operator': rg_dict.get('operator', 'user'),
            'meta': rg_dict.get('meta', {}),
        },
        devices=rg_dict.get('rungroup_subdevices', []),
    )
    if not rg:
        return DOLLOP_DEFAULT_ID, ''

    rg_id = rg['id']
    _safe_call(
        api.update_rungroup,
        id=rg_id,
        rungroup={'run_state': 'ready'},
    )
    return rg_id, 'ready'


def update_rungroup(
        rungroup_id: int,
        params: dict[str, Any],
) -> dict[str, Any]:
    '''Update a RunGroup on Dollop.

    Args:
        rungroup_id: Dollop RunGroup integer ID.
        params: Fields to update.

    Returns:
        Updated dict or {} on failure.
    '''
    if rungroup_id in (DOLLOP_DEFAULT_ID, DOLLOP_SKIP_ID):
        return {}
    api = _runs_api()
    return _safe_call(
        api.update_rungroup,
        id=rungroup_id,
        rungroup=params,
    )


def create_run(
        run_dict: dict[str, Any],
        reader_dollop_name: str = '',
) -> int:
    '''Create a Run on Dollop.

    The ``run_dict`` must contain all fields sway writes
    into run.json (run_uuid, rungroup_id, chip_pos,
    chip_id, reader_sn, note, meta, local_directory_path,
    log_file_path).

    Args:
        run_dict: run.json-compatible dict.
        reader_dollop_name: Dollop device name for the
            reader (e.g. ``reader1``).  When provided this
            overrides the ``reader_sn`` field in
            *run_dict* for the device lookup.

    Returns:
        Dollop run ID, or DOLLOP_DEFAULT_ID on failure.
    '''
    from siphox.dollopclient.api import (
        devices as ddev,
    )
    if reader_dollop_name:
        reader_sn = reader_dollop_name
    else:
        reader_sn = run_dict.get('reader_sn', '')
    if not reader_sn.startswith('reader'):
        reader_sn = f'reader{reader_sn}'

    dev_api = ddev.DevicesAPI(client=_client())
    dev_list = _safe_call(
        dev_api.get_devices,
        filters=[{
            'col': 'name', 'opr': 'eq',
            'value': reader_sn,
        }],
        page_size=1,
    )
    if not dev_list:
        LOG.error(
            'Reader device not found on Dollop: %s',
            reader_sn,
        )
        return DOLLOP_DEFAULT_ID

    reader_uuid = dev_list[0]['id']
    api = _runs_api()
    result = _safe_call(
        api.create_run,
        run={
            'uuid': run_dict['run_uuid'],
            'rungroup_id': run_dict['rungroup_id'],
            'rungroup_chip_pos': run_dict.get(
                'chip_pos', 0,
            ),
            'chip_id': run_dict.get('chip_id', ''),
            'reader_id': reader_uuid,
            'note': run_dict.get('note', ''),
            'meta': run_dict.get('meta', {}),
            'software_version': run_dict.get(
                'software_version', '0.1.0',
            ),
            'local_directory_path': run_dict.get(
                'local_directory_path', '',
            ),
            'log_file_path': run_dict.get(
                'log_file_path', '',
            ),
            'remote_directory_path': run_dict.get(
                'remote_directory_path', '',
            ),
        },
    )
    if not result:
        return DOLLOP_DEFAULT_ID
    return result['id']


def update_run(
        run_id: int,
        params: dict[str, Any],
) -> dict[str, Any]:
    '''Update a Run on Dollop.

    Args:
        run_id: Dollop Run integer ID.
        params: Fields to update (e.g. uploaded_at).

    Returns:
        Updated dict or {} on failure.
    '''
    if run_id in (DOLLOP_DEFAULT_ID, DOLLOP_SKIP_ID):
        return {}
    api = _runs_api()
    return _safe_call(
        api.update_run, id=run_id, run=params,
    )


# ----------------------------------------------------------
# RunGroup / Run JSON I/O (sway-compatible)
# ----------------------------------------------------------

def read_rungroup_json(
        rg_dir: str,
) -> dict[str, Any]:
    '''Read rungroup.json from a rungroup directory.

    Args:
        rg_dir: Path to the rungroup directory.

    Returns:
        Parsed rungroup dict.
    '''
    fp = op.join(rg_dir, 'rungroup.json')
    with open(fp, 'r') as fh:
        return json.load(fh)


def write_rungroup_json(
        rg_dir: str,
        data: dict[str, Any],
) -> None:
    '''Write rungroup.json to a rungroup directory.

    Uses sorted keys and indent=2 matching sway format.

    Args:
        rg_dir: Path to the rungroup directory.
        data: Full rungroup dict to write.
    '''
    fp = op.join(rg_dir, 'rungroup.json')
    with open(fp, 'w') as fh:
        json.dump(data, fh, sort_keys=True, indent=2)


def read_run_json(
        run_dir: str,
) -> dict[str, Any]:
    '''Read run.json from a run directory.

    Args:
        run_dir: Path to the run directory.

    Returns:
        Parsed run dict.
    '''
    fp = op.join(run_dir, 'run.json')
    with open(fp, 'r') as fh:
        return json.load(fh)
