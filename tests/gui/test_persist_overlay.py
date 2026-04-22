'''Tests for :func:`ultra.gui.api_config.persist_config_overlay`.

The helper underpins the tube-ROI save path (and any future
per-machine setting that needs restart durability), so the
read-modify-write semantics must be exact:

* unrelated keys in the overlay are preserved,
* mid-level non-dicts are overwritten rather than ignored,
* empty path_keys raises,
* a YAML file that isn't a mapping raises cleanly instead of
  corrupting state on subsequent writes.
'''
from __future__ import annotations

import pytest

from ultra.gui import api_config


@pytest.fixture
def fake_local_yaml(tmp_path, monkeypatch):
    '''Point ``LOCAL_MACHINE_YAML`` at a tmp path.

    We also stub out the S3 client by forcing ``device_sn`` to
    be empty so the upload branch never runs -- these tests are
    local-write-only.
    '''
    path = tmp_path / 'machine.yaml'
    monkeypatch.setattr(api_config, 'LOCAL_MACHINE_YAML', str(path))
    return str(path)


def test_persist_creates_overlay_when_absent(fake_local_yaml):
    result = api_config.persist_config_overlay(
        ['checks', 'tube', 'roi'],
        {'x': 10, 'y': 20, 'w': 30, 'h': 40},
        upload_s3=False,
    )
    assert result['local_written'] is True
    assert result['s3_uploaded'] is False

    import yaml
    with open(fake_local_yaml) as fh:
        parsed = yaml.safe_load(fh)
    assert parsed == {
        'checks': {'tube': {'roi': {
            'x': 10, 'y': 20, 'w': 30, 'h': 40,
        }}},
    }


def test_persist_preserves_sibling_keys(fake_local_yaml):
    # Seed the overlay with pre-existing device identity that
    # the operator set up through a different flow. The ROI
    # save must not touch it.
    import yaml
    with open(fake_local_yaml, 'w') as fh:
        yaml.safe_dump({
            'device_sn': 'ultra_42',
            'checks': {
                'qr': {'enabled': True},
                'tube': {'enabled': True, 'retries_per_close': 2},
            },
        }, fh)

    api_config.persist_config_overlay(
        ['checks', 'tube', 'roi'],
        {'x': 1, 'y': 2, 'w': 3, 'h': 4},
        upload_s3=False,
    )

    with open(fake_local_yaml) as fh:
        parsed = yaml.safe_load(fh)
    assert parsed['device_sn'] == 'ultra_42'
    assert parsed['checks']['qr'] == {'enabled': True}
    assert parsed['checks']['tube']['enabled'] is True
    assert parsed['checks']['tube']['retries_per_close'] == 2
    assert parsed['checks']['tube']['roi'] == {
        'x': 1, 'y': 2, 'w': 3, 'h': 4,
    }


def test_persist_overwrites_non_dict_midlevel(fake_local_yaml):
    # Pathological overlay where an intermediate key got set to
    # a scalar. The helper must recover by replacing it.
    import yaml
    with open(fake_local_yaml, 'w') as fh:
        yaml.safe_dump({'checks': {'tube': 'not-a-dict'}}, fh)

    api_config.persist_config_overlay(
        ['checks', 'tube', 'roi'],
        {'x': 1, 'y': 2, 'w': 3, 'h': 4},
        upload_s3=False,
    )
    with open(fake_local_yaml) as fh:
        parsed = yaml.safe_load(fh)
    assert parsed['checks']['tube']['roi'] == {
        'x': 1, 'y': 2, 'w': 3, 'h': 4,
    }


def test_persist_rejects_empty_path(fake_local_yaml):
    with pytest.raises(ValueError):
        api_config.persist_config_overlay(
            [], {'x': 0}, upload_s3=False,
        )


def test_persist_rejects_nonmapping_overlay(
    fake_local_yaml, tmp_path,
):
    with open(fake_local_yaml, 'w') as fh:
        fh.write('- just\n- a\n- list\n')
    with pytest.raises(ValueError):
        api_config.persist_config_overlay(
            ['checks', 'tube', 'roi'], {'x': 0},
            upload_s3=False,
        )


def test_persist_s3_upload_best_effort(fake_local_yaml, monkeypatch):
    # device_sn is set + upload_s3=True but the upload call
    # raises: the local write must still report success, and
    # the error must be surfaced for the GUI to display.
    fake_cfg = {'device_sn': 'ultra_err'}

    def _boom(_sn, _yaml):
        raise RuntimeError('network down')

    import ultra.services.config_store as cs
    monkeypatch.setattr(cs, 'put_machine_settings_yaml', _boom)

    result = api_config.persist_config_overlay(
        ['foo'], {'bar': 1},
        upload_s3=True, app_config=fake_cfg,
    )
    assert result['local_written'] is True
    assert result['s3_uploaded'] is False
    assert result['s3_error'] == 'network down'


def test_persist_skips_s3_when_no_device_sn(fake_local_yaml):
    result = api_config.persist_config_overlay(
        ['foo'], {'bar': 1},
        upload_s3=True, app_config={},
    )
    assert result['local_written'] is True
    assert result['s3_uploaded'] is False
    assert result['s3_error'] and 'device_sn' in result['s3_error']


def test_persist_local_write_failure_surfaces(tmp_path, monkeypatch):
    # Point LOCAL_MACHINE_YAML at a path we cannot create.
    # Using a regular file in place of a directory so
    # os.makedirs raises FileExistsError / NotADirectoryError.
    blocker = tmp_path / 'blocker'
    blocker.write_text('blocks the dir')
    monkeypatch.setattr(
        api_config, 'LOCAL_MACHINE_YAML',
        str(blocker / 'sub' / 'machine.yaml'),
    )
    result = api_config.persist_config_overlay(
        ['foo'], {'bar': 1}, upload_s3=False,
    )
    assert result['local_written'] is False
    # The yaml_text is still returned so callers know what
    # would have been written.
    assert 'foo:' in result['yaml_text']
