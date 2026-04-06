'''ultra.services.run_data -- Sway-compatible run directory writer.

Creates the exact on-disk layout that sway produces so that
the egress pipeline (S3 ZIP + Dollop metadata) works unchanged.

Directory structure::

    {data_dir}/{year}/{month:02d}/
      rg-{ts}-{user}-{name}-{rg_uuid[:8]}/
        rungroup.json
        chip.log
        R{reader_sn}-{chip_id}-{note}-{ts}/
          run.json
          run_cal.json
          chip.log
          tlv/
            data_1.tlv  ...
          spectra/
            peaks_nm.log  ...
'''
from __future__ import annotations

import json
import logging
import os
import os.path as op
import re
import shutil
import uuid
from datetime import datetime, timezone
from typing import Any, NamedTuple

LOG = logging.getLogger(__name__)

LOCAL_TZ = datetime.now(timezone.utc).astimezone().tzinfo

RG_FILE_TUP = (
    'chip_temp.csv',
    'chip_temp.log',
    'chip.log',
    'config.log',
    'console.log',
    'spectrify_tlv.log',
    'spectrify_tlv_data_complete.txt',
)


class RunDirTuple(NamedTuple):
    '''Sway-compatible run directory record.

    Matches sway's ``shelpers.RunDirTuple`` exactly so that
    ``rungroup.json`` ``run_uuid_dir_list`` entries have the
    same 6-field structure.
    '''
    run_uuid: str
    run_id: int
    rungroup_uuid: str
    rungroup_id: int
    run_dir_path: str
    rungroup_dir_path: str

    def to_list(self) -> list[Any]:
        '''Serialise to a JSON-safe list.'''
        return [
            self.run_uuid, self.run_id,
            self.rungroup_uuid, self.rungroup_id,
            self.run_dir_path, self.rungroup_dir_path,
        ]


def _get_uuid() -> str:
    return str(uuid.uuid4())


def _ts_now() -> datetime:
    return datetime.now(tz=LOCAL_TZ)


def _ts_str(dt: datetime | None = None) -> str:
    '''ISO-8601 timestamp matching sway format.'''
    if dt is None:
        dt = _ts_now()
    return dt.isoformat()


def _ts_file(dt: datetime | None = None) -> str:
    '''Filesystem-safe timestamp matching sway's
    ``morph_ts_to_file_str_underscore``.

    Produces ``yyyy_mm_ddthh_mm_ss`` (lowercase, colons
    and dashes replaced, subseconds + tz stripped).
    '''
    s = _ts_str(dt).lower()
    s = s.replace('-', '_').replace(':', '_')
    return s[:19]


def _sanitize(name: str) -> str:
    '''Slugify matching sway's ``sanitize_file_path``.'''
    name = name.lower()
    name = re.sub(r'[^\w\s.\-]', '', name)
    name = re.sub(r'\s+', '_', name).strip('_')
    return name


class RunGroupWriter:
    '''Creates and manages a sway-compatible RunGroup on disk.

    Attributes:
        rg_dir: Absolute path to the rungroup directory.
        rg_uuid: RunGroup UUID.
        rg_dict: Full rungroup.json contents.
    '''

    def __init__(
            self,
            data_dir: str,
            user: str = 'user',
            name: str = 'ultra',
            config: dict[str, Any] | None = None,
            device_sn: str = 'ultra-001',
            station_id: int = 1,
    ) -> None:
        '''Create a new RunGroup directory.

        Args:
            data_dir: Root data directory
                (e.g. ``/var/lib/ultra/data``).
            user: Operator / user name.
            name: Short rungroup name.
            config: Full device configuration dict
                written into rungroup.json.
            device_sn: Device serial number.
            station_id: Station ID for Dollop.
        '''
        self.rg_uuid = _get_uuid()
        dt = _ts_now()
        ts_file = _ts_file(dt)

        dir_name = _sanitize(
            f'rg-{ts_file}-{user}-{name}'
            f'-{self.rg_uuid[:8]}',
        )
        date_dir = op.join(
            data_dir,
            str(dt.year),
            f'{dt.month:02d}',
        )
        self.rg_dir = op.join(date_dir, dir_name)
        os.makedirs(self.rg_dir, exist_ok=True)

        self.rg_dict: dict[str, Any] = {
            'operator': user,
            'rungroup_name': name,
            'rungroup_description': '',
            'rungroup_uuid': self.rg_uuid,
            'rungroup_id': -1,
            'station_id': station_id,
            'run_uuid_dir_list': [],
            'chip_pos_id_note_rsn_list': [],
            'local_directory_path': self.rg_dir,
            'remote_directory_path': '',
            'run_meta': {},
            'config': config or {},
            'log_file_path': data_dir,
            'run_state': 'ready',
            'created_at': _ts_str(dt),
            'started_at': '',
            'ended_at': '',
            'uploaded_at': '',
            'meta': {},
            'device_sn': device_sn,
            'rungroup_subdevices': [],
            'rungroup_protocol': {},
            'well_plate': {},
        }
        self._write_rg_json()
        LOG.info(
            'RunGroup created: %s', self.rg_dir,
        )

    def _write_rg_json(self) -> None:
        fp = op.join(self.rg_dir, 'rungroup.json')
        with open(fp, 'w') as fh:
            json.dump(
                self.rg_dict, fh,
                sort_keys=True, indent=2,
            )

    def add_run(
            self, *,
            reader_sn: str,
            chip_id: str,
            note: str = '',
            chip_pos: int = 0,
            run_meta: dict[str, Any] | None = None,
            reader_cal: dict[str, Any] | None = None,
    ) -> tuple[str, RunDirTuple]:
        '''Create a run directory inside this RunGroup.

        Args:
            reader_sn: Reader serial number.
            chip_id: Chip ID string.
            note: Optional note.
            chip_pos: Chip position index.
            run_meta: Per-run metadata dict.
            reader_cal: Reader calibration (run_cal.json).

        Returns:
            (run_dir_path, RunDirTuple).
        '''
        run_uuid = _get_uuid()
        dt = _ts_now()
        ts_file = _ts_file(dt)
        dir_name = _sanitize(
            f'r{reader_sn}-{chip_id}-{note}-{ts_file}',
        )
        run_dir = op.join(self.rg_dir, dir_name)
        os.makedirs(run_dir, exist_ok=True)
        os.makedirs(op.join(run_dir, 'tlv'), exist_ok=True)
        os.makedirs(
            op.join(run_dir, 'spectra'), exist_ok=True,
        )

        rdt = RunDirTuple(
            run_uuid=run_uuid,
            run_id=-1,
            rungroup_uuid=self.rg_uuid,
            rungroup_id=self.rg_dict.get(
                'rungroup_id', -1,
            ),
            run_dir_path=run_dir,
            rungroup_dir_path=self.rg_dir,
        )

        self.rg_dict['run_uuid_dir_list'].append(
            rdt.to_list(),
        )
        self.rg_dict['chip_pos_id_note_rsn_list'].append(
            [chip_pos, chip_id, note, reader_sn],
        )
        if run_meta:
            self.rg_dict['run_meta'][reader_sn] = run_meta
        self._write_rg_json()

        self._write_chip_log(
            run_dir, run_uuid, reader_sn,
            chip_id, note,
        )
        if reader_cal:
            self._write_run_cal(run_dir, reader_cal)
        self._write_run_json(
            run_dir, run_uuid, chip_id,
            chip_pos, note, reader_sn, run_meta,
        )

        LOG.info('Run created: %s', run_dir)
        return run_dir, rdt

    def mark_started(self) -> None:
        '''Set run_state to started with timestamp.'''
        self.rg_dict['run_state'] = 'started'
        self.rg_dict['started_at'] = _ts_str()
        self._write_rg_json()

    def mark_completed(self) -> None:
        '''Set run_state to completed with timestamp.'''
        self.rg_dict['run_state'] = 'completed'
        self.rg_dict['ended_at'] = _ts_str()
        self._write_rg_json()

    def copy_rg_files_to_run(
            self, run_dir: str,
    ) -> None:
        '''Copy RunGroup-level files into a run dir.

        Matches sway's egress behavior of copying
        ``RG_FILE_TUP`` files before zipping.

        Args:
            run_dir: Destination run directory.
        '''
        for fn in RG_FILE_TUP:
            src = op.join(self.rg_dir, fn)
            if op.isfile(src):
                shutil.copy2(src, op.join(run_dir, fn))

    # ----------------------------------------------------------
    # private writers
    # ----------------------------------------------------------

    def _write_chip_log(
            self,
            run_dir: str,
            run_uuid: str,
            reader_sn: str,
            chip_id: str,
            note: str,
    ) -> None:
        '''Write chip.log in sway's plain-text format.'''
        lines = [
            f'User: {self.rg_dict["operator"]}',
            f'rungroup_name: '
            f'{self.rg_dict["rungroup_name"]}',
            f'rungroup_description: '
            f'{self.rg_dict["rungroup_description"]}',
            f'rungroup_uuid: {self.rg_uuid}',
            f'run_uuid: {run_uuid}',
            f'Reader ID: {reader_sn}',
            f'Chip ID: {chip_id}',
            f'Description: ',
            f'Note: {note}',
        ]
        fp = op.join(run_dir, 'chip.log')
        with open(fp, 'a') as fh:
            fh.write('\n'.join(lines) + '\n')

    def _write_run_cal(
            self,
            run_dir: str,
            cal: dict[str, Any],
    ) -> None:
        '''Write run_cal.json.'''
        fp = op.join(run_dir, 'run_cal.json')
        with open(fp, 'w') as fh:
            json.dump(cal, fh, sort_keys=True, indent=2)

    def _write_run_json(
            self,
            run_dir: str,
            run_uuid: str,
            chip_id: str,
            chip_pos: int,
            note: str,
            reader_sn: str,
            run_meta: dict[str, Any] | None,
    ) -> None:
        '''Write run.json (sway-compatible superset).'''
        d = dict(self.rg_dict)
        d['rungroup_meta'] = d.pop('meta', {})
        d['run_uuid'] = run_uuid
        d['run_id'] = -1
        d['chip_id'] = chip_id
        d['chip_pos'] = chip_pos
        d['note'] = note
        d['reader_sn'] = reader_sn
        d['reader_device_sn'] = f'reader{reader_sn}'
        d['meta'] = run_meta or {}

        cal_fp = op.join(run_dir, 'run_cal.json')
        if op.isfile(cal_fp):
            with open(cal_fp, 'r') as fh:
                d['reader_config'] = json.load(fh)
        else:
            d['reader_config'] = {}

        d['local_directory_path'] = run_dir

        fp = op.join(run_dir, 'run.json')
        with open(fp, 'w') as fh:
            json.dump(d, fh, sort_keys=True, indent=2)
