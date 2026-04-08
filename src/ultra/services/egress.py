'''ultra.services.egress -- Background egress service.

Ported from sway.egress.egressservice. Runs an async loop
that polls the local SQLite database for unegressed runs,
zips them to S3, creates/updates Dollop metadata, and
handles recovery for failed API calls.

The upload format (ZIP layout, S3 key structure, Dollop
payloads) is kept identical to sway so that downstream
ingestion pipelines work unchanged.

Usage::

    svc = EgressService(config, event_bus)
    await svc.start()   # runs forever
'''
from __future__ import annotations

import asyncio
import logging
import shutil
import time
from typing import Any

from ultra.events import EventBus
from ultra.services import dollop_client as dollop
from ultra.services import egress_db as edb
from ultra.services import s3_upload
from ultra.services.run_data import RG_FILE_TUP

LOG = logging.getLogger(__name__)

DEFAULT_LOOP_SLEEP_S = 10.0


class EgressService:
    '''Background service that uploads run data to S3 + Dollop.

    Mirrors sway's ``EgressService.egress_main`` loop:
      1. Poll DB for next unegressed run
      2. Ensure RunGroup + Run exist on Dollop
      3. Copy RunGroup files into run dir
      4. ZIP and upload to S3
      5. Mark as egressed locally + on Dollop
      6. When all runs in RunGroup egressed, mark
         RunGroup as egressed on Dollop
      7. Process recovery tables

    Attributes:
        _db: EgressDB instance.
        _s3: S3Uploader instance.
        _event_bus: Application event bus.
        _loop_sleep_s: Seconds between poll cycles.
    '''

    def __init__(
            self,
            config: dict[str, Any],
            event_bus: EventBus,
    ) -> None:
        '''Initialise the egress service.

        Args:
            config: Full application config. Reads the
                ``egress`` sub-dict for db_path, S3 bucket,
                Dollop endpoint, device_sn, etc.
            event_bus: Application event bus.
        '''
        self._event_bus = event_bus
        egress_cfg = config.get('egress', {})

        db_path = egress_cfg.get('db_path', '')
        self._db = edb.create_db(db_path)

        device_sn = egress_cfg.get(
            'device_sn',
            config.get('device_sn', 'ultra-001'),
        )
        self._s3 = s3_upload.S3Uploader(
            bucket=egress_cfg.get(
                's3_bucket', s3_upload.DEFAULT_BUCKET,
            ),
            region=egress_cfg.get(
                's3_region', s3_upload.DEFAULT_REGION,
            ),
            device_sn=device_sn,
            zip_temp_dir=egress_cfg.get(
                'zip_temp_dir', None,
            ),
        )

        dollop_host = egress_cfg.get(
            'dollop_host', '',
        )
        dollop_port = egress_cfg.get(
            'dollop_port', 8080,
        )
        if dollop_host:
            dollop.configure(dollop_host, dollop_port)

        self._loop_sleep_s: float = egress_cfg.get(
            'loop_sleep_s', DEFAULT_LOOP_SLEEP_S,
        )
        self._prev_rowid: int | None = None
        self._num_egressed = 0
        self._num_errors = 0
        self._hb_ts: float = 0.0

        self._event_bus.on(
            'protocol_done', self._on_protocol_done,
        )

    # ----------------------------------------------------------
    # public
    # ----------------------------------------------------------

    async def start(self) -> None:
        '''Run the egress loop forever.'''
        LOG.info('EgressService starting')
        await asyncio.sleep(0.5)
        while True:
            try:
                await self._tick()
            except Exception:
                LOG.exception('Egress tick error')
            await asyncio.sleep(self._loop_sleep_s)

    # ----------------------------------------------------------
    # event handler
    # ----------------------------------------------------------

    async def _on_protocol_done(
            self, data: dict[str, Any],
    ) -> None:
        '''Insert completed runs into the egress DB.

        Listens for ``protocol_done`` events emitted by the
        protocol runner, which include the
        ``run_uuid_dir_list`` in sway's RunDirTuple format.
        '''
        rdt_list = data.get('run_uuid_dir_list', [])
        for rdt in rdt_list:
            if hasattr(rdt, 'run_uuid'):
                self._db.insert_run(
                    run_uuid=rdt.run_uuid,
                    run_id=rdt.run_id,
                    rungroup_uuid=rdt.rungroup_uuid,
                    rungroup_id=rdt.rungroup_id,
                    rundate_ts_str=edb.get_current_ts_str(),
                    run_dir_path=rdt.run_dir_path,
                    rungroup_dir_path=rdt.rungroup_dir_path,
                    complete=1,
                )
            elif isinstance(rdt, (list, tuple)) and len(rdt) >= 6:
                self._db.insert_run(
                    run_uuid=rdt[0],
                    run_id=rdt[1],
                    rungroup_uuid=rdt[2],
                    rungroup_id=rdt[3],
                    rundate_ts_str=edb.get_current_ts_str(),
                    run_dir_path=rdt[4],
                    rungroup_dir_path=rdt[5],
                    complete=1,
                )
        LOG.info(
            'Queued %d run(s) for egress', len(rdt_list),
        )

    # ----------------------------------------------------------
    # main loop tick
    # ----------------------------------------------------------

    def _emit_egress_event(
            self,
            event_type: str,
            run_tup: edb.EgressTuple,
    ) -> None:
        '''Emit an egress event with summary counts.

        Args:
            event_type: One of ``egress_started``,
                ``egress_done``, ``egress_error``.
            run_tup: The run being processed.
        '''
        summary = self._db.get_summary()
        self._event_bus.emit_sync(
            event_type,
            {
                'run_uuid': run_tup.run_uuid,
                'run_dir_path': run_tup.run_dir_path,
                **summary,
            },
        )

    async def _tick(self) -> None:
        self._heartbeat()

        run_tup = self._db.get_unegressed_run(
            prev_rowid=self._prev_rowid,
        )
        if run_tup is None:
            self._prev_rowid = None
            self._process_recovery()
            return

        LOG.info('Egressing run %s', run_tup.run_uuid[:8])
        self._emit_egress_event(
            'egress_started', run_tup,
        )
        try:
            ok, ts = self._egress_run(run_tup)
        except Exception as err:
            LOG.warning(
                'Egress error for %s: %s',
                run_tup.run_uuid[:8], err,
            )
            self._num_errors += 1
            self._db.mark_egress_error(run_tup.run_uuid)
            self._emit_egress_event(
                'egress_error', run_tup,
            )
            return

        if not ok or not ts:
            self._num_errors += 1
            self._db.mark_egress_error(run_tup.run_uuid)
            self._emit_egress_event(
                'egress_error', run_tup,
            )
            return

        self._db.mark_egressed(
            rowid=run_tup.rowid,
            run_uuid=run_tup.run_uuid,
            egress_ts_str=ts,
        )

        self._emit_egress_event(
            'egress_done', run_tup,
        )
        self._update_dollop_run(run_tup, ts)
        self._check_rungroup_complete(run_tup)
        self._prev_rowid = None
        self._num_egressed += 1
        self._process_recovery()

    # ----------------------------------------------------------
    # egress one run
    # ----------------------------------------------------------

    def _egress_run(
            self,
            tup: edb.EgressTuple,
    ) -> tuple[bool, str]:
        '''Egress a single run: Dollop check + S3 upload.

        Returns:
            (success, egress_timestamp_str).
        '''
        self._ensure_dollop_entries(tup)
        self._copy_rg_files(tup)

        ok, zip_path = self._s3.upload_run_zip(
            tup.run_dir_path,
        )
        ts = edb.get_current_ts_str()

        if ok and zip_path:
            self._db.insert_s3_deletion(
                egress_ts_str=ts,
                run_dir_path=tup.run_dir_path,
                zip_path=zip_path,
                run_id=tup.run_id,
                run_uuid=tup.run_uuid,
                rungroup_dir_path=tup.rungroup_dir_path,
                rungroup_id=tup.rungroup_id,
                rungroup_uuid=tup.rungroup_uuid,
            )
        return ok, ts if ok else ''

    def _ensure_dollop_entries(
            self, tup: edb.EgressTuple,
    ) -> None:
        '''Create RunGroup + Run on Dollop if not yet done.'''
        if tup.rungroup_id == edb.DOLLOP_DEFAULT_ID:
            try:
                rg_dict = dollop.read_rungroup_json(
                    tup.rungroup_dir_path,
                )
            except OSError:
                LOG.warning(
                    'Cannot read rungroup.json: %s',
                    tup.rungroup_dir_path,
                )
                return

            rg_id, _ = dollop.create_rungroup(rg_dict)
            if rg_id != dollop.DOLLOP_DEFAULT_ID:
                self._db.set_api_rungroup_id(
                    rungroup_uuid=rg_dict['rungroup_uuid'],
                    rungroup_id=rg_id,
                )
                rg_dict['rungroup_id'] = rg_id
                dollop.write_rungroup_json(
                    tup.rungroup_dir_path, rg_dict,
                )

        if tup.run_id == edb.DOLLOP_DEFAULT_ID:
            try:
                run_dict = dollop.read_run_json(
                    tup.run_dir_path,
                )
            except OSError:
                LOG.warning(
                    'Cannot read run.json: %s',
                    tup.run_dir_path,
                )
                return

            run_dict['local_directory_path'] = (
                tup.run_dir_path
            )
            run_dict['reader_device_sn'] = 'reader1'

            if run_dict.get(
                'rungroup_id', -1,
            ) == edb.DOLLOP_DEFAULT_ID:
                run_dict['rungroup_id'] = tup.rungroup_id

            run_id = dollop.create_run(run_dict)
            if run_id != dollop.DOLLOP_DEFAULT_ID:
                self._db.set_api_run_id(
                    run_uuid=tup.run_uuid,
                    run_id=run_id,
                )

    def _copy_rg_files(
            self, tup: edb.EgressTuple,
    ) -> None:
        '''Copy RunGroup-level files into run dir.'''
        import os.path as op
        for fn in RG_FILE_TUP:
            src = op.join(tup.rungroup_dir_path, fn)
            if op.isfile(src):
                shutil.copy2(
                    src,
                    op.join(tup.run_dir_path, fn),
                )

    # ----------------------------------------------------------
    # post-upload Dollop updates
    # ----------------------------------------------------------

    def _update_dollop_run(
            self,
            tup: edb.EgressTuple,
            ts: str,
    ) -> None:
        '''Update Dollop with uploaded_at timestamp.'''
        result = dollop.update_run(
            tup.run_id,
            {'uploaded_at': ts},
        )
        if not result:
            self._db.insert_dollop_recovery_run(
                run_id=tup.run_id,
                uploaded_at_str=ts,
                remote_directory_path=tup.run_dir_path,
            )

    def _check_rungroup_complete(
            self, tup: edb.EgressTuple,
    ) -> None:
        '''If all runs in RunGroup are egressed, mark it.'''
        if tup.rungroup_id == edb.DOLLOP_DEFAULT_ID:
            return
        total = self._db.count_rungroup_runs(
            tup.rungroup_id,
        )
        done = self._db.count_egressed_runs(
            tup.rungroup_id,
        )
        if total > 0 and done >= total:
            result = dollop.update_rungroup(
                tup.rungroup_id,
                {'run_state': 'egressed'},
            )
            if not result:
                self._db.insert_dollop_recovery(
                    tup.rungroup_id,
                    tup.rungroup_dir_path,
                )
            else:
                LOG.info(
                    'RunGroup %d fully egressed',
                    tup.rungroup_id,
                )

    # ----------------------------------------------------------
    # recovery
    # ----------------------------------------------------------

    def _process_recovery(self) -> None:
        '''Retry failed Dollop RunGroup and Run updates.'''
        rg_rec = self._db.get_dollop_recovery_rg()
        if rg_rec:
            _, rg_id, rg_path = rg_rec
            result = dollop.update_rungroup(
                rg_id, {'run_state': 'egressed'},
            )
            if result:
                self._db.delete_dollop_recovery_rg(rg_path)
                LOG.info(
                    'Recovered RunGroup %d on Dollop', rg_id,
                )

        run_rec = self._db.get_dollop_recovery_run()
        if run_rec:
            _, run_id, uploaded_at, remote_dir = run_rec
            result = dollop.update_run(
                run_id,
                {
                    'uploaded_at': uploaded_at,
                    'remote_directory_path': remote_dir,
                },
            )
            if result:
                self._db.delete_dollop_recovery_run(run_id)
                LOG.info(
                    'Recovered Run %d on Dollop', run_id,
                )

    # ----------------------------------------------------------
    # heartbeat
    # ----------------------------------------------------------

    def _heartbeat(self) -> None:
        now = time.time()
        if now - self._hb_ts > edb.HEARTBEAT_INTERVAL_S:
            self._hb_ts = now
            self._db.update_heartbeat(
                edb.get_current_ts_str(),
            )
