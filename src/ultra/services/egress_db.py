'''ultra.services.egress_db -- SQLite tracker for data egress.

Ported from sway.egress.egressdb. Tracks which runs are pending
upload, egress status, Dollop API recovery, and heartbeat.

Schema and table names are kept identical to sway so that the
same EgressDB file can be inspected with the same tooling.
'''
from __future__ import annotations

import logging
import os
import os.path as op
import sqlite3
import time
from datetime import datetime, timezone
from typing import NamedTuple

LOG = logging.getLogger(__name__)

DOLLOP_DEFAULT_ID = -1
DOLLOP_SKIP_ID = -2
DEFAULT_MAX_RETRIES = 3
HEARTBEAT_INTERVAL_S = 60.0
REPORT_INTERVAL_S = 600.0
DB_FILENAME = 'ultra-egress.db'

LOCAL_TZ = datetime.now(timezone.utc).astimezone().tzinfo


def get_current_ts_str() -> str:
    '''Return local ISO-8601 timestamp with timezone offset.

    Matches sway's ``shelpers.get_current_ts_str`` format:
    ``YYYY-MM-DDTHH:MM:SS.SSSSSS-HH:MM``

    Returns:
        ISO-8601 timestamp string.
    '''
    return datetime.now(tz=LOCAL_TZ).isoformat()


class EgressTuple(NamedTuple):
    '''Row from the egresstable.'''
    rowid: int
    rundate_ts: str
    run_uuid: str
    run_id: int
    rungroup_uuid: str
    rungroup_id: int
    is_egressed: int
    egress_ts: str
    run_dir_path: str
    rungroup_dir_path: str
    complete: int
    egress_errors: int


class EgressDB:
    '''SQLite database mirroring sway's egress schema.

    Attributes:
        con: sqlite3 connection.
        max_retries: Max egress error count before giving up.
    '''

    TBL_EGRESS = 'egresstable'
    TBL_HEARTBEAT = 'serviceheartbeattable'
    TBL_DOLLOP_RECOVERY = 'dolloprecoverytable'
    TBL_DOLLOP_RECOVERY_RUN = 'dolloprecoveryruntable'
    TBL_S3_DELETION = 'egresss3deletionruntable'

    def __init__(
            self,
            db_path: str,
            max_retries: int = DEFAULT_MAX_RETRIES,
    ) -> None:
        self.db_path = db_path
        os.makedirs(op.dirname(db_path), exist_ok=True)
        self.con = sqlite3.connect(db_path)
        self.max_retries = max_retries

    def close(self) -> None:
        '''Close database connection.'''
        self.con.close()

    # ----------------------------------------------------------
    # Initialisation
    # ----------------------------------------------------------

    def init_db(self) -> None:
        '''Create all tables if they do not exist.'''
        con = self.con
        con.execute('PRAGMA journal_mode=WAL')

        con.execute(f'''CREATE TABLE IF NOT EXISTS {self.TBL_EGRESS}(
            rundate_ts TEXT,
            run_uuid TEXT,
            run_id INTEGER,
            rungroup_uuid TEXT,
            rungroup_id INTEGER,
            is_egressed INTEGER,
            egress_ts TEXT,
            run_dir_path TEXT,
            rungroup_dir_path TEXT,
            complete INTEGER,
            egress_errors INTEGER DEFAULT 0
        )''')

        con.execute(f'''CREATE TABLE IF NOT EXISTS {self.TBL_HEARTBEAT}(
            heartbeat_ts TEXT
        )''')
        cur = con.execute(
            f'SELECT COUNT(*) FROM {self.TBL_HEARTBEAT}',
        )
        if cur.fetchone()[0] == 0:
            con.execute(
                f'INSERT INTO {self.TBL_HEARTBEAT}(heartbeat_ts)'
                " VALUES('1970-01-01T00:00:00.000000-04:00')",
            )

        con.execute(f'''CREATE TABLE IF NOT EXISTS
            {self.TBL_DOLLOP_RECOVERY}(
            rungroup_id INTEGER,
            rungroup_dir_path TEXT
        )''')

        con.execute(f'''CREATE TABLE IF NOT EXISTS
            {self.TBL_DOLLOP_RECOVERY_RUN}(
            run_id INTEGER,
            uploaded_at TEXT,
            remote_directory_path TEXT
        )''')

        con.execute(f'''CREATE TABLE IF NOT EXISTS
            {self.TBL_S3_DELETION}(
            egress_ts_float REAL,
            egress_ts_str TEXT,
            is_acceptable_to_delete INTEGER DEFAULT 0,
            run_dir_path TEXT,
            run_file_path_local_zip TEXT,
            run_id INTEGER,
            run_uuid TEXT,
            rungroup_dir_path TEXT,
            rungroup_id INTEGER,
            rungroup_uuid TEXT
        )''')
        con.commit()
        LOG.info('EgressDB initialised at %s', self.db_path)

    # ----------------------------------------------------------
    # Heartbeat
    # ----------------------------------------------------------

    def update_heartbeat(self, ts_str: str) -> None:
        '''Update heartbeat timestamp.'''
        with self.con:
            self.con.execute(
                f'UPDATE {self.TBL_HEARTBEAT} '
                'SET heartbeat_ts=? WHERE ROWID=1',
                (ts_str,),
            )

    # ----------------------------------------------------------
    # Run insert / query
    # ----------------------------------------------------------

    def insert_run(
            self, *,
            run_uuid: str,
            run_id: int,
            rungroup_uuid: str,
            rungroup_id: int,
            rundate_ts_str: str,
            run_dir_path: str,
            rungroup_dir_path: str,
            complete: int,
    ) -> None:
        '''Insert a new run or mark existing as non-egressed.'''
        existing = self._get_by_uuid(run_uuid)
        if existing is not None:
            with self.con:
                self.con.execute(
                    f'UPDATE {self.TBL_EGRESS} SET '
                    'rundate_ts=?, is_egressed=0 '
                    'WHERE run_uuid=?',
                    (rundate_ts_str, run_uuid),
                )
            return
        with self.con:
            self.con.execute(
                f'INSERT INTO {self.TBL_EGRESS}'
                '(rundate_ts, run_uuid, run_id, '
                'rungroup_uuid, rungroup_id, '
                'is_egressed, egress_ts, '
                'run_dir_path, rungroup_dir_path, '
                'complete, egress_errors) '
                'VALUES(?,?,?,?,?,0,"",?,?,?,0)',
                (
                    rundate_ts_str, run_uuid, run_id,
                    rungroup_uuid, rungroup_id,
                    run_dir_path, rungroup_dir_path,
                    complete,
                ),
            )

    def get_unegressed_run(
            self,
            prev_rowid: int | None = None,
    ) -> EgressTuple | None:
        '''Fetch the next run that needs egress.'''
        if prev_rowid is None:
            cur = self.con.execute(
                f'SELECT rowid, * FROM {self.TBL_EGRESS} '
                'WHERE is_egressed=0 AND egress_errors<=? '
                'ORDER BY ROWID ASC LIMIT 1',
                (self.max_retries,),
            )
        else:
            cur = self.con.execute(
                f'SELECT rowid, * FROM {self.TBL_EGRESS} '
                'WHERE is_egressed=0 AND ROWID>? '
                'AND egress_errors<=? '
                'ORDER BY ROWID ASC LIMIT 1',
                (prev_rowid, self.max_retries),
            )
        row = cur.fetchone()
        return EgressTuple(*row) if row else None

    def mark_egressed(
            self, *,
            rowid: int,
            run_uuid: str,
            egress_ts_str: str,
    ) -> None:
        '''Mark a run as successfully egressed.'''
        with self.con:
            self.con.execute(
                f'UPDATE {self.TBL_EGRESS} SET '
                'is_egressed=1, egress_ts=? '
                'WHERE ROWID=? OR run_uuid=?',
                (egress_ts_str, rowid, run_uuid),
            )

    def mark_egress_error(self, run_uuid: str) -> None:
        '''Increment egress error counter.'''
        with self.con:
            self.con.execute(
                f'UPDATE {self.TBL_EGRESS} SET '
                'egress_errors=egress_errors+1 '
                'WHERE run_uuid=?',
                (run_uuid,),
            )

    def set_api_rungroup_id(
            self, *,
            rungroup_uuid: str,
            rungroup_id: int,
    ) -> None:
        '''Store Dollop rungroup_id for a given UUID.'''
        with self.con:
            self.con.execute(
                f'UPDATE {self.TBL_EGRESS} SET '
                'rungroup_id=? WHERE rungroup_uuid=?',
                (rungroup_id, rungroup_uuid),
            )

    def set_api_run_id(
            self, *,
            run_uuid: str,
            run_id: int,
    ) -> None:
        '''Store Dollop run_id for a given UUID.'''
        with self.con:
            self.con.execute(
                f'UPDATE {self.TBL_EGRESS} SET '
                'run_id=? WHERE run_uuid=?',
                (run_id, run_uuid),
            )

    def count_rungroup_runs(
            self, rungroup_id: int,
    ) -> int:
        '''Count total runs in a rungroup.'''
        cur = self.con.execute(
            f'SELECT COUNT(rowid) FROM {self.TBL_EGRESS} '
            'WHERE rungroup_id=?',
            (rungroup_id,),
        )
        row = cur.fetchone()
        return row[0] if row else 0

    def count_egressed_runs(
            self, rungroup_id: int,
    ) -> int:
        '''Count egressed runs in a rungroup.'''
        cur = self.con.execute(
            f'SELECT COUNT(rowid) FROM {self.TBL_EGRESS} '
            'WHERE rungroup_id=? AND is_egressed=1',
            (rungroup_id,),
        )
        row = cur.fetchone()
        return row[0] if row else 0

    # ----------------------------------------------------------
    # Dollop recovery
    # ----------------------------------------------------------

    def insert_dollop_recovery(
            self, rungroup_id: int,
            rungroup_dir_path: str,
    ) -> None:
        '''Queue a rungroup for Dollop recovery.'''
        with self.con:
            self.con.execute(
                f'INSERT INTO {self.TBL_DOLLOP_RECOVERY}'
                '(rungroup_id, rungroup_dir_path) '
                'VALUES(?,?)',
                (rungroup_id, rungroup_dir_path),
            )

    def get_dollop_recovery_rg(
            self,
    ) -> tuple[int, int, str] | None:
        '''Get next rungroup needing Dollop recovery.

        Returns:
            (rowid, rungroup_id, rungroup_dir_path) or None.
        '''
        cur = self.con.execute(
            'SELECT rowid, rungroup_id, rungroup_dir_path '
            f'FROM {self.TBL_DOLLOP_RECOVERY} '
            'ORDER BY ROWID ASC LIMIT 1',
        )
        row = cur.fetchone()
        return tuple(row) if row else None  # type: ignore

    def delete_dollop_recovery_rg(
            self, rungroup_dir_path: str,
    ) -> None:
        '''Remove a resolved rungroup recovery entry.'''
        with self.con:
            self.con.execute(
                f'DELETE FROM {self.TBL_DOLLOP_RECOVERY} '
                'WHERE rungroup_dir_path=?',
                (rungroup_dir_path,),
            )

    def insert_dollop_recovery_run(
            self, *,
            run_id: int,
            uploaded_at_str: str,
            remote_directory_path: str,
    ) -> None:
        '''Queue a run for Dollop recovery.'''
        with self.con:
            self.con.execute(
                f'INSERT INTO {self.TBL_DOLLOP_RECOVERY_RUN}'
                '(run_id, uploaded_at, '
                'remote_directory_path) VALUES(?,?,?)',
                (
                    run_id, uploaded_at_str,
                    remote_directory_path,
                ),
            )

    def get_dollop_recovery_run(
            self,
    ) -> tuple[int, int, str, str] | None:
        '''Get next run needing Dollop recovery.

        Returns:
            (rowid, run_id, uploaded_at,
             remote_directory_path) or None.
        '''
        cur = self.con.execute(
            'SELECT rowid, run_id, uploaded_at, '
            'remote_directory_path '
            f'FROM {self.TBL_DOLLOP_RECOVERY_RUN} '
            'ORDER BY ROWID ASC LIMIT 1',
        )
        row = cur.fetchone()
        return tuple(row) if row else None  # type: ignore

    def delete_dollop_recovery_run(
            self, run_id: int,
    ) -> None:
        '''Remove a resolved run recovery entry.'''
        with self.con:
            self.con.execute(
                f'DELETE FROM {self.TBL_DOLLOP_RECOVERY_RUN}'
                ' WHERE run_id=?',
                (run_id,),
            )

    # ----------------------------------------------------------
    # S3 deletion tracking
    # ----------------------------------------------------------

    def insert_s3_deletion(
            self, *,
            egress_ts_str: str,
            run_dir_path: str,
            zip_path: str,
            run_id: int,
            run_uuid: str,
            rungroup_dir_path: str,
            rungroup_id: int,
            rungroup_uuid: str,
    ) -> None:
        '''Record a ZIP upload for future cleanup.'''
        ts_float = datetime.fromisoformat(
            egress_ts_str,
        ).timestamp()
        with self.con:
            self.con.execute(
                f'INSERT INTO {self.TBL_S3_DELETION}'
                '(egress_ts_float, egress_ts_str, '
                'is_acceptable_to_delete, run_dir_path, '
                'run_file_path_local_zip, run_id, '
                'run_uuid, rungroup_dir_path, '
                'rungroup_id, rungroup_uuid) '
                'VALUES(?,?,0,?,?,?,?,?,?,?)',
                (
                    ts_float, egress_ts_str,
                    run_dir_path, zip_path,
                    run_id, run_uuid,
                    rungroup_dir_path, rungroup_id,
                    rungroup_uuid,
                ),
            )

    # ----------------------------------------------------------
    # helpers
    # ----------------------------------------------------------

    def _get_by_uuid(
            self, run_uuid: str,
    ) -> EgressTuple | None:
        cur = self.con.execute(
            f'SELECT rowid, * FROM {self.TBL_EGRESS} '
            'WHERE run_uuid=?',
            (run_uuid,),
        )
        row = cur.fetchone()
        return EgressTuple(*row) if row else None


def create_db(db_path: str = '') -> EgressDB:
    '''Create and initialise an EgressDB.

    Args:
        db_path: Path to the SQLite file. Empty string
            uses a default under /var/lib/ultra/.

    Returns:
        Initialised EgressDB instance.
    '''
    if not db_path:
        db_dir = os.environ.get(
            'ULTRA_DATA_DIR', '/var/lib/ultra',
        )
        db_path = op.join(db_dir, DB_FILENAME)
    db = EgressDB(db_path)
    db.init_db()
    return db
