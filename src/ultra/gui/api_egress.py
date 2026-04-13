'''Egress (data upload) status endpoints.

Handles /egress/status, /egress/runs, /egress/clear,
and /egress/clear_uploaded.
'''
from __future__ import annotations

from typing import TYPE_CHECKING

from fastapi import APIRouter, HTTPException

if TYPE_CHECKING:
    from ultra.app import Application


def create_egress_router(app: 'Application') -> APIRouter:
    router = APIRouter()

    def _get_egress_db():
        svc = getattr(app, '_egress_svc', None)
        if svc is None:
            return None
        return getattr(svc, '_db', None)

    @router.get('/egress/status')
    async def egress_status():
        '''Return egress summary counts.'''
        db = _get_egress_db()
        if db is None:
            return {
                'total': 0, 'egressed': 0,
                'pending': 0, 'errored': 0,
            }
        return db.get_summary()

    @router.get('/egress/runs')
    async def egress_runs():
        '''Return all egress runs as JSON list.'''
        db = _get_egress_db()
        if db is None:
            return []
        rows = db.get_all_runs()
        return [
            {
                'rowid': r.rowid,
                'rundate_ts': r.rundate_ts,
                'run_uuid': r.run_uuid,
                'run_id': r.run_id,
                'rungroup_uuid': r.rungroup_uuid,
                'is_egressed': bool(r.is_egressed),
                'egress_ts': r.egress_ts,
                'run_dir_path': r.run_dir_path,
                'egress_errors': r.egress_errors,
            }
            for r in rows
        ]

    @router.post('/egress/clear')
    async def egress_clear():
        '''Mark all pending runs as egressed.'''
        db = _get_egress_db()
        if db is None:
            raise HTTPException(
                status_code=503,
                detail='Egress not enabled',
            )
        n = db.clear_all()
        return {'cleared': n}

    @router.post('/egress/clear_uploaded')
    async def egress_clear_uploaded():
        '''Delete only successfully uploaded (egressed) runs.'''
        db = _get_egress_db()
        if db is None:
            raise HTTPException(
                status_code=503,
                detail='Egress not enabled',
            )
        n = db.clear_egressed()
        return {'deleted': n}

    return router
