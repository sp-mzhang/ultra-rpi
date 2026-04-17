'''ultra.services.s3_upload -- S3 ZIP uploader.

Ported from sway.egress.s3bktmgr. Zips a run directory and
uploads it to the configured S3 bucket under the same path
structure sway uses: ``Device/{device_sn}/{rg_dir}/{run_dir}.zip``

The S3 key layout and ZIP contents are byte-compatible with
sway so that Dollop ingestion pipelines work unchanged.
'''
from __future__ import annotations

import logging
import os
import os.path as op
import re
import socket
import zipfile
from typing import Any

LOG = logging.getLogger(__name__)

DEFAULT_BUCKET = 'siphox-home'
DEFAULT_REGION = 'us-east-2'


def _is_online() -> bool:
    '''Quick internet reachability check.'''
    try:
        socket.create_connection(('www.google.com', 80), 3)
        return True
    except OSError:
        return False


def _sanitize(name: str) -> str:
    '''Slugify a path component to match sway's sanitize.

    Lowercases, strips non-word/dot/dash chars, collapses
    whitespace to underscore.
    '''
    name = name.lower()
    name = re.sub(r'[^\w\s.\-]', '', name)
    name = re.sub(r'\s+', '_', name).strip('_')
    return name


def _sanitize_path(path: str) -> str:
    '''Sanitize each component of a path.'''
    parts = path.replace('\\', '/').split('/')
    return '/'.join(_sanitize(p) for p in parts if p)


def compute_run_s3_url(
        run_dir_path: str,
        bucket: str = DEFAULT_BUCKET,
        device_sn: str = 'ultra-unknown',
) -> str:
    '''Compute the prospective ``s3://`` URL for a run zip.

    The Dollop analysis pipeline reads
    ``run['remote_directory_path']`` to locate the zip on S3
    (see ``AnalysisHelper.fetch_run_file`` in analysis-model-
    store). Sway sets this at Run-creation time by computing
    the deterministic S3 key in advance. We do the same here
    so Dollop can resolve ``run.json`` et al. even before the
    egress service finishes uploading.

    The key layout matches ``S3Uploader.upload_run_zip``::

        Device/{device_sn}/{rg_name}/{run_name}.zip

    Args:
        run_dir_path: Absolute path to the run directory.
        bucket: Target S3 bucket.
        device_sn: Device serial number for the key prefix.

    Returns:
        ``s3://{bucket}/Device/{device_sn}/{rg}/{run}.zip``.
    '''
    rg_dir = op.dirname(run_dir_path)
    trim_dir = op.dirname(rg_dir)
    rel = op.relpath(run_dir_path, trim_dir)
    s3_rel = _sanitize_path(rel)
    s3_key = f'Device/{device_sn}/{s3_rel}.zip'
    return f's3://{bucket}/{s3_key}'


def zip_directory(
        dir_path: str,
        zip_path: str,
) -> str:
    '''Create a ZIP of a directory (matching sway format).

    Existing valid ZIPs are reused.  The archive root is the
    directory basename so that paths inside the ZIP read
    ``run_dir_name/tlv/data_1.tlv`` etc.

    Args:
        dir_path: Directory to compress.
        zip_path: Destination ZIP file path.

    Returns:
        Absolute path to the ZIP file.
    '''
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            if len(zf.filelist) > 0:
                return zip_path
    except (FileNotFoundError, zipfile.BadZipFile):
        pass

    os.makedirs(op.dirname(zip_path), exist_ok=True)
    base = op.basename(dir_path)
    with zipfile.ZipFile(
        zip_path, 'w', zipfile.ZIP_DEFLATED,
    ) as zf:
        for root, _dirs, files in os.walk(dir_path):
            for fname in files:
                full = op.join(root, fname)
                arcname = op.join(
                    base,
                    op.relpath(full, dir_path),
                )
                zf.write(full, arcname)
    LOG.info('Zipped %s -> %s', dir_path, zip_path)
    return zip_path


class S3Uploader:
    '''Upload run ZIPs to S3.

    Attributes:
        bucket: S3 bucket name.
        prefix: Device prefix (``Device/{sn}``).
        _client: boto3 S3 client (lazy).
    '''

    def __init__(
            self,
            bucket: str = DEFAULT_BUCKET,
            region: str = DEFAULT_REGION,
            device_sn: str = 'ultra-unknown',
            zip_temp_dir: str | None = None,
            max_bandwidth_bps: int | None = None,
    ) -> None:
        '''Initialise the uploader.

        Args:
            bucket: S3 bucket name.
            region: AWS region.
            device_sn: Device serial number for the
                S3 key prefix.
            zip_temp_dir: Optional temp dir for ZIPs.
                Defaults to sibling of run dir.
            max_bandwidth_bps: Upload bandwidth cap in
                bytes/sec. ``None`` or ``<= 0`` means
                unlimited (default).
        '''
        self.bucket = bucket
        self.region = region
        self.prefix = f'Device/{device_sn}'
        self.zip_temp_dir = zip_temp_dir
        self.max_bandwidth_bps = (
            max_bandwidth_bps
            if max_bandwidth_bps and max_bandwidth_bps > 0
            else None
        )
        self._client: Any = None
        self._transfer_cfg: Any = None

    def _get_client(self) -> Any:
        if self._client is None:
            import boto3
            from botocore.config import Config
            cfg = Config(
                region_name=self.region,
                signature_version='s3v4',
                retries={
                    'max_attempts': 10,
                    'mode': 'standard',
                },
            )
            self._client = boto3.client('s3', config=cfg)
        return self._client

    def _get_transfer_config(self) -> Any:
        '''Build a TransferConfig honouring the BW cap.

        Returns ``None`` when no cap is configured so callers
        can skip the ``Config=`` kwarg entirely.
        '''
        if self.max_bandwidth_bps is None:
            return None
        if self._transfer_cfg is None:
            from boto3.s3.transfer import TransferConfig
            self._transfer_cfg = TransferConfig(
                max_bandwidth=self.max_bandwidth_bps,
            )
        return self._transfer_cfg

    def get_run_s3_url(self, run_dir_path: str) -> str:
        '''Return the prospective ``s3://`` URL for a run zip.

        Uses this uploader's configured ``bucket`` and
        ``device_sn``-based prefix so the result matches
        whatever ``upload_run_zip`` will produce.
        '''
        device_sn = self.prefix.split('/', 1)[-1]
        return compute_run_s3_url(
            run_dir_path,
            bucket=self.bucket,
            device_sn=device_sn,
        )

    def upload_run_zip(
            self,
            run_dir_path: str,
    ) -> tuple[bool, str]:
        '''ZIP and upload a run directory.

        The S3 key follows sway's layout so Dollop
        ingestion works unchanged::

            Device/{sn}/{rg_name}/{run_name}.zip

        Args:
            run_dir_path: Absolute path to the run directory.

        Returns:
            (success_bool, local_zip_path).
        '''
        if not op.isdir(run_dir_path):
            LOG.error('Not a directory: %s', run_dir_path)
            return False, ''

        rg_dir = op.dirname(run_dir_path)
        trim_dir = op.dirname(rg_dir)

        rel = op.relpath(run_dir_path, trim_dir)
        s3_rel = _sanitize_path(rel)
        s3_key = f'{self.prefix}/{s3_rel}.zip'

        zip_base = self.zip_temp_dir or rg_dir
        zip_name = f'{op.basename(run_dir_path)}.zip'
        zip_path = op.join(zip_base, zip_name)

        zip_directory(run_dir_path, zip_path)

        if not _is_online():
            LOG.error('No internet -- cannot upload')
            return False, zip_path

        try:
            client = self._get_client()
            transfer_cfg = self._get_transfer_config()
            if transfer_cfg is not None:
                LOG.info(
                    'Uploading %s -> s3://%s/%s '
                    '(bw cap %d B/s)',
                    zip_path, self.bucket, s3_key,
                    self.max_bandwidth_bps,
                )
            else:
                LOG.info(
                    'Uploading %s -> s3://%s/%s',
                    zip_path, self.bucket, s3_key,
                )
            with open(zip_path, 'rb') as fh:
                kwargs: dict[str, Any] = {}
                if transfer_cfg is not None:
                    kwargs['Config'] = transfer_cfg
                client.upload_fileobj(
                    fh, self.bucket, s3_key, **kwargs,
                )
            LOG.info('Upload complete: %s', s3_key)
            return True, zip_path
        except Exception as err:
            LOG.error('S3 upload failed: %s', err)
            return False, zip_path
