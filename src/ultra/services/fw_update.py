'''ultra.services.fw_update -- STM32 firmware OTA update.

Lists firmware builds from S3, downloads a selected binary,
and flashes it to the STM32H735 over UART using the custom
UART bootloader (ENTER_BOOTLOADER cmd + chunked transfer).

S3 bucket layout (set by ultra-firmware/rpi/upload.sh):
  siphox-ultra-firmware/
    latest/ultra_mcu.bin
    latest/manifest.json      {version, sha, branch, ...}
    archive/<ver>_ultra_mcu.bin
'''
from __future__ import annotations

import binascii
import hashlib
import json
import logging
import os
import struct
import threading
import time
from typing import Any

LOG = logging.getLogger(__name__)

FW_BUCKET = 'siphox-ultra-firmware'
FW_REGION = 'us-east-1'
DOWNLOAD_DIR = '/tmp/ultra_fw'

UART_PORT = '/dev/ttyAMA3'
APP_BAUD = 921600
BL_BAUD = 115200

ACK = 0x79
NACK = 0x1F
READY = 0x7F
CHUNK_SZ = 1024
MAX_APP_SIZE = 7 * 128 * 1024

_s3_client: Any = None
_flash_lock = threading.Lock()
_flash_status: dict[str, Any] = {
    'state': 'idle',
    'progress': 0,
    'message': '',
    'log': [],
}


def _get_s3():
    '''Return a lazily-created boto3 S3 client.'''
    global _s3_client
    if _s3_client is None:
        import boto3
        from botocore.config import Config
        cfg = Config(
            region_name=FW_REGION,
            signature_version='s3v4',
            retries={
                'max_attempts': 3,
                'mode': 'standard',
            },
        )
        _s3_client = boto3.client('s3', config=cfg)
    return _s3_client


def _log(msg: str) -> None:
    '''Append a line to the flash status log.'''
    _flash_status['log'].append(msg)
    LOG.info('fw_update: %s', msg)


def _set_status(
        state: str,
        progress: int = 0,
        message: str = '',
) -> None:
    '''Update the flash status dict.'''
    _flash_status['state'] = state
    _flash_status['progress'] = progress
    _flash_status['message'] = str(message)


def get_status(log_offset: int = 0) -> dict:
    '''Return current flash status with log from offset.

    Args:
        log_offset: Index into the log list; only lines
            from this index onward are returned.

    Returns:
        Dict with state, progress, message, log, log_total.
    '''
    full_log = _flash_status['log']
    return {
        'state': _flash_status['state'],
        'progress': _flash_status['progress'],
        'message': _flash_status['message'],
        'log': full_log[log_offset:],
        'log_total': len(full_log),
    }


# -------------------------------------------------------
# S3 listing
# -------------------------------------------------------

def list_firmware() -> list[dict]:
    '''List available firmware versions from S3.

    Enumerates the archive/ prefix, fetches
    latest/manifest.json and archive/manifests.json
    for per-version metadata (notes, sha, branch, etc).

    Returns:
        List of dicts with version, key, size, date,
        is_latest, notes, and optional manifest fields.
    '''
    s3 = _get_s3()

    manifest: dict = {}
    try:
        resp = s3.get_object(
            Bucket=FW_BUCKET,
            Key='latest/manifest.json',
        )
        manifest = json.loads(
            resp['Body'].read().decode(),
        )
    except Exception as exc:
        LOG.warning(
            'Could not fetch manifest: %s', exc,
        )

    all_manifests: dict = {}
    try:
        resp = s3.get_object(
            Bucket=FW_BUCKET,
            Key='archive/manifests.json',
        )
        all_manifests = json.loads(
            resp['Body'].read().decode(),
        )
    except Exception as exc:
        LOG.debug(
            'No archive/manifests.json: %s', exc,
        )

    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(
        Bucket=FW_BUCKET, Prefix='archive/',
    )

    builds: list[dict] = []
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.endswith('_ultra_mcu.bin'):
                continue
            name = key.split('/')[-1]
            version = name.replace(
                '_ultra_mcu.bin', '',
            )
            entry: dict = {
                'version': version,
                'key': key,
                'size': obj.get('Size', 0),
                'date': obj['LastModified'].isoformat()
                if obj.get('LastModified') else '',
                'is_latest': (
                    version == manifest.get('version')
                ),
                'notes': '',
            }
            ver_meta = all_manifests.get(version, {})
            if ver_meta:
                entry['sha'] = ver_meta.get('sha', '')
                entry['branch'] = ver_meta.get(
                    'branch', '',
                )
                entry['md5'] = ver_meta.get('md5', '')
                entry['notes'] = ver_meta.get(
                    'notes', '',
                )
            builds.append(entry)

    builds.sort(key=lambda b: b['date'], reverse=True)

    if manifest:
        for b in builds:
            if b['is_latest']:
                b.setdefault('sha', '')
                b.setdefault('branch', '')
                b.setdefault('md5', '')
                b['sha'] = (
                    b['sha']
                    or manifest.get('sha', '')
                )
                b['branch'] = (
                    b['branch']
                    or manifest.get('branch', '')
                )
                b['md5'] = (
                    b['md5']
                    or manifest.get('md5', '')
                )
                if not b['notes']:
                    b['notes'] = manifest.get(
                        'notes', '',
                    )
                break

    return builds


# -------------------------------------------------------
# Download
# -------------------------------------------------------

def download_firmware(s3_key: str) -> str:
    '''Download a firmware binary from S3.

    Args:
        s3_key: S3 object key
            (e.g. "archive/v1.2.3_ultra_mcu.bin").

    Returns:
        Local file path of the downloaded binary.
    '''
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    filename = s3_key.split('/')[-1]
    local_path = os.path.join(DOWNLOAD_DIR, filename)

    _log(f'Downloading s3://{FW_BUCKET}/{s3_key} ...')
    _set_status('downloading', 10, 'Downloading...')

    s3 = _get_s3()
    s3.download_file(FW_BUCKET, s3_key, local_path)

    size = os.path.getsize(local_path)
    _log(f'Downloaded {size} bytes -> {local_path}')

    md5 = hashlib.md5(
        open(local_path, 'rb').read(),
    ).hexdigest()
    _log(f'MD5: {md5}')

    try:
        resp = s3.get_object(
            Bucket=FW_BUCKET,
            Key='latest/manifest.json',
        )
        manifest = json.loads(
            resp['Body'].read().decode(),
        )
        expected_md5 = manifest.get('md5', '')
        name = s3_key.split('/')[-1]
        ver = name.replace('_ultra_mcu.bin', '')
        if (
            expected_md5
            and ver == manifest.get('version')
        ):
            if md5 == expected_md5:
                _log('MD5 checksum verified OK')
            else:
                _log(
                    f'MD5 MISMATCH: expected '
                    f'{expected_md5}, got {md5}',
                )
    except Exception:
        _log('Skipping manifest MD5 check')

    _set_status('downloading', 30, 'Download complete')
    return local_path


# -------------------------------------------------------
# Custom UART bootloader protocol
# -------------------------------------------------------

def _send_enter_bootloader(skip_reset: bool = False):
    '''Send ENTER_BOOTLOADER (0x8009) to the running app.

    Opens the UART at the application baud rate (921600 8N1,
    rtscts=True to match the running firmware), sends the
    framed command, then waits for the MCU to reset into the
    custom bootloader.

    Args:
        skip_reset: If True, skip sending the command
            (assumes MCU is already in bootloader mode).
    '''
    if skip_reset:
        _log('Skipping ENTER_BOOTLOADER (--skip-reset)')
        return

    import serial

    from ultra.hw import frame_protocol as fp

    _log(
        f'[1/5] Sending ENTER_BOOTLOADER '
        f'to {UART_PORT} @ {APP_BAUD}',
    )
    _set_status('flashing', 32, 'Entering bootloader...')

    try:
        ser = serial.Serial(
            UART_PORT, APP_BAUD, timeout=0.5,
            parity=serial.PARITY_NONE,
            bytesize=serial.EIGHTBITS,
            stopbits=serial.STOPBITS_ONE,
            rtscts=True,
        )
        payload = fp.pack_seq(0)
        frame = fp.build_frame(
            command=fp.CMD_ENTER_BOOTLOADER,
            data=payload,
        )
        ser.write(frame)
        ser.flush()
        time.sleep(0.1)

        resp = ser.read(64)
        ser.close()

        if resp:
            _log(f'  App responded ({len(resp)} bytes)')
        else:
            _log(
                '  No app response '
                '(may already be in bootloader)',
            )
    except Exception as exc:
        _log(f'  ENTER_BOOTLOADER warning: {exc}')

    _log('  Waiting for STM32 reset...')
    time.sleep(2.0)


def _wait_for_sync(ser, timeout: float = 30.0) -> bool:
    '''Wait for READY (0x7F) from bootloader, send SYNC.

    The custom bootloader sends 0x7F every 500 ms until a
    host responds with 0x7F.  On successful sync the
    bootloader replies 0x79 (ACK).

    Args:
        ser: Open serial.Serial at bootloader baud rate.
        timeout: Maximum seconds to wait.

    Returns:
        True if sync succeeded, False on timeout.
    '''
    _log('[2/5] Waiting for bootloader READY...')
    _set_status('flashing', 35, 'Syncing bootloader...')

    deadline = time.time() + timeout
    while time.time() < deadline:
        data = ser.read(1)
        if data and data[0] == READY:
            ser.write(bytes([READY]))
            ser.flush()
            ack = ser.read(1)
            if ack and ack[0] == ACK:
                _log('  Bootloader synced')
                return True
            _log(
                f'  Unexpected sync reply: '
                f'{ack.hex() if ack else "timeout"}',
            )
    return False


def _send_info(
        ser,
        fw_size: int,
        fw_crc: int,
) -> bool:
    '''Send firmware size + CRC-32 and wait for erase ACK.

    Args:
        ser: Open serial.Serial at bootloader baud rate.
        fw_size: Firmware binary size in bytes.
        fw_crc: IEEE 802.3 CRC-32 of the binary.

    Returns:
        True if erase succeeded, False on NACK/timeout.
    '''
    _log(
        f'[3/5] Sending info: size={fw_size}, '
        f'crc=0x{fw_crc:08X}',
    )
    _set_status('flashing', 38, 'Erasing flash...')

    info = struct.pack('<II', fw_size, fw_crc)
    ser.write(info)
    ser.flush()

    _log('  Erasing flash sectors...')
    ack = ser.read(1)
    if not ack or ack[0] != ACK:
        tag = ack.hex() if ack else 'timeout'
        _log(f'  Erase NACK: {tag}')
        return False
    _log('  Erase OK')
    return True


def _stream_firmware(ser, fw_data: bytes) -> bool:
    '''Stream firmware in CHUNK_SZ blocks with per-block ACK.

    Args:
        ser: Open serial.Serial at bootloader baud rate.
        fw_data: Raw firmware binary bytes.

    Returns:
        True if all chunks were ACKed, False on failure.
    '''
    total = len(fw_data)
    sent = 0
    _log(f'[4/5] Streaming {total} bytes...')

    while sent < total:
        end = min(sent + CHUNK_SZ, total)
        chunk = fw_data[sent:end]
        ser.write(chunk)
        ser.flush()

        ack = ser.read(1)
        if not ack or ack[0] != ACK:
            _log(f'  Write NACK at offset 0x{sent:06X}')
            return False

        sent = end
        pct = 100 * sent // total
        scaled = 40 + int(pct * 0.55)
        _set_status(
            'flashing', scaled, f'Flashing... {pct}%',
        )

    _log(f'  Streamed {sent} bytes')
    return True


def _verify(ser) -> bool:
    '''Wait for bootloader CRC-32 verification result.

    Args:
        ser: Open serial.Serial at bootloader baud rate.

    Returns:
        True if CRC matched (ACK), False on NACK/timeout.
    '''
    _log('[5/5] Verifying CRC-32...')
    _set_status('flashing', 96, 'Verifying...')

    ack = ser.read(1)
    if ack and ack[0] == ACK:
        _log('  CRC OK')
        return True
    tag = ack.hex() if ack else 'timeout'
    _log(f'  CRC FAILED: {tag}')
    return False


def flash_firmware(
        bin_path: str,
        skip_reset: bool = False,
) -> bool:
    '''Flash a .bin file via the custom UART bootloader.

    Must be called with the UART port free (no active
    STM32Interface or StatusMonitor).

    Protocol:
      1. Send ENTER_BOOTLOADER (0x8009) at 921600 8N1
      2. Sync with bootloader at 115200 8N1
      3. Send size + CRC-32, wait for erase ACK
      4. Stream 1024-byte chunks with per-block ACK
      5. Wait for CRC verification ACK

    Args:
        bin_path: Path to the .bin firmware file.
        skip_reset: If True, skip sending
            ENTER_BOOTLOADER (MCU already in bootloader).

    Returns:
        True on success, False on failure.
    '''
    import serial

    with open(bin_path, 'rb') as f:
        fw_data = f.read()

    fw_size = len(fw_data)
    fw_crc = binascii.crc32(fw_data) & 0xFFFFFFFF

    _log(f'Firmware: {bin_path}')
    _log(f'  Size : {fw_size} bytes '
         f'({fw_size / 1024:.1f} KB)')
    _log(f'  CRC  : 0x{fw_crc:08X}')

    if fw_size > MAX_APP_SIZE:
        _log(
            f'ERROR: firmware too large '
            f'(max {MAX_APP_SIZE} bytes)',
        )
        _set_status('error', 0, 'Firmware too large')
        return False
    if fw_size == 0:
        _log('ERROR: firmware file is empty')
        _set_status('error', 0, 'Firmware file is empty')
        return False

    _send_enter_bootloader(skip_reset=skip_reset)

    _log(f'Opening {UART_PORT} @ {BL_BAUD} 8N1')
    ser = serial.Serial(
        UART_PORT, BL_BAUD, timeout=5.0,
        parity=serial.PARITY_NONE,
        bytesize=serial.EIGHTBITS,
        stopbits=serial.STOPBITS_ONE,
        rtscts=False,
    )
    ser.reset_input_buffer()

    try:
        if not _wait_for_sync(ser, timeout=30.0):
            _log('ERROR: bootloader did not respond')
            _set_status(
                'error', 0,
                'Bootloader did not respond',
            )
            return False

        if not _send_info(ser, fw_size, fw_crc):
            _set_status('error', 0, 'Flash erase failed')
            return False

        ser.timeout = 10.0
        if not _stream_firmware(ser, fw_data):
            _set_status('error', 0, 'Write failed')
            return False

        ser.timeout = 5.0
        if not _verify(ser):
            _set_status('error', 0, 'CRC verification failed')
            return False

    finally:
        ser.close()

    name = os.path.basename(bin_path)
    ver = name.replace('_ultra_mcu.bin', '')
    _log(f'Flash complete! Firmware: {ver}')
    _set_status('done', 100, f'Flashed {ver}')
    return True


# -------------------------------------------------------
# Combined download + flash (run in background thread)
# -------------------------------------------------------

def download_and_flash(s3_key: str) -> None:
    '''Download firmware from S3 and flash it.

    Intended to run in a background thread. Updates
    _flash_status throughout.

    Args:
        s3_key: S3 object key for the firmware binary.
    '''
    if not _flash_lock.acquire(blocking=False):
        _set_status(
            'error', 0, 'Flash already in progress',
        )
        return

    try:
        _flash_status['log'].clear()
        _set_status('downloading', 5, 'Starting...')
        _log(f'Starting firmware update: {s3_key}')

        bin_path = download_firmware(s3_key)
        flash_firmware(bin_path)

    except Exception as exc:
        _log(f'Unexpected error: {exc}')
        _set_status('error', 0, str(exc))
    finally:
        _flash_lock.release()
