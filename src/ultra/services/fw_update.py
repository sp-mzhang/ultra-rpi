'''ultra.services.fw_update -- STM32 firmware OTA update.

Lists firmware builds from S3, downloads a selected binary,
and flashes it to the STM32H735 over UART3 using the system
bootloader (BOOT0/nRST via GPIO + stm32flash).

S3 bucket layout (set by ultra-firmware/rpi/upload.sh):
  siphox-ultra-firmware/
    latest/ultra_mcu.bin
    latest/manifest.json      {version, sha, branch, ...}
    archive/<ver>_ultra_mcu.bin
'''
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import subprocess
import threading
import time
from typing import Any

LOG = logging.getLogger(__name__)

FW_BUCKET = 'siphox-ultra-firmware'
FW_REGION = 'us-east-1'
DOWNLOAD_DIR = '/tmp/ultra_fw'

UART_PORT = '/dev/ttyAMA3'
FLASH_BAUD = 115200
GPIO_CHIPS = ['4', '0']
GPIO_BOOT0 = 22
GPIO_NRST = 26

_s3_client: Any = None
_gpio_method: str | None = None
_gpio_chip: str | None = None
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

    Enumerates the archive/ prefix and fetches
    latest/manifest.json for metadata.

    Returns:
        List of dicts with version, key, size, date,
        is_latest, and optional manifest fields.
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
            builds.append({
                'version': version,
                'key': key,
                'size': obj.get('Size', 0),
                'date': obj['LastModified'].isoformat()
                if obj.get('LastModified') else '',
                'is_latest': (
                    version == manifest.get('version')
                ),
            })

    builds.sort(key=lambda b: b['date'], reverse=True)

    if manifest:
        for b in builds:
            if b['is_latest']:
                b['sha'] = manifest.get('sha', '')
                b['branch'] = manifest.get('branch', '')
                b['md5'] = manifest.get('md5', '')
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
# GPIO + stm32flash
# -------------------------------------------------------

def _has_pinctrl() -> bool:
    '''Check whether the pinctrl command is available.'''
    try:
        subprocess.run(
            ['pinctrl', '-h'],
            capture_output=True, timeout=3,
        )
        return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _detect_gpio_method() -> tuple[str, str | None]:
    '''Detect the best GPIO control method.

    Prefers ``pinctrl`` (RPi-native, set-and-exit).
    Falls back to ``gpioset`` with the correct chip number.

    Returns:
        Tuple of (method, chip) where method is 'pinctrl',
        'gpioset_v2', or 'gpioset_v1', and chip is None
        for pinctrl.
    '''
    global _gpio_method, _gpio_chip
    if _gpio_method:
        return _gpio_method, _gpio_chip

    if _has_pinctrl():
        _gpio_method = 'pinctrl'
        _gpio_chip = None
        LOG.info('GPIO method: pinctrl')
        return _gpio_method, _gpio_chip

    for chip in GPIO_CHIPS:
        for method, cmd in [
            (
                'gpioset_v1',
                ['gpioset', chip, f'{GPIO_BOOT0}=0'],
            ),
        ]:
            try:
                subprocess.run(
                    cmd, check=True, timeout=2,
                    capture_output=True,
                )
                _gpio_method, _gpio_chip = method, chip
                LOG.info(
                    'GPIO method: %s chip=%s',
                    method, chip,
                )
                return method, chip
            except (
                subprocess.CalledProcessError,
                subprocess.TimeoutExpired,
                FileNotFoundError,
            ):
                continue

    raise RuntimeError(
        'No working GPIO method found. '
        'Install pinctrl (RPi OS) or libgpiod v1.'
    )


def _gpio_set(pin: int, value: int) -> None:
    '''Set a GPIO pin to a value (set-and-exit).

    Uses ``pinctrl`` on RPi OS (recommended), or
    ``gpioset`` v1 as a fallback.

    Args:
        pin: BCM GPIO pin number.
        value: 0 or 1.
    '''
    method, chip = _detect_gpio_method()
    if method == 'pinctrl':
        drive = 'dh' if value else 'dl'
        subprocess.run(
            ['pinctrl', 'set', str(pin), 'op', drive],
            check=True, timeout=5,
        )
    elif method == 'gpioset_v1':
        subprocess.run(
            ['gpioset', chip, f'{pin}={value}'],
            check=True, timeout=5,
        )
    else:
        raise RuntimeError(f'Unknown GPIO method: {method}')


def _enter_bootloader() -> None:
    '''Enter STM32 system bootloader via BOOT0 + nRST.

    Sequence: set BOOT0 HIGH, hold nRST LOW for 100 ms,
    release nRST, wait for bootloader to initialize.
    The STM32 samples BOOT0 on the rising edge of nRST.
    '''
    _log('Entering bootloader (BOOT0=HIGH, nRST pulse)')
    _gpio_set(GPIO_BOOT0, 1)
    time.sleep(0.05)
    _gpio_set(GPIO_NRST, 0)
    time.sleep(0.15)
    _gpio_set(GPIO_NRST, 1)
    time.sleep(1.0)
    _log('Bootloader ready')


def _exit_bootloader() -> None:
    '''Return to normal boot: BOOT0=LOW, then reset.'''
    _log('Exiting bootloader (BOOT0=LOW, nRST pulse)')
    _gpio_set(GPIO_BOOT0, 0)
    time.sleep(0.05)
    _gpio_set(GPIO_NRST, 0)
    time.sleep(0.15)
    _gpio_set(GPIO_NRST, 1)
    _log('STM32 reset to application mode')


FLASH_MAX_RETRIES = 3


def _configure_uart() -> None:
    '''Reset UART port settings for bootloader communication.'''
    try:
        subprocess.run(
            [
                'stty', '-F', UART_PORT,
                str(FLASH_BAUD), 'raw',
                '-crtscts', '-clocal', 'cs8',
                '-parenb', '-cstopb',
            ],
            check=True, timeout=5,
        )
        _log(f'Configured {UART_PORT} for bootloader')
    except Exception as exc:
        _log(f'stty warning: {exc}')


def _probe_bootloader() -> bool:
    '''Test if the STM32 bootloader is responding.

    Runs stm32flash without -w to just query the chip.

    Returns:
        True if the bootloader responded.
    '''
    _log('Probing bootloader...')
    try:
        result = subprocess.run(
            [
                'stm32flash',
                '-b', str(FLASH_BAUD),
                UART_PORT,
            ],
            capture_output=True, text=True, timeout=10,
        )
        output = result.stdout + result.stderr
        for line in output.splitlines():
            _log(f'  {line}')
        return result.returncode == 0
    except Exception as exc:
        _log(f'  probe error: {exc}')
        return False


def flash_firmware(bin_path: str) -> bool:
    '''Flash a .bin file to the STM32 via stm32flash.

    Must be called with the UART port free (no active
    STM32Interface or StatusMonitor). Retries bootloader
    entry up to FLASH_MAX_RETRIES times.

    Args:
        bin_path: Path to the .bin firmware file.

    Returns:
        True on success, False on failure.
    '''
    for attempt in range(1, FLASH_MAX_RETRIES + 1):
        _log(
            f'--- Attempt {attempt}/{FLASH_MAX_RETRIES} ---',
        )
        _set_status(
            'flashing', 35,
            f'Entering bootloader (attempt {attempt})...',
        )
        try:
            _exit_bootloader()
            time.sleep(0.2)
        except Exception:
            pass

        try:
            _enter_bootloader()
        except Exception as exc:
            _log(f'GPIO error: {exc}')
            _set_status(
                'error', 0, f'GPIO error: {exc}',
            )
            return False

        _configure_uart()

        if not _probe_bootloader():
            _log('Bootloader not responding, retrying...')
            continue

        _set_status('flashing', 40, 'Flashing...')
        _log(
            f'Running stm32flash on {UART_PORT} '
            f'@ {FLASH_BAUD} baud',
        )
        _log(f'Binary: {bin_path}')

        cmd = [
            'stm32flash',
            '-w', bin_path,
            '-v',
            '-g', '0x08000000',
            '-b', str(FLASH_BAUD),
            UART_PORT,
        ]
        _log(f'$ {" ".join(cmd)}')

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            progress_re = re.compile(r'(\d+)%')
            for line in proc.stdout:
                line = line.rstrip('\n')
                _log(line)
                m = progress_re.search(line)
                if m:
                    pct = int(m.group(1))
                    scaled = 40 + int(pct * 0.55)
                    _set_status(
                        'flashing', scaled,
                        f'Flashing... {pct}%',
                    )

            proc.wait(timeout=120)
            if proc.returncode == 0:
                break

            _log(
                f'stm32flash exited with code '
                f'{proc.returncode}',
            )
            if attempt < FLASH_MAX_RETRIES:
                _log('Retrying...')
                continue

            _set_status(
                'error', 0,
                f'stm32flash failed (rc='
                f'{proc.returncode})',
            )
            _exit_bootloader()
            return False

        except Exception as exc:
            _log(f'stm32flash error: {exc}')
            if attempt < FLASH_MAX_RETRIES:
                _log('Retrying...')
                continue
            _set_status(
                'error', 0, f'Flash error: {exc}',
            )
            try:
                _exit_bootloader()
            except Exception:
                pass
            return False

    _set_status('flashing', 97, 'Resetting MCU...')
    try:
        _exit_bootloader()
    except Exception as exc:
        _log(f'GPIO reset warning: {exc}')

    _log('Flash complete!')
    _set_status('done', 100, 'Flash complete')
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
