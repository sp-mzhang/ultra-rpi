"""OTA firmware update via MCUboot (Slot B write + reset).

Replaces the old UART-bootloader-mode flow. The app stays running
at 921600 baud while the Pi writes the signed image to Slot B.
On the next reset, MCUboot verifies the signature and copies B→A.

Usage:
    from ultra.services.fw_update_mcuboot import flash_signed_image
    flash_signed_image(stm32_interface, '/path/to/signed_app.bin')
"""
from __future__ import annotations

import logging
import struct
import time
from pathlib import Path

LOG = logging.getLogger(__name__)

SLOT_B_SIZE = 3 * 128 * 1024   # 384 KB
CHUNK_SIZE  = 224               # must be ≤ MAX_DATA_LEN(255) - 8 (seq+offset)
                                # and a multiple of 32 (flash word)


def flash_signed_image(
        stm32,
        image_path: str | Path,
        timeout_erase: float = 30.0,
        timeout_write: float = 5.0,
) -> dict:
    """Write a signed MCUboot image to Slot B and reset.

    Args:
        stm32: Connected STM32Interface instance.
        image_path: Path to the signed .bin from imgtool.
        timeout_erase: Timeout for the erase command (seconds).
        timeout_write: Timeout per write-block command.

    Returns:
        dict with 'ok' (bool), 'message' (str), 'bytes_written' (int).
    """
    data = Path(image_path).read_bytes()
    if len(data) > SLOT_B_SIZE:
        return {'ok': False, 'message': f'Image too large ({len(data)} > {SLOT_B_SIZE})', 'bytes_written': 0}

    LOG.info(f'OTA: flashing {len(data)} bytes to Slot B')

    # 1. Erase Slot B
    LOG.info('OTA: erasing Slot B...')
    r = stm32.send_command({'cmd': 'fw_update_start'}, timeout_s=timeout_erase)
    if r is None or r.get('error_code', 0xFF) != 0:
        return {'ok': False, 'message': f'Erase failed: {r}', 'bytes_written': 0}
    LOG.info('OTA: Slot B erased')

    # 2. Write chunks
    offset = 0
    total = len(data)
    while offset < total:
        chunk = data[offset:offset + CHUNK_SIZE]
        # Pad to 32-byte alignment
        pad = (32 - len(chunk) % 32) % 32
        if pad:
            chunk += b'\xff' * pad

        payload = struct.pack('<II', 0, offset) + chunk
        import ultra.hw.frame_protocol as fp
        cmd_id = fp.CMD_FW_WRITE_BLOCK
        frame = fp.build_frame(command=cmd_id, data=payload)

        # Use raw write since _pack_command doesn't handle fw_write_block
        stm32._tx_queue.put(frame, timeout=2.0)

        # Wait for ACK via pending future
        rsp_id = fp.cmd_to_rsp(cmd_id)
        from ultra.hw.stm32_interface import _PendingResult
        fut = _PendingResult()
        with stm32._pending_lock:
            stm32._pending_ack[(rsp_id, 0)] = fut
        result = fut.wait(timeout_write)
        with stm32._pending_lock:
            stm32._pending_ack.pop((rsp_id, 0), None)

        if result is None:
            return {'ok': False, 'message': f'Write timeout at offset {offset}', 'bytes_written': offset}

        rsp_cmd, rsp_data = result
        if len(rsp_data) >= 5 and rsp_data[4] != 0:
            return {'ok': False, 'message': f'Write error at offset {offset}: err={rsp_data[4]}', 'bytes_written': offset}

        offset += CHUNK_SIZE
        if offset % (32 * 1024) < CHUNK_SIZE:
            pct = min(100, offset * 100 // total)
            LOG.info(f'OTA: {pct}%% ({offset}/{total})')

    LOG.info(f'OTA: write complete ({total} bytes)')

    # 3. Reset — MCUboot will verify and copy B→A
    LOG.info('OTA: resetting STM32...')
    stm32.send_command({'cmd': 'reset'}, timeout_s=2.0)

    return {'ok': True, 'message': f'Wrote {total} bytes to Slot B, reset sent', 'bytes_written': total}
