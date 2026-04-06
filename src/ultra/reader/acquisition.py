'''ultra.reader.acquisition -- Single-reader TLV capture.

Simplified from sway's multi-reader readeracquire.py.
Captures raw TLV bytes from the PProc reader, writes
blocks to disk, and emits events for the pipeline.
'''
from __future__ import annotations

import asyncio
import logging
import os
import os.path as op
import struct
import time
from datetime import datetime, timezone
from typing import Any

from ultra.events import EventBus

LOG = logging.getLogger(__name__)

TLV_HEADER_SIZE = 4
TLV_TYPE_CHUNK = 4
TLV_TYPE_DATA_LOSS = 5

CAPTURE_SLEEP_S = 0.003
BONUS_TIME_S = 1.2
INTER_BLOCK_SLEEP_S = 0.001

BYTES_PER_SEC_TABLE = {
    1: 272_756, 2: 545_116, 3: 817_436,
    4: 1_089_796, 5: 1_362_136, 6: 1_634_486,
    7: 1_906_826, 8: 2_179_176, 9: 2_451_516,
    10: 2_540_848, 11: 2_996_206, 12: 3_268_566,
}


class AcquisitionService:
    '''Single-reader TLV data capture service.

    Manages the capture loop: starts the reader stream,
    accumulates raw TLV bytes, writes complete blocks to
    disk, and emits events for downstream processing.

    Attributes:
        _reader: ReaderInterface or ReaderMock.
        _event_bus: Event bus for emitting data events.
        _output_dir: Directory for raw TLV files.
    '''

    def __init__(
            self,
            reader: Any,
            event_bus: EventBus,
            output_dir: str = '/tmp/ultra-tlv',
    ) -> None:
        '''Initialize the acquisition service.

        Args:
            reader: ReaderInterface or ReaderMock.
            event_bus: Application event bus.
            output_dir: Directory for raw TLV block files.
        '''
        self._reader = reader
        self._event_bus = event_bus
        self._output_dir = output_dir
        self._block_counter = -1
        os.makedirs(output_dir, exist_ok=True)

    def stop(self) -> None:
        '''Stop the reader stream explicitly.

        Call this when the acquisition loop is done (protocol
        end or cancellation). Matches sway's pattern of only
        stopping the stream on exit, not between blocks.
        '''
        try:
            self._reader.stop_stream()
            LOG.info('Reader stream stopped')
        except Exception as exc:
            LOG.warning(
                'Error stopping reader stream: %s', exc,
            )

    def set_output_dir(self, path: str) -> None:
        '''Change the TLV output directory and reset counter.

        Called before each protocol run to direct TLV files
        into the run's ``tlv/`` subdirectory.

        Args:
            path: New output directory path.
        '''
        self._output_dir = path
        self._block_counter = -1
        os.makedirs(path, exist_ok=True)

    async def capture_block(
            self,
            acq_seconds: int = 3,
    ) -> str | None:
        '''Capture one acquisition block of TLV data.

        Starts the reader stream, accumulates bytes until
        the expected block size is reached, writes to disk,
        and returns the file path.

        Args:
            acq_seconds: Acquisition time in seconds.

        Returns:
            Path to the saved .tlv file, or None on failure.
        '''
        est_bytes = BYTES_PER_SEC_TABLE.get(
            acq_seconds,
            acq_seconds * 180_000,
        )
        buf = bytearray(est_bytes * 2)
        total_bytes = 0

        if not self._reader.start_stream(acq_seconds):
            LOG.error('Failed to start reader stream')
            return None

        start_time = time.time()
        deadline = (
            start_time + acq_seconds + BONUS_TIME_S
        )

        while time.time() < deadline:
            await asyncio.sleep(CAPTURE_SLEEP_S)
            raw = self._reader.read_bytes()
            if raw:
                end = total_bytes + len(raw)
                if end > len(buf):
                    buf.extend(
                        bytearray(end - len(buf)),
                    )
                buf[total_bytes:end] = raw
                total_bytes = end

            if total_bytes >= est_bytes:
                break

        if total_bytes == 0:
            LOG.warning('No TLV data captured')
            return None

        end_time = time.time()
        duration = end_time - start_time
        self._block_counter += 1
        bc = self._block_counter

        tlv_path = os.path.join(
            self._output_dir,
            f'data_{bc}.tlv',
        )
        tmp_path = tlv_path + '0'
        with open(tmp_path, 'wb') as fh:
            fh.write(buf[:total_bytes])
        os.replace(tmp_path, tlv_path)

        self._write_time_log(bc, start_time, end_time)
        self._append_root_time_log(start_time, end_time)

        LOG.info(
            'TLV block %d: %d bytes in %.1fs -> %s',
            bc, total_bytes, duration, tlv_path,
        )

        self._event_bus.emit_sync(
            'tlv_block_captured', {
                'block': bc,
                'path': tlv_path,
                'bytes': total_bytes,
                'duration_s': round(duration, 2),
            },
        )
        await asyncio.sleep(INTER_BLOCK_SLEEP_S)
        return tlv_path

    def _write_time_log(
            self,
            block_idx: int,
            t_start: float,
            t_end: float,
    ) -> None:
        '''Write per-block time_N.log matching sway format.

        Format::
            Time,Counter,Start,End,Duration
            2026/04/06-20:15:55,0,1743972955.12,...,3.33
        '''
        dt = datetime.fromtimestamp(
            t_start,
            tz=timezone.utc,
        ).astimezone()
        ts = dt.strftime('%Y/%m/%d-%H:%M:%S')
        dur = t_end - t_start

        path = os.path.join(
            self._output_dir,
            f'time_{block_idx}.log',
        )
        tmp = path + '0'
        with open(tmp, 'w') as fh:
            fh.write('Time,Counter,Start,End,Duration\n')
            fh.write(
                f'{ts},{block_idx},'
                f'{t_start:.2f},{t_end:.2f},'
                f'{dur:.2f}\n',
            )
        os.replace(tmp, path)

    def _append_root_time_log(
            self,
            t_start: float,
            t_end: float,
    ) -> None:
        '''Append to the run-root time.log (legacy).

        The run directory is the parent of ``self._output_dir``
        (which points to ``{run_dir}/tlv/``).
        '''
        run_dir = op.dirname(self._output_dir)
        fp = op.join(run_dir, 'time.log')
        with open(fp, 'a') as fh:
            fh.write(f'{t_start:.2f}, {t_end:.2f}\n')

    def parse_tlv_chunks(
            self,
            data: bytes | bytearray,
    ) -> list[dict]:
        '''Parse TLV chunks from raw byte data.

        Extracts chunk records from the binary TLV stream.
        Partial records at the end are silently ignored.

        Args:
            data: Raw TLV byte data.

        Returns:
            List of parsed chunk dicts with keys:
            chunk_id, adc_id, payload.
        '''
        chunks = []
        offset = 0
        while offset + TLV_HEADER_SIZE <= len(data):
            tlv_type, tlv_len = struct.unpack_from(
                '<HH', data, offset,
            )
            payload_start = offset + TLV_HEADER_SIZE
            payload_end = payload_start + tlv_len

            if payload_end > len(data):
                break

            if tlv_type == TLV_TYPE_CHUNK and tlv_len >= 6:
                chunk_id, adc_id, _ = struct.unpack_from(
                    '<IBB', data, payload_start,
                )
                chunks.append({
                    'chunk_id': chunk_id,
                    'adc_id': adc_id,
                    'payload': bytes(
                        data[
                            payload_start + 6:payload_end
                        ],
                    ),
                })
            elif tlv_type == TLV_TYPE_DATA_LOSS:
                LOG.warning(
                    'Data loss TLV at offset '
                    f'{offset}',
                )

            offset = payload_end

        return chunks
