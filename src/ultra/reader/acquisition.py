'''ultra.reader.acquisition -- Single-reader TLV capture.

Simplified from sway's multi-reader readeracquire.py.
Captures raw TLV bytes from the PProc reader, writes
blocks to disk, and emits events for the pipeline.
'''
from __future__ import annotations

import asyncio
import logging
import os
import struct
import time
from typing import Any

from ultra.events import EventBus

LOG = logging.getLogger(__name__)

TLV_HEADER_SIZE = 4
TLV_TYPE_CHUNK = 4
TLV_TYPE_DATA_LOSS = 5

CAPTURE_SLEEP_S = 0.003
BONUS_TIME_S = 3.0

BYTES_PER_SEC_TABLE = {
    1: 180_000, 2: 360_000, 3: 540_000,
    4: 720_000, 5: 900_000, 6: 1_080_000,
    7: 1_260_000, 8: 1_440_000, 9: 1_620_000,
    10: 1_800_000, 11: 1_980_000, 12: 2_160_000,
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
        self._block_counter = 0
        os.makedirs(output_dir, exist_ok=True)

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

        try:
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
        finally:
            self._reader.stop_stream()

        if total_bytes == 0:
            LOG.warning('No TLV data captured')
            return None

        self._block_counter += 1
        tlv_path = os.path.join(
            self._output_dir,
            f'data_{self._block_counter}.tlv',
        )
        tmp_path = tlv_path + '0'
        with open(tmp_path, 'wb') as fh:
            fh.write(buf[:total_bytes])
        os.replace(tmp_path, tlv_path)

        duration = time.time() - start_time
        LOG.info(
            f'TLV block {self._block_counter}: '
            f'{total_bytes} bytes in {duration:.1f}s '
            f'-> {tlv_path}',
        )

        self._event_bus.emit_sync(
            'tlv_block_captured', {
                'block': self._block_counter,
                'path': tlv_path,
                'bytes': total_bytes,
                'duration_s': round(duration, 2),
            },
        )
        return tlv_path

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
