'''ultra.hw.reader_mock -- Mock reader for testing.

Simulates the PProc reader interface without hardware.
Returns synthetic TLV data for GUI and pipeline testing.
'''
from __future__ import annotations

import logging
import struct
import time

LOG = logging.getLogger(__name__)

TLV_TYPE_CHUNK = 4


class ReaderMock:
    '''Mock PProc reader interface.

    Generates synthetic TLV chunk data when streaming
    is active.

    Attributes:
        _streaming: Whether the mock is "streaming".
        _board_id: Simulated board ID.
    '''

    def __init__(
            self,
            port: str = 'mock',
            baud: int = 1_000_000,
    ) -> None:
        self._port = port
        self._baud = baud
        self._streaming = False
        self._board_id = 'siphox-pproc-mock-001'
        self._chunk_id = 0

    def connect(self) -> bool:
        '''Simulate connection.'''
        LOG.info('ReaderMock connected')
        return True

    def disconnect(self) -> None:
        '''Simulate disconnection.'''
        self._streaming = False
        LOG.info('ReaderMock disconnected')

    @property
    def board_id(self) -> str:
        return self._board_id

    def get_id(self) -> str:
        return self._board_id

    def start_stream(
            self, acq_seconds: int = 3,
    ) -> bool:
        '''Start mock stream.'''
        self._streaming = True
        self._chunk_id = 0
        return True

    def stop_stream(self) -> bool:
        '''Stop mock stream.'''
        self._streaming = False
        return True

    def resume_stream(
            self, chunk_id: int,
    ) -> bool:
        self._chunk_id = chunk_id
        self._streaming = True
        return True

    def read_bytes(
            self, max_bytes: int = 65536,
    ) -> bytes:
        '''Return a synthetic TLV chunk.'''
        if not self._streaming:
            return b''
        time.sleep(0.01)
        self._chunk_id += 1
        payload = struct.pack(
            '<IBB',
            self._chunk_id, 1, 0,
        )
        payload += bytes(256)
        header = struct.pack(
            '<HH',
            TLV_TYPE_CHUNK,
            len(payload),
        )
        return header + payload

    def read_config(self, param_id: int) -> str:
        return f'param_{param_id}=0'

    def write_config(
            self, param_id: int,
            value: str | int | float,
    ) -> bool:
        return True
