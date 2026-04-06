'''ultra.hw.reader_mock -- Mock reader for testing.

Simulates the PProc reader interface without hardware.
Returns synthetic TLV data containing realistic-looking
spectral curves with a Lorentzian resonance dip that
drifts slowly over time (simulating a binding event).
'''
from __future__ import annotations

import logging
import math
import struct
import time

LOG = logging.getLogger(__name__)

TLV_TYPE_CHUNK = 4

N_SPECTRAL_POINTS = 512
WL_START_NM = 1530.0
WL_SPAN_NM = 50.0
BASELINE_ADC = 40000
DIP_DEPTH_ADC = 25000
DIP_CENTER_NM = 1555.0
DIP_FWHM_NM = 2.0
DRIFT_PM_PER_SEC = 0.8


class ReaderMock:
    '''Mock PProc reader interface.

    Generates synthetic TLV chunk data when streaming
    is active. Each chunk contains a 512-point 16-bit
    spectrum with a Lorentzian dip that drifts in time,
    mimicking a real binding assay.

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
        self._t0 = time.monotonic()

    def connect(self) -> bool:
        '''Simulate connection.'''
        LOG.info('ReaderMock connected (port=%s)', self._port)
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
        '''Return a synthetic TLV chunk with spectral data.

        Generates a 512-point uint16 spectrum with a
        Lorentzian dip whose center drifts in time to
        simulate a binding event.
        '''
        if not self._streaming:
            return b''
        time.sleep(0.01)
        self._chunk_id += 1

        elapsed = time.monotonic() - self._t0
        drift_nm = (elapsed * DRIFT_PM_PER_SEC) / 1000.0
        center = DIP_CENTER_NM + drift_nm

        spectrum = self._make_spectrum(center)

        chunk_header = struct.pack(
            '<IBB',
            self._chunk_id,
            1,
            0,
        )
        raw_payload = chunk_header + spectrum

        tlv_header = struct.pack(
            '<HH',
            TLV_TYPE_CHUNK,
            len(raw_payload),
        )
        return tlv_header + raw_payload

    def read_config(self, param_id: int) -> str:
        return f'param_{param_id}=0'

    def write_config(
            self, param_id: int,
            value: str | int | float,
    ) -> bool:
        return True

    # ----------------------------------------------------------
    # Internal helpers
    # ----------------------------------------------------------

    @staticmethod
    def _make_spectrum(
            center_nm: float,
    ) -> bytes:
        '''Build a uint16 spectrum with a Lorentzian dip.

        Args:
            center_nm: Resonance dip center wavelength.

        Returns:
            Raw bytes of N_SPECTRAL_POINTS uint16 samples.
        '''
        gamma_sq = (DIP_FWHM_NM / 2.0) ** 2
        buf = bytearray(N_SPECTRAL_POINTS * 2)
        for i in range(N_SPECTRAL_POINTS):
            wl = (
                WL_START_NM
                + (i / N_SPECTRAL_POINTS) * WL_SPAN_NM
            )
            delta = wl - center_nm
            lorentz = gamma_sq / (delta * delta + gamma_sq)
            val = int(
                BASELINE_ADC - DIP_DEPTH_ADC * lorentz,
            )
            val = max(0, min(65535, val))
            struct.pack_into('<H', buf, i * 2, val)
        return bytes(buf)
