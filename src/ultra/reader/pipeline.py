'''ultra.reader.pipeline -- Analysis pipeline glue.

Bridges raw TLV data from the acquisition service to
the siphox.analysis_tools package (installed from sway
repo via pip). All computation happens in the external
package; this module handles orchestration and event
emission.

Data flow:
  raw TLV bytes -> tlv_proc.decode() -> numpy arrays
  -> peaks.run_peak_detection() -> peak wavelengths
  -> event_bus.emit('peak_data', ...)
'''
from __future__ import annotations

import logging
from typing import Any

from ultra.events import EventBus

LOG = logging.getLogger(__name__)


class ReaderPipeline:
    '''Glue layer between raw TLV data and analysis tools.

    Calls siphox.analysis_tools functions to decode TLV
    binary data into spectra, run peak detection, and
    emit results to the event bus.

    Attributes:
        _event_bus: Application event bus.
        _baseline: Per-channel baseline wavelengths for
            shift calculation.
    '''

    def __init__(self, event_bus: EventBus) -> None:
        '''Initialize the pipeline.

        Args:
            event_bus: Application event bus for emitting
                peak data events.
        '''
        self._event_bus = event_bus
        self._baseline: dict[int, float] = {}
        self._timestamp_s: float = 0.0

    def process_tlv_file(
            self,
            path: str,
            timestamp_s: float = 0.0,
    ) -> list[dict] | None:
        '''Process a raw TLV file through the analysis pipeline.

        Decodes TLV binary, converts to spectra, runs peak
        detection, and emits peak_data events.

        Args:
            path: Path to raw .tlv file.
            timestamp_s: Protocol timestamp in seconds.

        Returns:
            List of per-channel peak result dicts, or None
            if analysis_tools is not available.
        '''
        self._timestamp_s = timestamp_s

        try:
            from siphox.analysis_tools.utils import (
                tlv_proc,
            )
        except ImportError:
            LOG.warning(
                'siphox.analysis_tools not installed '
                '-- skipping TLV processing',
            )
            return None

        try:
            decoded = tlv_proc.decode_tlv_file(path)
            if decoded is None:
                LOG.warning(
                    f'TLV decode returned None: {path}',
                )
                return None

            return self._run_peak_detection(decoded)

        except Exception as err:
            LOG.error(
                f'Pipeline error processing {path}: '
                f'{err}',
            )
            return None

    def _run_peak_detection(
            self, decoded: Any,
    ) -> list[dict]:
        '''Run peak detection on decoded TLV data.

        Args:
            decoded: Decoded TLV data from tlv_proc.

        Returns:
            List of per-channel peak dicts.
        '''
        results = []

        try:
            from siphox.analysis_tools.utils.\
                fitting_functions import peaks
        except ImportError:
            LOG.warning(
                'siphox.analysis_tools.peaks '
                'not available',
            )
            return results

        try:
            peak_results = peaks.run_peak_detection(
                decoded,
            )
            if peak_results is None:
                return results

            for ch_idx, peak in enumerate(peak_results):
                if peak is None:
                    continue
                wl = peak.get('wavelength_nm', 0.0)

                if ch_idx not in self._baseline:
                    self._baseline[ch_idx] = wl

                shift_pm = (
                    (wl - self._baseline[ch_idx]) * 1000
                )

                result = {
                    'channel': ch_idx,
                    'wavelength_nm': round(wl, 4),
                    'shift_pm': round(shift_pm, 2),
                    'timestamp_s': round(
                        self._timestamp_s, 2,
                    ),
                }
                results.append(result)

                self._event_bus.emit_sync(
                    'peak_data', result,
                )

        except Exception as err:
            LOG.error(
                f'Peak detection error: {err}',
            )

        return results

    def reset_baseline(self) -> None:
        '''Clear baseline wavelengths for a new run.'''
        self._baseline.clear()
        LOG.debug('Pipeline baseline reset')
