'''ultra.reader.pipeline -- Analysis pipeline glue.

Bridges raw TLV data from the acquisition service to
the siphox.analysis_tools package (installed from sway
repo via pip). Uses the real sway peak detection API
when available, with a lightweight fallback that does
simple min-finding on raw spectral data.

Data flow (sway path):
  raw TLV file
  -> readertospectra.process_tlv_file_to_sweeps()
     (decode, unpack, bin, wavelength-calibrate)
  -> peaks.run_peak_detection(channels, SweepData,
     config_peak_detect, chip_mapping)
  -> event_bus.emit('peak_data', ...)

Fallback path (no analysis_tools):
  raw TLV file -> _parse_tlv_chunks()
  -> numpy find-minimum on raw ADC sensor data
  -> event_bus.emit('peak_data', ...)
'''
from __future__ import annotations

import logging
import os
import struct
import tempfile
from typing import Any

from ultra.events import EventBus

LOG = logging.getLogger(__name__)

TLV_HEADER_SIZE = 4
TLV_TYPE_CHUNK = 4

NOMINAL_WL_START_NM = 1530.0
NOMINAL_WL_SPAN_NM = 50.0


def _parabolic_min(
        wl: 'np.ndarray',
        vals: 'np.ndarray',
        idx: int,
) -> float:
    '''Sub-bin minimum via 3-point parabolic interpolation.

    Fits a parabola through the minimum and its two
    neighbours to find a sub-sample estimate of the
    resonance dip wavelength. Falls back to the bin
    center when the index is at the array edge.

    Args:
        wl: Wavelength axis array.
        vals: Sample values (uint16 ADC counts).
        idx: Index of the discrete minimum.

    Returns:
        Interpolated wavelength of the minimum.
    '''
    n = len(vals)
    if idx <= 0 or idx >= n - 1:
        return float(wl[idx])

    y0 = float(vals[idx - 1])
    y1 = float(vals[idx])
    y2 = float(vals[idx + 1])
    denom = 2.0 * (y0 - 2.0 * y1 + y2)
    if abs(denom) < 1e-12:
        return float(wl[idx])

    frac = (y0 - y2) / denom
    return float(wl[idx]) + frac * float(
        wl[1] - wl[0],
    )


class ReaderPipeline:
    '''Glue layer between raw TLV data and analysis tools.

    Tries siphox.analysis_tools for real peak detection.
    Falls back to simple min-finding on raw TLV spectral
    data when analysis_tools is not available.

    Attributes:
        _event_bus: Application event bus.
        _peak_config: Peak detection config dict from YAML.
        _baseline: Per-channel baseline wavelengths for
            shift calculation.
        _wl_rth_fit: Persistent wavelength-vs-thermistor
            calibration (mutated across TLV files).
        _pow_rth_fit: Persistent power calibration.
    '''

    def __init__(
            self,
            event_bus: EventBus,
            config: dict[str, Any] | None = None,
    ) -> None:
        '''Initialize the pipeline.

        Args:
            event_bus: Application event bus for emitting
                peak data events.
            config: Full application config dict. The
                ``peak_detect`` section is forwarded to
                sway's ``run_peak_detection``.
        '''
        self._event_bus = event_bus
        self._config = config or {}
        self._peak_config: dict[str, Any] = dict(
            self._config.get('peak_detect', {}),
        )
        self._baseline: dict[int, float] = {}
        self._timestamp_s: float = 0.0
        self._block_count: int = 0

        self._wl_rth_fit: list[Any] = [None, None]
        self._pow_rth_fit: list[Any] = [None, None]
        self._tlvtospec: Any = None
        self._sway_ok: bool | None = None
        self._run_dir: str = ''

    def set_run_dir(self, run_dir: str) -> None:
        '''Set the run directory for TlvToSpectra logs.

        Reinitialises the internal TlvToSpectra instance so
        that any resonance-property CSV files land in the
        correct run directory.

        Args:
            run_dir: Absolute path to the current run dir.
        '''
        self._run_dir = run_dir
        self._tlvtospec = None
        self._sway_ok = None

    # ----------------------------------------------------------
    # Lazy sway init
    # ----------------------------------------------------------

    def _ensure_sway(self) -> bool:
        '''Lazy-initialise the sway analysis_tools pipeline.

        Creates a TlvToSpectra instance on the first call.
        Returns True if analysis_tools is importable and
        the instance was created successfully.
        '''
        if self._sway_ok is not None:
            return self._sway_ok

        try:
            from siphox.analysis_tools.readertospectra import (
                TlvToSpectra,
            )
        except ImportError:
            LOG.warning(
                'siphox.analysis_tools not installed '
                '-- using fallback peak detection',
            )
            self._sway_ok = False
            return False

        try:
            run_dir = self._run_dir or tempfile.mkdtemp(
                prefix='ultra-pipeline-',
            )
            self._tlvtospec = TlvToSpectra(
                chip_pos=0,
                run_dir_path=run_dir,
                reader_params_dict={},
                peak_config=dict(self._peak_config),
                do_broadcast=False,
            )
            self._sway_ok = True
            LOG.info(
                'Sway analysis_tools pipeline initialised',
            )
        except Exception as err:
            LOG.warning(
                'Failed to init sway pipeline: %s '
                '-- using fallback',
                err,
            )
            self._sway_ok = False

        return self._sway_ok

    # ----------------------------------------------------------
    # Public API
    # ----------------------------------------------------------

    def process_tlv_file(
            self,
            path: str,
            timestamp_s: float = 0.0,
    ) -> list[dict] | None:
        '''Process a raw TLV file through the analysis
        pipeline.

        Attempts the full sway analysis_tools pipeline
        first. If that is unavailable or fails, falls back
        to simple min-finding on raw spectral data.

        Args:
            path: Path to raw .tlv file.
            timestamp_s: Protocol timestamp in seconds.

        Returns:
            List of per-channel peak result dicts, or None
            if the file yielded no usable data.
        '''
        self._timestamp_s = timestamp_s
        self._block_count += 1

        if self._ensure_sway():
            try:
                result = self._process_with_sway(path)
                if result:
                    return result
                LOG.debug(
                    'Sway pipeline returned no peaks '
                    'for %s -- trying fallback',
                    path,
                )
            except Exception as err:
                LOG.warning(
                    'Sway pipeline error on %s: %s '
                    '-- trying fallback',
                    path, err,
                )

        try:
            return self._process_fallback(path)
        except Exception as err:
            LOG.warning(
                'Fallback pipeline error on %s: %s',
                path, err,
            )
            return None

    def reset_baseline(self) -> None:
        '''Clear baselines and calibration for a new run.'''
        self._baseline.clear()
        self._block_count = 0
        self._wl_rth_fit = [None, None]
        self._pow_rth_fit = [None, None]
        self._tlvtospec = None
        self._sway_ok = None
        LOG.debug('Pipeline baseline and calibration reset')

    # ----------------------------------------------------------
    # Internal: sway pipeline
    # ----------------------------------------------------------

    def _process_with_sway(
            self, path: str,
    ) -> list[dict] | None:
        '''Run the full sway decode -> calibrate -> detect
        pipeline on a single TLV file.

        Args:
            path: Path to .tlv file on disk.

        Returns:
            List of peak result dicts or None.
        '''
        import numpy as np
        from siphox.analysis_tools.readertospectra import (
            process_tlv_file_to_sweeps,
        )
        from siphox.analysis_tools.utils.\
            fitting_functions import peaks as span_peaks

        calibrated = process_tlv_file_to_sweeps(
            tlv_filepath=path,
            tlvtospec_obj=self._tlvtospec,
            wl_rth_fit=self._wl_rth_fit,
            pow_rth_fit=self._pow_rth_fit,
        )

        if calibrated.chunk_id_mismatch:
            LOG.warning(
                'Chunk ID mismatch in %s', path,
            )

        if not calibrated.wavelength_sweep_list:
            return None

        results: list[dict] = []
        for i, wl_arr in enumerate(
            calibrated.wavelength_sweep_list,
        ):
            sensor_arr = calibrated.sensor_sweep_list[i]
            if len(wl_arr) == 0:
                continue

            if (
                calibrated.b_loss_list
                and calibrated.b_loss_list[i]
            ):
                continue

            no_ch = (
                sensor_arr.shape[0]
                if sensor_arr.ndim > 1
                else 1
            )
            no_points = len(wl_arr)

            t_data = (
                calibrated.time_data_list[i]
                if calibrated.time_data_list
                else np.array([self._timestamp_s])
            )

            sweep = span_peaks.SweepData(
                x_axis=wl_arr,
                sensor_curves=sensor_arr,
                time=t_data,
                no_ch=no_ch,
                no_points=no_points,
            )

            channels = list(range(1, no_ch + 1))
            chip_map = (
                span_peaks
                .minimal_chip_mapping_for_channels(no_ch)
            )

            peak_cfg = dict(self._peak_config)
            peak_cfg['t'] = round(self._timestamp_s, 2)

            _, peaks_nm, _ = span_peaks.run_peak_detection(
                channels=channels,
                latest_sweep=sweep,
                config_peak_detect=peak_cfg,
                chip_mapping=chip_map,
            )

            self._emit_peaks(
                span_peaks.track_peaks(peaks_nm),
            )
            results.extend(self._last_emitted)

        return results or None

    # ----------------------------------------------------------
    # Internal: lightweight fallback pipeline
    # ----------------------------------------------------------

    def _process_fallback(
            self, path: str,
    ) -> list[dict] | None:
        '''Lightweight peak detection without analysis_tools.

        Reads the TLV file, extracts raw ADC spectra from
        chunk payloads, finds the minimum (resonance dip)
        in each channel's 16-bit sensor curve, and maps
        it to a nominal wavelength.

        Args:
            path: Path to .tlv file on disk.

        Returns:
            List of peak result dicts or None.
        '''
        if not os.path.isfile(path):
            return None

        with open(path, 'rb') as fh:
            data = fh.read()

        if len(data) < TLV_HEADER_SIZE:
            return None

        chunks = self._parse_tlv_chunks(data)
        if not chunks:
            LOG.debug(
                'Fallback: no TLV chunks in %s '
                '(%d bytes)',
                path, len(data),
            )
            return None

        try:
            import numpy as np
        except ImportError:
            LOG.warning(
                'numpy not available for fallback '
                'pipeline',
            )
            return None

        seen_channels: dict[int, list[float]] = {}
        for chunk in chunks:
            payload = chunk['payload']
            adc_id = chunk['adc_id']
            if len(payload) < 20:
                continue

            samples = np.frombuffer(
                payload, dtype=np.uint16,
            )
            if len(samples) < 10:
                continue

            n_pts = len(samples)
            wl_axis = np.linspace(
                NOMINAL_WL_START_NM,
                NOMINAL_WL_START_NM + NOMINAL_WL_SPAN_NM,
                n_pts,
            )

            min_idx = int(np.argmin(samples))
            peak_wl = _parabolic_min(
                wl_axis, samples, min_idx,
            )

            seen_channels.setdefault(
                adc_id, [],
            ).append(peak_wl)

        results: list[dict] = []
        for ch, wl_list in seen_channels.items():
            avg_wl = sum(wl_list) / len(wl_list)

            if ch not in self._baseline:
                self._baseline[ch] = avg_wl

            shift_pm = (
                (avg_wl - self._baseline[ch]) * 1000
            )

            result = {
                'channel': ch,
                'wavelength_nm': round(avg_wl, 4),
                'shift_pm': round(shift_pm, 2),
                'timestamp_s': round(
                    self._timestamp_s, 2,
                ),
            }
            results.append(result)
            self._event_bus.emit_sync(
                'peak_data', result,
            )

        if results:
            LOG.debug(
                'Fallback: %d peaks from %d chunks '
                'in %s',
                len(results), len(chunks), path,
            )
        return results or None

    @staticmethod
    def _parse_tlv_chunks(
            data: bytes | bytearray,
    ) -> list[dict]:
        '''Extract chunk payloads from raw TLV binary.

        Args:
            data: Raw TLV byte data.

        Returns:
            List of dicts with chunk_id, adc_id, payload.
        '''
        chunks: list[dict] = []
        offset = 0
        while offset + TLV_HEADER_SIZE <= len(data):
            if offset + 4 > len(data):
                break
            tlv_type, tlv_len = struct.unpack_from(
                '<HH', data, offset,
            )
            payload_start = offset + TLV_HEADER_SIZE
            payload_end = payload_start + tlv_len

            if payload_end > len(data):
                break

            if (
                tlv_type == TLV_TYPE_CHUNK
                and tlv_len >= 6
            ):
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

            offset = payload_end

        return chunks

    def _emit_peaks(
            self,
            peaks_now: list[Any],
    ) -> None:
        '''Convert tracked peaks to dicts and emit events.

        Args:
            peaks_now: Per-channel list of PeakNm | None
                from ``span_peaks.track_peaks()``.
        '''
        self._last_emitted: list[dict] = []
        for ch_idx, peak in enumerate(peaks_now):
            if peak is None:
                continue
            wl = peak[0]

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
            self._last_emitted.append(result)

            self._event_bus.emit_sync(
                'peak_data', result,
            )
