'''ultra.reader.pipeline -- Analysis pipeline glue.

Bridges raw TLV data from the acquisition service to
the siphox.analysis_tools package (installed from sway
repo via pip). Uses the real sway peak detection API:

Data flow:
  raw TLV file
  -> readertospectra.process_tlv_file_to_sweeps()
     (decode, unpack, bin, wavelength-calibrate)
  -> peaks.run_peak_detection(channels, SweepData,
     config_peak_detect, chip_mapping)
  -> event_bus.emit('peak_data', ...)
'''
from __future__ import annotations

import logging
import tempfile
from typing import Any

from ultra.events import EventBus

LOG = logging.getLogger(__name__)


class ReaderPipeline:
    '''Glue layer between raw TLV data and analysis tools.

    Calls siphox.analysis_tools to decode TLV binary data,
    wavelength-calibrate sweeps, run peak detection with
    the peak_detect config, and emit results to the event
    bus.

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
                '-- peak detection disabled',
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
        except Exception as err:
            LOG.warning(
                'Failed to init sway pipeline: %s', err,
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
        '''Process a raw TLV file through the full sway
        analysis pipeline.

        Decodes TLV binary, wavelength-calibrates sweeps,
        runs peak detection with ``peak_detect`` config, and
        emits ``peak_data`` events.

        Args:
            path: Path to raw .tlv file.
            timestamp_s: Protocol timestamp in seconds.

        Returns:
            List of per-channel peak result dicts, or None
            if analysis_tools is not available or the file
            yielded no peaks.
        '''
        self._timestamp_s = timestamp_s

        if not self._ensure_sway():
            return None

        try:
            return self._process_with_sway(path)
        except Exception as err:
            LOG.error(
                'Pipeline error processing %s: %s',
                path, err,
            )
            return None

    def reset_baseline(self) -> None:
        '''Clear baselines and calibration for a new run.'''
        self._baseline.clear()
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
