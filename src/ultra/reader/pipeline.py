'''ultra.reader.pipeline -- Analysis pipeline glue.

Bridges raw TLV data from the acquisition service to
the siphox.analysis_tools package (installed from sway
repo via pip). Uses the real sway peak detection API:

Data flow:
  raw TLV file
  -> readertospectra.process_tlv_file_to_sweeps()
     (decode, demux adc_id 1/2, unpack 15-ch interleave,
      bin sweeps, wavelength-calibrate)
  -> peaks.run_peak_detection(channels, SweepData,
     config_peak_detect, chip_mapping)
  -> event_bus.emit('peak_data', ...)

Requires siphox.analysis_tools to be installed:
  pip install "analysis-tools @ git+https://..."
'''
from __future__ import annotations

import logging
import os.path as op
import sys
import tempfile
import time as _time
import types
from typing import Any

# analysis_tools has unguarded module-level imports from
# sway.utils.dollop_helpers (in tlv_proc.py / tlv_utils.py).
# Inject empty stub modules so the import chain succeeds
# without installing the full sway desktop application.
if 'sway' not in sys.modules:
    _sway = types.ModuleType('sway')
    _sway_utils = types.ModuleType('sway.utils')
    _sway_dh = types.ModuleType('sway.utils.dollop_helpers')
    _sway.utils = _sway_utils  # type: ignore[attr-defined]
    _sway_utils.dollop_helpers = _sway_dh  # type: ignore[attr-defined]

    def _not_available(*a, **kw):
        raise NotImplementedError(
            'sway.utils.dollop_helpers stub'
        )

    _sway_dh.fetch_run_info = _not_available
    _sway_dh.db_api_fetch_device_by_id_or_serial_number = (
        _not_available
    )
    _sway_dh.db_api_device_get_config = _not_available

    sys.modules['sway'] = _sway
    sys.modules['sway.utils'] = _sway_utils
    sys.modules['sway.utils.dollop_helpers'] = _sway_dh

from ultra.events import EventBus

LOG = logging.getLogger(__name__)


class ReaderPipeline:
    '''Glue layer between raw TLV data and analysis tools.

    Uses siphox.analysis_tools to decode TLV binary data,
    demux PProc/SProc streams, de-interleave 15 optical
    channels, wavelength-calibrate sweeps, run peak
    detection, and emit per-channel wavelength results
    to the event bus.

    Also writes sway-compatible log files:
    - ``peaks_nm.log`` (via ``peaklogutils``)
    - ``resonance_props.csv`` (via ``save_resonances_log``)

    Attributes:
        _event_bus: Application event bus.
        _peak_config: Peak detection config dict from YAML.
        _baseline: Per-channel baseline wavelengths for
            optional shift calculation.
        _wl_rth_fit: Persistent wavelength-vs-thermistor
            calibration (mutated across TLV files).
        _pow_rth_fit: Persistent power calibration.
        _peaks_nm_fp: Path to peaks_nm.log (set on first
            successful calibration sweep).
        _resonance_props_fp: Path to resonance_props.csv.
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
        self._peaks_nm_fp: str | None = None
        self._resonance_props_fp: str | None = None
        self._sweep_idx: int = 0

    def set_run_dir(self, run_dir: str) -> None:
        '''Set the run directory for TlvToSpectra logs.

        Reinitialises the internal TlvToSpectra instance
        so that any resonance-property CSV files land in
        the correct run directory.

        Args:
            run_dir: Absolute path to the current run dir.
        '''
        self._run_dir = run_dir
        self._tlvtospec = None
        self._sway_ok = None

    # ----------------------------------------------------------
    # Lazy init
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
        except Exception:
            LOG.exception(
                'Failed to import siphox.analysis_tools. '
                'Peak detection disabled. If not installed '
                'run: uv sync',
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
                'analysis_tools pipeline initialised '
                '(run_dir=%s)', run_dir,
            )
        except Exception:
            LOG.exception(
                'Failed to init TlvToSpectra -- '
                'peak detection disabled',
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
        '''Process a raw TLV file through the sway analysis
        pipeline.

        Decodes TLV binary (demuxes PProc/SProc streams,
        de-interleaves 15 optical channels), applies
        wavelength calibration, runs peak detection per
        channel, and emits ``peak_data`` events.

        Args:
            path: Path to raw .tlv file.
            timestamp_s: Protocol timestamp in seconds.

        Returns:
            List of per-channel peak result dicts, or None
            if the file yielded no peaks.
        '''
        self._timestamp_s = timestamp_s
        self._block_count += 1

        if not self._ensure_sway():
            return None

        try:
            return self._process_with_sway(path)
        except Exception:
            LOG.exception(
                'Pipeline error processing block %d: %s',
                self._block_count, path,
            )
            return None

    def reset_baseline(self) -> None:
        '''Clear baselines and calibration for a new run.'''
        self._baseline.clear()
        self._block_count = 0
        self._sweep_idx = 0
        self._wl_rth_fit = [None, None]
        self._pow_rth_fit = [None, None]
        self._tlvtospec = None
        self._sway_ok = None
        self._peaks_nm_fp = None
        self._resonance_props_fp = None
        LOG.debug('Pipeline baseline and calibration reset')

    # ----------------------------------------------------------
    # Internal: sway pipeline
    # ----------------------------------------------------------

    def _process_with_sway(
            self, path: str,
    ) -> list[dict] | None:
        '''Run the full sway decode -> calibrate -> detect
        pipeline on a single TLV file.

        Also writes peaks_nm.log rows and resonance_props.csv
        rows for every sweep, matching sway's on-disk format.

        Args:
            path: Path to .tlv file on disk.

        Returns:
            List of peak result dicts or None.
        '''
        import numpy as np
        from siphox.analysis_tools.readertospectra import (
            process_tlv_file_to_sweeps,
        )
        from siphox.analysis_tools.utils import (
            peaklogutils,
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
            LOG.debug(
                'No sweeps in block %d (%s)',
                self._block_count, path,
            )
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
            time_start_s = round(
                float(t_data[0])
                if len(t_data) > 0
                else self._timestamp_s,
                2,
            )
            peak_cfg['t'] = time_start_s

            res_props, peaks_nm, peaks_by_sensor = (
                span_peaks.run_peak_detection(
                    channels=channels,
                    latest_sweep=sweep,
                    config_peak_detect=peak_cfg,
                    chip_mapping=chip_map,
                )
            )

            self._sweep_idx += 1

            self._write_log_files(
                peaklogutils, span_peaks,
                chip_map, peak_cfg,
                peaks_nm, peaks_by_sensor,
                res_props, time_start_s,
            )

            tracked = span_peaks.track_peaks(peaks_nm)
            self._emit_peaks(tracked)
            results.extend(self._last_emitted)

        if results:
            LOG.info(
                'Block %d: %d peaks from %d sweeps',
                self._block_count,
                len(results),
                len(calibrated.wavelength_sweep_list),
            )
        return results or None

    # ----------------------------------------------------------
    # Log file writers (peaks_nm.log, resonance_props.csv)
    # ----------------------------------------------------------

    def _write_log_files(
            self,
            peaklogutils: Any,
            span_peaks: Any,
            chip_map: dict[str, Any],
            peak_cfg: dict[str, Any],
            peaks_nm: list,
            peaks_by_sensor: list,
            res_props: list[dict[str, Any]],
            time_start_s: float,
    ) -> None:
        '''Write peaks_nm.log row and resonance_props rows.

        Initialises the log files lazily on the first sweep
        that has a valid wavelength calibration, matching
        sway's deferred-init pattern.

        Args:
            peaklogutils: Imported peaklogutils module.
            span_peaks: Imported peaks module.
            chip_map: Chip mapping dict.
            peak_cfg: Peak detection config for this sweep.
            peaks_nm: Per-channel peak wavelengths from
                ``run_peak_detection``.
            peaks_by_sensor: Per-sensor peak lists from
                ``run_peak_detection``.
            res_props: Resonance property dicts from
                ``run_peak_detection``.
            time_start_s: Sweep start time in seconds.
        '''
        if not self._run_dir:
            return

        if (
            self._peaks_nm_fp is None
            and self._wl_rth_fit[0] is not None
        ):
            try:
                self._peaks_nm_fp = (
                    peaklogutils.init_peaks_nm_log(
                        run_dir_path=self._run_dir,
                        peak_config=peak_cfg,
                        chip_mapping=chip_map,
                        wl_rth_fit=self._wl_rth_fit[0],
                        initial_cal_time_sweep_s=(
                            time_start_s
                        ),
                    )
                )
                LOG.info(
                    'peaks_nm.log created: %s',
                    self._peaks_nm_fp,
                )
            except Exception:
                LOG.exception(
                    'Failed to init peaks_nm.log',
                )

        if self._resonance_props_fp is None:
            rp = op.join(
                self._run_dir, 'resonance_props.csv',
            )
            try:
                peaklogutils.init_resonance_props_log(
                    run_dir_path=self._run_dir,
                    resonance_props_fp=rp,
                    peak_config=peak_cfg,
                    logger=LOG,
                )
                self._resonance_props_fp = rp
                LOG.info(
                    'resonance_props.csv created: %s', rp,
                )
            except Exception:
                LOG.exception(
                    'Failed to init resonance_props.csv',
                )

        if self._peaks_nm_fp is not None and peaks_nm:
            try:
                sensor_names = (
                    peaklogutils
                    .sensor_column_names_from_chip_mapping(
                        chip_map,
                    )
                )
                peaklogutils.append_peaks_nm_row(
                    peaks_nm_fp=self._peaks_nm_fp,
                    time_logged=_time.time(),
                    sweep_idx=self._sweep_idx,
                    time_sweep_s=time_start_s,
                    sensor_names=sensor_names,
                    peaks_nm=peaks_by_sensor,
                )
            except Exception:
                LOG.exception(
                    'Failed to append peaks_nm row',
                )

        if self._resonance_props_fp is not None:
            try:
                peaklogutils.save_resonances_log(
                    resonance_props_fp=(
                        self._resonance_props_fp
                    ),
                    resonance_props=res_props,
                    run_dir_path=self._run_dir,
                    peak_config=peak_cfg,
                )
            except Exception:
                LOG.exception(
                    'Failed to save resonance props',
                )

    def _emit_peaks(
            self,
            peaks_now: list[Any],
    ) -> None:
        '''Convert tracked peaks to dicts and emit events.

        Emits one ``peak_data`` event per channel with
        absolute wavelength (nm), shift from baseline (pm),
        and timestamp. Channels are 1-based (1..15).

        Args:
            peaks_now: Per-channel list of PeakNm | None
                from ``span_peaks.track_peaks()``.
        '''
        self._last_emitted: list[dict] = []
        for ch_idx, peak in enumerate(peaks_now):
            if peak is None:
                continue
            wl = peak[0]
            ch = ch_idx + 1

            if ch not in self._baseline:
                self._baseline[ch] = wl

            shift_pm = (
                (wl - self._baseline[ch]) * 1000
            )

            result = {
                'channel': ch,
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
