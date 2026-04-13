'''ultra.reader.pipeline -- Analysis pipeline glue.

Bridges raw TLV data from the acquisition service to
siphox.analysis_tools (sway v5.1.0).  Data flow:

  raw TLV file
  -> process_tlv_file_to_sweeps()   (decode + calibrate)
  -> TlvToSpectra.process_calibrated_sweeps()
     (peak detect, stitch, laser.log, peaks_nm.log,
      resonance_props.csv)
  -> event_bus.emit('peak_data' / 'sweep_data')

Reader calibration (tia_gain, fsr_nm, ...) is fetched
from Dollop at init time via fetch_reader_config().
'''
from __future__ import annotations

import logging
import os.path as op
import sys
import tempfile
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

    def _db_api_fetch_device(
            id_str='', serial_number='',
    ):
        from ultra.services import (
            dollop_client as _dollop,
        )
        from siphox.dollopclient.api import (
            devices as _ddev,
        )
        dev_api = _ddev.DevicesAPI(
            client=_dollop._client(),
        )
        name = serial_number or id_str
        if not name.startswith('reader'):
            name = f'reader{name}'
        devs = dev_api.get_devices(
            filters=[{
                'col': 'name', 'opr': 'eq',
                'value': name,
            }],
            page_size=1,
        )
        return devs[0] if devs else {}

    def _db_api_device_get_config(device_uuid=''):
        from ultra.services import (
            dollop_client as _dollop,
        )
        from siphox.dollopclient.api import (
            devices as _ddev,
        )
        dev_api = _ddev.DevicesAPI(
            client=_dollop._client(),
        )
        return dev_api.get_device_config(device_uuid) or {}

    _sway_dh.fetch_run_info = _not_available
    _sway_dh.db_api_fetch_device_by_id_or_serial_number = (
        _db_api_fetch_device
    )
    _sway_dh.db_api_device_get_config = (
        _db_api_device_get_config
    )

    sys.modules['sway'] = _sway
    sys.modules['sway.utils'] = _sway_utils
    sys.modules['sway.utils.dollop_helpers'] = _sway_dh

from ultra.events import EventBus

LOG = logging.getLogger(__name__)


class ReaderPipeline:
    '''Glue layer between raw TLV data and analysis tools.

    Delegates all heavy lifting to sway's
    ``siphox.analysis_tools`` package:

    * ``process_tlv_file_to_sweeps`` -- decode, demux,
      wavelength-calibrate
    * ``TlvToSpectra.process_calibrated_sweeps`` -- peak
      detection, stitching, laser.log, peaks_nm.log,
      resonance_props.csv

    Reader calibration parameters (tia_gain, fsr_nm, etc.)
    are fetched from Dollop at init time.  If Dollop is
    unreachable, ``LOW_POWER_CUTOFF_DB`` is lowered as a
    fallback so that RPi reader sweeps are not silently
    discarded.
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
        self._laser_fp: str = ''

    def set_peak_config(
            self, peak_cfg: dict[str, Any],
    ) -> None:
        '''Replace peak detection config (e.g. per-recipe).'''
        self._peak_config = dict(peak_cfg)

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
        Fetches reader calibration parameters from Dollop
        to populate ``reader_params_dict``; falls back to
        an empty dict with a lowered power-cutoff threshold
        if Dollop is unreachable.

        Returns True if analysis_tools is importable and
        the instance was created successfully.
        '''
        if self._sway_ok is not None:
            return self._sway_ok

        try:
            import siphox.analysis_tools.readertospectra \
                as _rts
            import siphox.analysis_tools.utils.tlv_proc \
                as _tlv_proc
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

        reader_cfg = self._config.get('reader', {})
        reader_name = reader_cfg.get(
            'dollop_name', 'reader7',
        )

        reader_params: dict[str, Any] = {}
        try:
            from ultra.services import (
                dollop_client as dollop,
            )
            reader_params = dollop.fetch_reader_config(
                reader_name,
            )
        except Exception:
            LOG.warning(
                'Could not fetch reader config from '
                'Dollop for %s', reader_name,
            )

        if not reader_params:
            cutoff = self._peak_config.get(
                'low_power_cutoff_db', -60,
            )
            _rts.LOW_POWER_CUTOFF_DB = cutoff
            _tlv_proc.LOW_POWER_CUTOFF_DB = cutoff
            LOG.info(
                'No reader params from Dollop; patched '
                'LOW_POWER_CUTOFF_DB to %s', cutoff,
            )

        try:
            run_dir = self._run_dir or tempfile.mkdtemp(
                prefix='ultra-pipeline-',
            )
            self._tlvtospec = TlvToSpectra(
                chip_pos=0,
                run_dir_path=run_dir,
                reader_params_dict=reader_params,
                peak_config=dict(self._peak_config),
                do_broadcast=False,
            )
            self._sway_ok = True

            cfg = self._tlvtospec.config
            LOG.info(
                'TlvToSpectra initialised '
                '(run_dir=%s, reader_params=%d keys)',
                run_dir, len(reader_params),
            )
            LOG.info(
                'Reader config: tia_gain=%.0e eta=%.2f '
                'fsr_nm=%.4f rth_high_v=%.2f '
                'rth_low_v=%.2f wm_prom=%.3f '
                'wavelength_offset_ref=%.2f',
                cfg.get('tia_gain', 0),
                cfg.get('eta', 0),
                cfg.get('fsr_nm', 0),
                cfg.get('rth_high_v', 0),
                cfg.get('rth_low_v', 0),
                cfg.get('wm_prom', 0),
                cfg.get('wavelength_offset_reference', 0),
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
        self._wl_rth_fit = [None, None]
        self._pow_rth_fit = [None, None]
        self._tlvtospec = None
        self._sway_ok = None
        self._peaks_nm_fp = None
        self._laser_fp = ''
        LOG.debug('Pipeline baseline and calibration reset')

    # ----------------------------------------------------------
    # Internal: sway pipeline
    # ----------------------------------------------------------

    def _init_peaks_nm_log(self) -> None:
        '''Create peaks_nm.log header (deferred until first
        successful wavelength calibration, matching sway).
        '''
        from siphox.analysis_tools.utils import (
            peaklogutils,
        )
        from siphox.analysis_tools.utils.\
            fitting_functions import peaks as span_peaks

        chip_map = (
            span_peaks
            .minimal_chip_mapping_for_channels(
                self._tlvtospec.no_ch,
            )
        )
        self._laser_fp = op.join(
            self._run_dir, 'laser.log',
        )
        self._peaks_nm_fp = peaklogutils.init_peaks_nm_log(
            run_dir_path=self._run_dir,
            peak_config=dict(self._peak_config),
            chip_mapping=chip_map,
            wl_rth_fit=self._wl_rth_fit[0],
            initial_cal_time_sweep_s=self._timestamp_s,
        )
        LOG.info(
            'peaks_nm.log created: %s',
            self._peaks_nm_fp,
        )

    def _process_with_sway(
            self, path: str,
    ) -> list[dict] | None:
        '''Run the full sway decode -> calibrate -> detect
        pipeline on a single TLV file.

        Uses ``process_tlv_file_to_sweeps`` to decode and
        wavelength-calibrate, then delegates peak detection,
        log writing, and stitching to
        ``TlvToSpectra.process_calibrated_sweeps``.

        Args:
            path: Path to .tlv file on disk.

        Returns:
            List of peak result dicts or None.
        '''
        import siphox.analysis_tools.readertospectra as _rts
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

        n_cal = len(calibrated.wavelength_sweep_list)
        n_bloss = sum(
            1 for b in calibrated.b_loss_list if b
        )
        cfg = self._tlvtospec.config
        LOG.info(
            'Block %d: tia_gain=%.0e eta=%.2f '
            'LOW_POWER_CUTOFF_DB=%s | '
            '%d calibrated sweeps, %d b_loss, %d usable',
            self._block_count,
            cfg.get('tia_gain', 0),
            cfg.get('eta', 0),
            getattr(_rts, 'LOW_POWER_CUTOFF_DB', '?'),
            n_cal, n_bloss, n_cal - n_bloss,
        )

        if not calibrated.wavelength_sweep_list:
            return None

        if (
            self._peaks_nm_fp is None
            and self._run_dir
            and self._wl_rth_fit[0] is not None
        ):
            try:
                self._init_peaks_nm_log()
            except Exception:
                LOG.exception(
                    'Failed to init peaks_nm.log',
                )

        chip_map = (
            span_peaks
            .minimal_chip_mapping_for_channels(
                self._tlvtospec.no_ch,
            )
        )

        prev_sweeps = self._tlvtospec.num_sweeps

        self._tlvtospec.process_calibrated_sweeps(
            proc_logger=LOG,
            laser_fp=getattr(self, '_laser_fp', ''),
            time_relative_start_s=self._timestamp_s,
            time_end_exact_s=self._timestamp_s,
            calibrated_sweeps=calibrated,
            do_save_data_files=bool(self._run_dir),
            do_broadcast=False,
            peaks_nm_fp=self._peaks_nm_fp,
            chip_mapping=chip_map,
        )

        new_sweeps = (
            self._tlvtospec.num_sweeps - prev_sweeps
        )
        if new_sweeps > 0:
            self._emit_peaks(self._tlvtospec.peaks_now)
            sweep = self._tlvtospec.latest_sweep
            self._emit_sweep(
                sweep.x_axis,
                sweep.sensor_curves,
                sweep.no_ch,
            )
            LOG.info(
                'Block %d: %d new sweeps processed, '
                '%d total',
                self._block_count,
                new_sweeps,
                self._tlvtospec.num_sweeps,
            )
            return self._last_emitted or None

        LOG.info(
            'Block %d: 0 peaks (peaks_nm.log %s)',
            self._block_count,
            'exists' if self._peaks_nm_fp else
            'NOT created',
        )
        return None

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

    def _emit_sweep(
            self,
            wl_arr: Any,
            sensor_arr: Any,
            no_ch: int,
    ) -> None:
        '''Emit latest sweep spectrum for the GUI.

        Sends one ``sweep_data`` event containing the
        wavelength x-axis and per-channel dB curves so the
        web UI can render a live spectrum plot (matching
        sway's Spectrum view).

        The arrays are down-sampled to keep the WebSocket
        payload manageable (~every 4th point).

        Args:
            wl_arr: 1-D wavelength array (nm).
            sensor_arr: 2-D sensor array (no_ch x points)
                or 1-D for single channel, in dB.
            no_ch: Number of channels in this sweep.
        '''
        import numpy as np
        step = max(1, len(wl_arr) // 200)
        wl_ds = wl_arr[::step]
        curves: dict[int, list[float]] = {}
        for ch_idx in range(no_ch):
            row = (
                sensor_arr[ch_idx]
                if sensor_arr.ndim > 1
                else sensor_arr
            )
            ds = row[::step]
            vals = [
                round(float(v), 2)
                if not np.isnan(v) else None
                for v in ds
            ]
            curves[ch_idx + 1] = vals

        self._event_bus.emit_sync(
            'sweep_data',
            {
                'wavelengths': [
                    round(float(w), 4) for w in wl_ds
                ],
                'curves': curves,
            },
        )
