'''AnalysisService -- local concentration analysis.

Reads run data (peaks_nm.log + sample.log) and calibration
files (analysis_config.yaml + fitting_protocol_sheet.xlsx)
to compute per-analyte concentration via dose-response curve
inversion using the analysis-model-store libraries.

Sensor group assignment is fetched from Dollop when a chip_id
is available, falling back to the local analysis_config.yaml
groups mapping when the Dollop API is unreachable.
'''
from __future__ import annotations

import logging
import os.path as op
from dataclasses import dataclass, field
from typing import Any

import yaml

LOG = logging.getLogger(__name__)


@dataclass
class AnalyteResult:
    '''Single analyte concentration result.'''
    analyte: str
    concentration: float | None
    unit: str
    signal: float | None = None
    fit_type: str = ''
    in_range: bool = True
    excluded_by_validation: bool = False


@dataclass
class AnalysisResult:
    '''Collection of analyte results for a run.'''
    analytes: list[AnalyteResult] = field(default_factory=list)
    run_dir: str = ''
    error: str = ''

    @property
    def ok(self) -> bool:
        return not self.error and len(self.analytes) > 0

    def to_dict(self) -> dict[str, Any]:
        return {
            'analytes': [
                {
                    'analyte': a.analyte,
                    'concentration': a.concentration,
                    'unit': a.unit,
                    'signal': a.signal,
                    'fit_type': a.fit_type,
                    'in_range': a.in_range,
                    'excluded_by_validation': (
                        a.excluded_by_validation
                    ),
                }
                for a in self.analytes
            ],
            'run_dir': self.run_dir,
            'error': self.error,
        }


def _groups_from_print_map(
        print_map: list[dict[str, str]],
) -> dict[str, list[int]]:
    '''Convert a Dollop microprint map to group dict.

    Each entry has ``capture_reagent`` and ``sensor`` keys.
    Returns ``{reagent_code: [sensor_indices]}``.
    '''
    groups: dict[str, list[int]] = {}
    for entry in print_map:
        cr = entry.get('capture_reagent')
        sensor_str = entry.get('sensor')
        if not cr or sensor_str is None:
            continue
        try:
            sensor = int(sensor_str)
        except (ValueError, TypeError):
            continue
        groups.setdefault(cr, []).append(sensor)
    return groups


def fetch_groups_from_dollop(
        chip_id: str,
) -> dict[str, list[int]] | None:
    '''Try to fetch sensor groups from Dollop via chip_id.

    Returns the group map or ``None`` on any failure.
    '''
    try:
        from ultra.services.dollop_client import (
            _client,
        )
        from siphox.dollopclient.api import (
            chips as dc_chips,
        )
        chips_api = dc_chips.ChipsAPI(client=_client())
        prints_list = chips_api.get_microarray_prints(chip_id)
        if not prints_list:
            LOG.warning(
                'Dollop returned no prints for chip %s',
                chip_id,
            )
            return None
        print_map = prints_list[0].get('print_map', [])
        groups = _groups_from_print_map(print_map)
        LOG.info(
            'Fetched groups from Dollop for chip %s: %s',
            chip_id, groups,
        )
        return groups
    except Exception as exc:
        LOG.warning(
            'Failed to fetch groups from Dollop '
            'for chip %s: %s', chip_id, exc,
        )
        return None


class _MinimalAnalysisConfig:
    '''Minimal stand-in for analysis.AnalysisConfiguration.

    ``validation_lib.apply_rule_to_measurements`` only reads
    ``slope_units`` from the config object.
    '''

    def __init__(self, slope_units: str = 'pm/min') -> None:
        self.slope_units = slope_units


class AnalysisService:
    '''Runs local concentration analysis for a single run.

    Uses calibration data from the local cache (synced from S3)
    and the analysis-model-store fitting / validation libraries.
    '''

    def __init__(
            self,
            assay: str = 'crp',
            version: str = 'v1.0',
            calib_cache: str = (
                '/tmp/ultra_config_cache/calibration_data'
            ),
            chip_id: str = '',
    ) -> None:
        self._assay = assay
        self._version = version
        self._calib_dir = op.join(calib_cache, assay, version)
        self._chip_id = chip_id

    def _ensure_calib_cached(self) -> None:
        '''Sync calibration files from S3 if not already cached.'''
        check = op.join(self._calib_dir, 'analysis_config.yaml')
        if op.isfile(check):
            return
        try:
            from ultra.services.config_store import (
                sync_calibration_version,
            )
            paths = sync_calibration_version(
                self._assay, self._version,
            )
            LOG.info(
                'Synced %d calibration files for %s/%s',
                len(paths), self._assay, self._version,
            )
        except Exception as exc:
            LOG.warning(
                'Failed to sync calibration from S3: %s',
                exc,
            )

    def _load_calib_config(self) -> dict[str, Any]:
        '''Load analysis_config.yaml from calibration dir.'''
        self._ensure_calib_cached()
        path = op.join(self._calib_dir, 'analysis_config.yaml')
        if not op.isfile(path):
            raise FileNotFoundError(
                f'Calibration config not found: {path}',
            )
        with open(path, encoding='utf-8') as fh:
            return yaml.safe_load(fh) or {}

    def _find_peaks_log(self, run_dir: str) -> str | None:
        '''Locate peaks_nm.log (sway laser.log equivalent).'''
        for name in ('peaks_nm.log', 'laser.log'):
            p = op.join(run_dir, name)
            if op.isfile(p):
                return p
        return None

    def _find_sample_log(self, run_dir: str) -> str | None:
        '''Locate sample.log in the run directory.'''
        p = op.join(run_dir, 'sample.log')
        return p if op.isfile(p) else None

    # ----------------------------------------------------------
    # Validation
    # ----------------------------------------------------------

    def _run_validation(
            self,
            meas: Any,
            slope_units: str,
    ) -> tuple[set[int], set[str], dict[str, set[int]]]:
        '''Parse validation_rules_sheet.xlsx and apply rules.

        Returns:
            (rings_excluded, cartridge_excluded_assays,
             rings_excluded_by_assay).
        '''
        rules_path = op.join(
            self._calib_dir,
            'validation_rules_sheet.xlsx',
        )
        if not op.isfile(rules_path):
            LOG.info(
                'No validation_rules_sheet.xlsx found; '
                'skipping validation',
            )
            return set(), set(), {}

        try:
            from validation_lib import (
                apply_rules_to_run,
                build_cartridges_to_exclude,
                build_rings_to_exclude,
                parse_rule_sheet,
            )
        except ImportError:
            LOG.warning(
                'validation_lib not available; '
                'skipping validation',
            )
            return set(), set(), {}

        rules, _ = parse_rule_sheet(rules_path)
        if not rules:
            return set(), set(), {}

        cfg = _MinimalAnalysisConfig(slope_units=slope_units)
        results = apply_rules_to_run(
            meas=meas,
            rules=rules,
            analysis_config=cfg,  # type: ignore[arg-type]
        )

        rings_excl, rings_by_assay = build_rings_to_exclude(
            results,
        )
        always_excl, assay_cart_excl = (
            build_cartridges_to_exclude(results)
        )
        if always_excl:
            LOG.warning(
                'Validation: entire cartridge excluded',
            )

        LOG.info(
            'Validation: rings_excluded=%s, '
            'cartridge_excluded_assays=%s, '
            'assay_rings_excluded=%s',
            rings_excl, assay_cart_excl, rings_by_assay,
        )
        return rings_excl, assay_cart_excl, rings_by_assay

    # ----------------------------------------------------------
    # Main entry point
    # ----------------------------------------------------------

    def analyze(self, run_dir: str) -> AnalysisResult:
        '''Run concentration analysis on a completed run.

        Args:
            run_dir: Path to the run directory containing
                peaks_nm.log and sample.log.

        Returns:
            AnalysisResult with per-analyte concentrations.
        '''
        result = AnalysisResult(run_dir=run_dir)

        try:
            calib = self._load_calib_config()
        except FileNotFoundError as exc:
            result.error = str(exc)
            LOG.warning('Analysis skipped: %s', exc)
            return result

        params = calib.get('parameters', {})
        yaml_groups = params.get('groups', {})

        groups: dict[str, list[int]] = {}
        if self._chip_id:
            dollop_groups = fetch_groups_from_dollop(
                self._chip_id,
            )
            if dollop_groups:
                groups = dollop_groups
        if not groups:
            LOG.info(
                'Using groups from analysis_config.yaml',
            )
            groups = yaml_groups

        peak_processing = params.get(
            'peak_processing', 'Peak Difference',
        )
        stitch_th = params.get('stitch_th', 1.15)
        slope_units = params.get('slope_units', 'pm/min')

        peaks_path = self._find_peaks_log(run_dir)
        sample_path = self._find_sample_log(run_dir)

        if not peaks_path:
            result.error = (
                'No peaks_nm.log or laser.log found in '
                + run_dir
            )
            LOG.warning('Analysis skipped: %s', result.error)
            return result

        fitting_sheet = op.join(
            self._calib_dir, 'fitting_protocol_sheet.xlsx',
        )
        if not op.isfile(fitting_sheet):
            result.error = (
                'fitting_protocol_sheet.xlsx not found in '
                + self._calib_dir
            )
            LOG.warning('Analysis skipped: %s', result.error)
            return result

        try:
            result = self._run_analysis(
                run_dir=run_dir,
                peaks_path=peaks_path,
                sample_path=sample_path,
                fitting_sheet=fitting_sheet,
                groups=groups,
                peak_processing=peak_processing,
                stitch_th=stitch_th,
                slope_units=slope_units,
            )
        except Exception as exc:
            result.error = f'Analysis failed: {exc}'
            LOG.exception('Analysis error')

        return result

    def _run_analysis(
            self,
            run_dir: str,
            peaks_path: str,
            sample_path: str | None,
            fitting_sheet: str,
            groups: dict[str, list[int]],
            peak_processing: str,
            stitch_th: float,
            slope_units: str = 'pm/min',
    ) -> AnalysisResult:
        '''Core analysis using analysis-model-store libs.'''
        from demo import (
            FIT_TYPE_MODEL_FUNC_INFO,
            parse_fit_protocol_sheet,
        )
        from siphox.analysis_tools.main_analysis import (
            MeasurementAnalysis,
        )

        meas = MeasurementAnalysis(run_id=-1)

        int_groups = None
        if groups:
            int_groups = {
                k: [int(s) for s in v]
                for k, v in groups.items()
            }

        meas.load_laser(
            peaks_path,
            peak_processing=peak_processing,
            stitch_th=stitch_th,
            groups_dict=int_groups,
        )
        if sample_path:
            meas.load_samples(sample_path)

        # --- Validation: exclude bad rings / cartridges ---
        rings_excluded, cart_excl_assays, rings_by_assay = (
            self._run_validation(meas, slope_units)
        )

        analyte_params_list, _ = parse_fit_protocol_sheet(
            fitting_sheet,
        )

        result = AnalysisResult(run_dir=run_dir)

        for ap in analyte_params_list:
            if ap.exclude_from_summary:
                continue

            assay_name = ap.name

            if assay_name in cart_excl_assays:
                LOG.warning(
                    'Cartridge excluded for assay %s '
                    'by validation rules', assay_name,
                )
                result.analytes.append(AnalyteResult(
                    analyte=assay_name,
                    concentration=None,
                    unit=ap.concentration_units,
                    signal=None,
                    fit_type=ap.fit_type,
                    in_range=False,
                    excluded_by_validation=True,
                ))
                continue

            fit_key = ap.fit_type.lower().strip()
            fit_info = FIT_TYPE_MODEL_FUNC_INFO.get(fit_key)
            if not fit_info:
                LOG.warning(
                    'Unknown fit type %r for %s',
                    ap.fit_type, ap.name,
                )
                continue

            fit_params = ap.fit_parameters
            if not fit_params or len(fit_params) < 2:
                LOG.warning(
                    'No fit parameters for %s', ap.name,
                )
                continue

            assay_excl = rings_by_assay.get(
                assay_name, set(),
            )
            excluded = rings_excluded | assay_excl

            signal = self._compute_signal(
                meas, ap, groups, excluded,
            )
            if signal is None:
                LOG.warning(
                    'No signal data for %s', ap.name,
                )
                result.analytes.append(AnalyteResult(
                    analyte=assay_name,
                    concentration=None,
                    unit=ap.concentration_units,
                    signal=None,
                    fit_type=ap.fit_type,
                    in_range=False,
                ))
                continue

            scaled_signal = signal * ap.signal_scaling_factor

            try:
                conc = float(
                    fit_info.inverse_func(
                        scaled_signal, *fit_params,
                    ),
                )
            except Exception as exc:
                LOG.warning(
                    'Inverse fit failed for %s: %s',
                    ap.name, exc,
                )
                conc = None

            in_range = True
            if conc is not None:
                if (
                    ap.minimum_concentration is not None
                    and conc < ap.minimum_concentration
                ):
                    in_range = False
                if (
                    ap.maximum_concentration is not None
                    and conc > ap.maximum_concentration
                ):
                    in_range = False
                conc = round(
                    conc, ap.concentration_num_decimals,
                )

            result.analytes.append(AnalyteResult(
                analyte=assay_name,
                concentration=conc,
                unit=ap.concentration_units,
                signal=round(scaled_signal, 4),
                fit_type=ap.fit_type,
                in_range=in_range,
            ))

        LOG.info(
            'Analysis complete: %d analytes, run_dir=%s',
            len(result.analytes), run_dir,
        )
        return result

    def _compute_signal(
            self,
            meas: Any,
            ap: Any,
            groups: dict[str, list[int]],
            excluded_rings: set[int] | None = None,
    ) -> float | None:
        '''Compute the net signal for an analyte.

        Uses the capture_group from the analyte parameters
        to select sensor channels, then computes the mean
        shift in the analysis window.  Rings in
        ``excluded_rings`` are skipped.

        In sway v5.1.0, data lives in ``meas.sensor_df``
        (columns ``sensor1``..``sensor15``, time index)
        and step boundaries in ``meas.samples_df``.
        '''
        import numpy as np

        if excluded_rings is None:
            excluded_rings = set()

        capture = ap.capture_group
        if not capture or capture not in groups:
            channels = list(range(1, 16))
        else:
            channels = groups[capture]

        channels = [
            ch for ch in channels
            if ch not in excluded_rings
        ]
        if not channels:
            return None

        neg_channels: list[int] = []
        neg_group = ap.subtract_negative_control_capture_group
        if neg_group and neg_group in groups:
            neg_channels = [
                ch for ch in groups[neg_group]
                if ch not in excluded_rings
            ]

        step_idx = ap.step_to_analyze
        start_offset = ap.start_time_after_step_begins_secs
        window = ap.window_secs

        try:
            sdf = meas.sensor_df
            if sdf.empty:
                return None
            times = np.array(sdf.index)

            t_start = None
            t_end = None
            if (
                not meas.samples_df.empty
                and step_idx is not None
                and step_idx < len(meas.samples_df)
            ):
                row = meas.samples_df.iloc[step_idx]
                t_start = (
                    row['start_time_shifted']
                    + start_offset
                )
                t_end = (
                    t_start + window
                    if window
                    else row['end_time_shifted']
                )

            signals: list[float] = []
            for ch in channels:
                col = f'sensor{ch}'
                if col not in sdf.columns:
                    continue
                shifts = sdf[col].dropna()
                if shifts.empty:
                    continue

                if t_start is not None:
                    mask = (
                        (shifts.index >= t_start) &
                        (shifts.index <= t_end)
                    )
                    windowed = shifts[mask]
                    if not windowed.empty:
                        signals.append(
                            float(windowed.mean()),
                        )
                else:
                    if len(shifts) > 10:
                        signals.append(
                            float(shifts.iloc[-10:].mean()),
                        )
                    elif len(shifts) > 0:
                        signals.append(
                            float(shifts.mean()),
                        )

            if not signals:
                return None

            mean_signal = sum(signals) / len(signals)

            if neg_channels:
                neg_signals: list[float] = []
                for ch in neg_channels:
                    col = f'sensor{ch}'
                    if col not in sdf.columns:
                        continue
                    shifts = sdf[col].dropna()
                    if not shifts.empty:
                        neg_signals.append(
                            float(
                                shifts.iloc[-10:].mean(),
                            ),
                        )
                if neg_signals:
                    neg_mean = (
                        sum(neg_signals)
                        / len(neg_signals)
                    )
                    mean_signal -= neg_mean

            return mean_signal

        except Exception as exc:
            LOG.warning(
                'Signal computation error for %s: %s',
                ap.name, exc,
            )
            return None
