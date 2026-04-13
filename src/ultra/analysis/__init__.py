'''Local concentration analysis for Ultra RPi.

Provides :func:`run_analysis` which reads run data
(peaks_nm.log + sample.log) and calibration files
(analysis_config.yaml + fitting_protocol_sheet.xlsx),
computes per-analyte concentration via dose-response
curve inversion, and returns structured results.
'''
from __future__ import annotations

from ultra.analysis.analyzer import AnalysisResult, AnalysisService

__all__ = ['AnalysisResult', 'AnalysisService', 'run_analysis']


def run_analysis(
        run_dir: str,
        assay: str,
        version: str,
        calib_cache: str = '/tmp/ultra_config_cache/calibration_data',
        chip_id: str = '',
) -> AnalysisResult:
    '''Convenience wrapper for a single run analysis.'''
    svc = AnalysisService(
        assay=assay,
        version=version,
        calib_cache=calib_cache,
        chip_id=chip_id,
    )
    return svc.analyze(run_dir)
