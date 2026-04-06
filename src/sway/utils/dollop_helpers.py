'''Stub for sway.utils.dollop_helpers.

Provides the symbols imported by analysis_tools at module
level.  These functions are only called in sway-specific
code paths (Dollop device lookup) that ultra-rpi never
reaches.
'''


def db_api_fetch_device_by_id_or_serial_number(
        id_str: str = '',
        serial_number: str = '',
) -> dict:
    raise NotImplementedError(
        'dollop_helpers stub -- not available in ultra-rpi',
    )


def db_api_device_get_config(
        device_uuid: str = '',
) -> dict:
    raise NotImplementedError(
        'dollop_helpers stub -- not available in ultra-rpi',
    )


def fetch_run_info(
        run_group_path: str = '',
        **kwargs,
) -> dict:
    raise NotImplementedError(
        'dollop_helpers stub -- not available in ultra-rpi',
    )
