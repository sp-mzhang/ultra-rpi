'''ultra.services.state_machine -- Headless Ultra state machine.

Drives the system through WiFi provisioning, AWS IoT
provisioning, cloud registration, and a continuous
protocol loop driven by drawer open/close events.

Ported from sway.ultra_state_machine with all GUI/Qt
dependencies removed. State changes are published to the
event bus; the web GUI and IoT client subscribe.
'''
from __future__ import annotations

import asyncio
import logging
import os
import threading
from enum import Enum
from typing import Any

from ultra.events import EventBus

LOG = logging.getLogger(__name__)


class SystemState(Enum):
    '''System states for the Ultra state machine.'''
    INITIALIZING = 'initializing'
    WIFI_PROVISIONING = 'wifi_provisioning'
    AWS_IOT_PROVISIONING = 'aws_iot_provisioning'
    CLOUD_REGISTRATION = 'cloud_registration'
    IDLE = 'idle'
    DRAWER_OPEN_LOAD_CARTRIDGE = (
        'drawer_open_load_cartridge'
    )
    SELF_CHECK = 'self_check'
    AWAITING_PROTOCOL_START = 'awaiting_protocol_start'
    RUNNING_PROTOCOL = 'running_protocol'
    PROTOCOL_COMPLETE = 'protocol_complete'
    DATA_UPLOAD = 'data_upload'
    ERROR = 'error'


STATUS_MESSAGES: dict[SystemState, str] = {
    SystemState.INITIALIZING: 'Initializing...',
    SystemState.WIFI_PROVISIONING: (
        'Waiting for WiFi...'
    ),
    SystemState.AWS_IOT_PROVISIONING: (
        'Provisioning IoT credentials...'
    ),
    SystemState.CLOUD_REGISTRATION: (
        'Registering with cloud...'
    ),
    SystemState.IDLE: (
        'Ready -- press button to open drawer'
    ),
    SystemState.DRAWER_OPEN_LOAD_CARTRIDGE: (
        'Drawer open -- load cartridge and close'
    ),
    SystemState.SELF_CHECK: 'Running self check...',
    SystemState.AWAITING_PROTOCOL_START: (
        'Drawer open -- load sample and close to start'
    ),
    SystemState.RUNNING_PROTOCOL: 'Running protocol...',
    SystemState.PROTOCOL_COMPLETE: 'Protocol complete',
    SystemState.DATA_UPLOAD: 'Uploading results...',
    SystemState.ERROR: 'Error',
}


LED_NONE = 0
LED_WAITING = 1
LED_READY = 2
LED_ERROR = 3
LED_PROGRESS = 4
LED_SCANNING = 5


class _SceneEvalError(RuntimeError):
    '''Hard failure inside :meth:`_evaluate_scene`.

    Raised when the SM cannot run the cartridge QR / tube check
    pair (STM32 unreachable, vision detector raised). The caller
    publishes ``cartridge_validation_failed`` so the cloud /
    mobile app surface a hard error, then loops back to the
    drawer-open state for an operator retry.
    '''


class UltraStateMachine:
    '''Headless async state machine for Ultra RPi.

    Runs through provisioning, then enters a continuous
    protocol loop driven by drawer open/close events from
    the STM32StatusMonitor.

    State transitions are published to the event bus. The
    web GUI and IoT client both subscribe to these events.

    Attributes:
        state: Current SystemState.
        chip_id: Chip ID from NFC/QR or default.
    '''

    def __init__(
            self,
            config: dict[str, Any],
            event_bus: EventBus,
            monitor: Any = None,
            iot_client: Any = None,
    ) -> None:
        self._config = config
        self._event_bus = event_bus
        self._monitor = monitor
        self._iot_client = iot_client

        self.state = SystemState.INITIALIZING
        self.chip_id: str = ''
        self._error_message: str = ''
        self._running = False

        self.drawer_opened_event = asyncio.Event()
        self.drawer_closed_event = asyncio.Event()

        self.protocol_trigger = asyncio.Event()
        self.protocol_done = asyncio.Event()
        self._last_analysis_result: dict[str, Any] | None = None

        startup = config.get('startup', {})
        self._skip_nfc = startup.get(
            'skip_nfc', False,
        )
        # Default is now False because ``checks.qr`` is wired up and
        # runs a real QR decode in ``_state_self_check``. Bench work
        # that doesn't have a QR-coded cartridge can flip this back
        # to True in ``config/ultra_default.yaml``.
        self._skip_qr = startup.get('skip_qr', False)
        # Tube check needs on-device calibration of
        # ``checks.tube.roi`` and the intensity / Hough thresholds
        # before it can meaningfully refuse to start a run. Ships
        # True so early builds are not blocked; flip to False once
        # calibration is locked in.
        self._skip_tube_check = startup.get(
            'skip_tube_check', True,
        )
        self._default_chip_id = startup.get(
            'default_chip_id', 'ULTRA-TEST-001',
        )
        self._self_check_stub_s = startup.get(
            'self_check_stub_s', 5.0,
        )
        self._restart_delay_s = startup.get(
            'restart_delay_s', 5.0,
        )

        # Two-stage observation-driven validation. Stage A
        # latches ``_cartridge_loaded`` when a single scene shows
        # QR-valid AND tube-absent. Stage B transitions to
        # RUNNING_PROTOCOL when ``_cartridge_loaded`` is true and
        # the next scene shows QR-valid AND tube-present. Drawer
        # cycle count is log-only -- the SM is order-free over
        # drawer cycles, only reacts to the observed scene.
        # Cloud telemetry is suppressed for soft retry conditions
        # (the started/ended pair fires at most once per assay,
        # except after a hard cartridge_validation_failed).
        self._cartridge_loaded: bool = False
        self._last_qr_payload: str | None = None
        self._cycle_count: int = 0
        self._validation_started_emitted: bool = False
        self._validation_ended_emitted: bool = False

        self._nfc_provisioner: Any = None
        self._wifi_provisioner: Any = None
        self._wifi_just_provisioned: bool = False

        self._apply_iot_config_to_env()

        if monitor:
            self.drawer_opened_event = (
                monitor.drawer_opened_event
            )
            self.drawer_closed_event = (
                monitor.drawer_closed_event
            )

    def _set_state(
            self, new_state: SystemState,
    ) -> None:
        '''Transition to a new state and publish to event bus.'''
        self.state = new_state
        msg = STATUS_MESSAGES.get(
            new_state, new_state.value,
        )
        if (
            new_state == SystemState.ERROR
            and self._error_message
        ):
            msg = f'Error: {self._error_message}'
        LOG.info(
            f'State -> {new_state.value}: {msg}',
        )
        self._event_bus.emit_sync(
            'status_changed', {
                'state': new_state.value,
                'message': msg,
            },
        )

    def _set_led(
            self, pattern: int, stage: int = 0,
    ) -> None:
        '''Send LED pattern to STM32 via monitor.'''
        if self._monitor is None:
            return
        try:
            self._monitor.send_led_pattern(
                pattern=pattern, stage=stage,
            )
        except Exception as err:
            LOG.warning(f'_set_led failed: {err}')

    def _publish_event(
            self, event_type: str, **kwargs: Any,
    ) -> None:
        '''Publish an event to IoT if available.'''
        if self._iot_client is None:
            LOG.debug(
                'IoT publish skipped (no client): %s',
                event_type,
            )
            return
        try:
            self._iot_client.publish_event(
                event_type=event_type, **kwargs,
            )
            LOG.info('IoT event published: %s', event_type)
        except Exception as err:
            LOG.warning(
                f'IoT publish failed: {err}',
            )

    def _ensure_monitor_running(self) -> None:
        '''Re-start STM32StatusMonitor if something stopped it.

        The engineering /stm32/connect endpoint, firmware
        updates, and protocol execution all call
        STM32StatusMonitor.stop_active() so they can take
        /dev/ttyAMA3 exclusively.  If one of them forgets to
        restart the monitor (e.g. user opened the engineering
        panel and never clicked Disconnect), the SM would sit
        forever in IDLE / SELF_CHECK / AWAITING_PROTOCOL_START
        because no MSG_STATUS frames are being read and the
        drawer events never fire.

        This helper is called on entry to every state that
        awaits a drawer event.  It releases the engineering
        interface if it is still holding the UART, then
        restarts the monitor.  Safe to call when the monitor
        is already running -- it's a no-op in that case.
        '''
        if self._monitor is None:
            return
        if getattr(self._monitor, '_running', False):
            return

        try:
            from ultra.gui._eng_state import eng_stm32
            iface = eng_stm32.get('iface')
            if iface is not None:
                LOG.warning(
                    'Engineering STM32 iface is holding the '
                    'UART -- releasing it so the monitor can '
                    'see drawer events',
                )
                try:
                    iface.disconnect()
                except Exception as err:
                    LOG.warning(
                        'iface.disconnect failed: %s', err,
                    )
                eng_stm32['iface'] = None
        except Exception as err:
            LOG.debug(
                'Could not inspect engineering iface: %s', err,
            )

        LOG.info(
            'Restarting STM32StatusMonitor so state machine '
            'can receive drawer events',
        )
        try:
            self._monitor.start()
        except Exception as err:
            LOG.error(
                'Failed to restart STM32StatusMonitor: %s', err,
            )

    def _seed_open_if_level(self) -> None:
        '''Fire drawer_opened_event if the door is already open.

        The monitor's _door_handler only fires on a rising
        edge (door_open False -> True).  When a state handler
        clears the event and starts waiting, the door may
        already be open -- in which case no rising edge is
        coming and the SM would block forever.  This seed
        makes the await level-sensitive on entry.
        '''
        if self._monitor is not None and self._monitor.is_door_open():
            LOG.info(
                'Drawer already open on entry '
                '-- advancing without waiting for edge',
            )
            self.drawer_opened_event.set()

    def _seed_closed_if_level(self) -> None:
        '''Fire drawer_closed_event if the door is already closed.

        Level-sensitive mirror of _seed_open_if_level; see
        that docstring for rationale.
        '''
        if self._monitor is not None and self._monitor.is_door_closed():
            LOG.info(
                'Drawer already closed on entry '
                '-- advancing without waiting for edge',
            )
            self.drawer_closed_event.set()


    def _apply_iot_config_to_env(self) -> None:
        '''Bridge YAML iot config into env vars.

        The iot_provisioning.config module reads settings
        from environment variables. This sets them from the
        YAML config so that ultra_default.yaml and
        machine.yaml values take effect without modifying
        the ported config module.
        '''
        iot_cfg = self._config.get('iot', {})

        _SIMPLE = {
            'endpoint': 'SIPHOX_IOT_ENDPOINT',
            'template': 'SIPHOX_IOT_TEMPLATE',
            'credentials_dir': 'SIPHOX_IOT_CREDENTIALS_DIR',
        }
        for yaml_key, env_key in _SIMPLE.items():
            val = iot_cfg.get(yaml_key)
            if val and env_key not in os.environ:
                os.environ[env_key] = str(val)

        claim_dir = iot_cfg.get('claim_cert_dir')
        if claim_dir:
            _CLAIM = {
                'SIPHOX_IOT_CLAIM_CERT': os.path.join(
                    claim_dir, 'claim.cert.pem',
                ),
                'SIPHOX_IOT_CLAIM_KEY': os.path.join(
                    claim_dir, 'claim.private.key',
                ),
                'SIPHOX_IOT_CA_CERT': os.path.join(
                    claim_dir, 'root-CA.crt',
                ),
            }
            for env_key, path in _CLAIM.items():
                if env_key not in os.environ:
                    os.environ[env_key] = path

    def _notify_device_ready_to_cloud(self) -> None:
        '''Publish device_ready with current WiFi snapshot.'''
        if self._iot_client is None:
            LOG.debug(
                'Skipping device_ready notify: no IoT client',
            )
            return
        from ultra.utils.network import check_wifi_connected

        device_sn = self._config.get('device_sn', 'unknown')
        station_id = self._config.get('station_id', -1)
        _, wifi_status = check_wifi_connected()
        try:
            self._iot_client.notify_device_ready(
                hw_status={
                    'device_sn': device_sn,
                    'station_id': station_id,
                    'ip_address': wifi_status.get('ip', ''),
                    'wifi_ssid': wifi_status.get('ssid', ''),
                },
            )
            LOG.info('Device ready notification sent')
        except Exception as err:
            LOG.warning(
                'Device ready notification failed '
                '(non-fatal): %s', err,
            )

    def _on_nfc_tap(self) -> None:
        '''Called by NFCService on phone tap (NFC thread).'''
        LOG.info('NFC tap detected')
        if (
            self.state == SystemState.IDLE
            and self._iot_client is not None
        ):
            LOG.info(
                'NFC tap in IDLE -- publishing device_ready',
            )
            self._notify_device_ready_to_cloud()

    def _on_ble_wifi_connected(
            self,
            ssid: str,
            ip_address: str,
    ) -> None:
        '''Called by WiFiProvisioner on BLE credential apply.'''
        LOG.info(
            'BLE WiFi connected: ssid=%s, ip=%s',
            ssid, ip_address,
        )
        if self._iot_client is None:
            LOG.warning(
                'BLE WiFi connected but no IoT client yet '
                '-- skipping register_device',
            )
            return
        self._notify_device_ready_to_cloud()

    def _on_analysis_complete(self, data: dict) -> None:
        '''Store analysis results for cloud publishing.'''
        self._last_analysis_result = data
        LOG.info(
            'Analysis result captured for cloud upload',
        )

    async def run(self) -> None:
        '''Main state machine loop.'''
        LOG.info('Starting Ultra state machine...')
        self._running = True
        self._set_state(SystemState.INITIALIZING)
        self._event_bus.on(
            'analysis_complete',
            self._on_analysis_complete,
        )

        while self._running:
            try:
                handler = self._STATE_HANDLERS.get(
                    self.state,
                )
                if handler is None:
                    LOG.warning(
                        'Unhandled state: '
                        f'{self.state.value}',
                    )
                    break
                await handler(self)
                if self.state == SystemState.ERROR:
                    break
            except asyncio.CancelledError:
                LOG.info('State machine cancelled')
                self._running = False
                break
            except Exception as err:
                LOG.error(
                    f'State machine error: {err}',
                )
                self._error_message = str(err)
                self._set_state(SystemState.ERROR)

        LOG.info('Ultra state machine exited')

    async def _state_initializing(self) -> None:
        '''Check WiFi and start NFC + BLE provisioners.

        Starts NFCWiFiProvisioner and WiFiProvisioner in
        daemon threads, then checks WiFi connectivity.
        If connected, proceeds to AWS_IOT_PROVISIONING;
        otherwise enters WIFI_PROVISIONING.
        '''
        from ultra.utils.network import check_wifi_connected

        device_sn = self._config.get(
            'device_sn', 'Setup',
        )
        device_name = f'SiPhox-{device_sn}'

        self.chip_id = self._default_chip_id

        if not self._skip_nfc:
            try:
                from ultra.services.nfc_provisioning.provisioner import (
                    NFCWiFiProvisioner,
                )
                self._nfc_provisioner = NFCWiFiProvisioner(
                    device_sn=device_sn,
                    advertise_while_connected=True,
                    on_tap=self._on_nfc_tap,
                )
                threading.Thread(
                    target=self._nfc_provisioner.run,
                    name='NFCProvisioningThread',
                    daemon=True,
                ).start()
                LOG.info(
                    'NFC provisioner started '
                    '(URL: siphox.com/setup?sn=%s)',
                    device_sn,
                )
            except Exception as err:
                LOG.warning(
                    'NFC provisioner unavailable: %s', err,
                )

        try:
            from ultra.services.wifi_provisioning.provisioner import (
                WiFiProvisioner,
            )
            self._wifi_provisioner = WiFiProvisioner(
                device_name=device_name,
                advertise_while_connected=True,
                on_wifi_connected=(
                    self._on_ble_wifi_connected
                ),
            )
            threading.Thread(
                target=self._wifi_provisioner.run,
                name='BLEProvisioningThread',
                daemon=True,
            ).start()
            LOG.info(
                'BLE provisioner started '
                '(advertising as %s)',
                device_name,
            )
        except Exception as err:
            LOG.warning(
                'BLE provisioner unavailable: %s', err,
            )

        wifi_connected, wifi_status = check_wifi_connected()
        if wifi_connected:
            LOG.info(
                'WiFi already connected: %s, IP: %s',
                wifi_status.get('ssid'),
                wifi_status.get('ip'),
            )
            self._set_state(
                SystemState.AWS_IOT_PROVISIONING,
            )
        else:
            LOG.warning(
                'WiFi not connected -- waiting for '
                'BLE/NFC provisioning',
            )
            self._set_state(SystemState.WIFI_PROVISIONING)

    async def _state_wifi_provisioning(self) -> None:
        '''Poll until WiFi connects via BLE/NFC provisioners.

        Both provisioners were started in _state_initializing.
        This state polls check_wifi_connected every 1s until
        connected, then stops provisioners and proceeds.
        '''
        from ultra.utils.network import check_wifi_connected

        LOG.info(
            'Waiting for WiFi credentials via BLE/NFC...',
        )

        while self._running:
            await asyncio.sleep(1.0)
            wifi_connected, wifi_status = (
                check_wifi_connected()
            )
            if wifi_connected:
                LOG.info(
                    'WiFi provisioning complete: '
                    'ssid=%s, ip=%s',
                    wifi_status.get('ssid'),
                    wifi_status.get('ip'),
                )
                if self._wifi_provisioner:
                    self._wifi_provisioner.stop()
                if self._nfc_provisioner:
                    self._nfc_provisioner.stop()
                self._wifi_just_provisioned = True
                self._set_state(
                    SystemState.AWS_IOT_PROVISIONING,
                )
                return

        self._error_message = 'WiFi provisioning failed'
        self._set_state(SystemState.ERROR)

    async def _state_aws_iot(self) -> None:
        '''Run AWS IoT fleet provisioning.

        Checks if device already has valid credentials.
        If not, runs IoTProvisioner in a background thread
        with retry logic. On success proceeds to
        CLOUD_REGISTRATION; on failure goes to ERROR.
        '''
        try:
            from ultra.services.iot_provisioning import (
                IoTProvisioner,
                is_provisioned,
            )
        except ImportError:
            LOG.warning(
                'IoT provisioning SDK not available '
                '-- skipping to CLOUD_REGISTRATION',
            )
            self._set_state(SystemState.CLOUD_REGISTRATION)
            return

        if is_provisioned():
            LOG.info('AWS IoT credentials already valid')
            self._set_state(SystemState.CLOUD_REGISTRATION)
            return

        device_sn = self._config.get(
            'device_sn', 'unknown',
        )
        station_id = self._config.get('station_id', -1)

        LOG.info(
            'Starting IoT provisioning for %s', device_sn,
        )

        prov_complete = threading.Event()
        prov_success = [False]

        def _run_provisioning() -> None:
            try:
                iot_prov = IoTProvisioner(
                    device_sn=device_sn,
                    station_id=station_id,
                )
                prov_success[0] = iot_prov.run()
            except Exception as err:
                LOG.error(
                    'AWS IoT provisioning error: %s', err,
                )
                prov_success[0] = False
            finally:
                prov_complete.set()

        prov_thread = threading.Thread(
            target=_run_provisioning,
            name='IoTProvisioningThread',
            daemon=True,
        )
        prov_thread.start()

        while (
            not prov_complete.is_set()
            and self._running
        ):
            await asyncio.sleep(1.0)
            if is_provisioned():
                prov_complete.set()
                prov_success[0] = True
                break

        if prov_success[0]:
            LOG.info('AWS IoT provisioning successful')
            self._set_state(SystemState.CLOUD_REGISTRATION)
        else:
            self._error_message = (
                'AWS IoT provisioning failed -- '
                'check claim certs in /etc/ultra/certs/ '
                'and IoT endpoint/template config'
            )
            LOG.error(self._error_message)
            self._set_state(SystemState.ERROR)

    async def _state_cloud_registration(self) -> None:
        '''Connect to IoT with device certs and publish device_ready.

        Uses the fleet provisioning IoTClient to connect with
        device certificates. On success, stores as _iot_client
        for subsequent event publishing. All operations are
        non-fatal: failures log a warning and proceed to IDLE
        with _iot_client=None.
        '''
        try:
            from ultra.services.iot_provisioning import (
                create_device_client,
                is_provisioned,
            )
        except ImportError:
            LOG.warning(
                'IoT provisioning SDK not available '
                '-- skipping cloud registration',
            )
            self._set_state(SystemState.IDLE)
            return

        if not is_provisioned():
            LOG.warning(
                'Device not provisioned -- '
                'skipping cloud connection',
            )
            self._set_state(SystemState.IDLE)
            return

        try:
            self._iot_client = create_device_client()
            if not self._iot_client:
                LOG.warning(
                    'Could not create IoT client -- '
                    'skipping cloud connection',
                )
                self._set_state(SystemState.IDLE)
                return

            loop = asyncio.get_event_loop()
            connected = await loop.run_in_executor(
                None,
                self._iot_client.connect_with_device_cert,
            )

            if not connected:
                LOG.warning(
                    'IoT client connection failed -- '
                    'skipping cloud connection',
                )
                self._iot_client = None
            else:
                LOG.info('IoT client connected')
                if self._wifi_just_provisioned:
                    LOG.info(
                        'WiFi just provisioned -- '
                        'registering device',
                    )
                    self._wifi_just_provisioned = False
                self._notify_device_ready_to_cloud()

        except Exception as err:
            LOG.error('Cloud connection error: %s', err)
            self._iot_client = None

        self._set_state(SystemState.IDLE)

    async def _state_idle(self) -> None:
        '''Wait for drawer to open.'''
        # Reset two-stage validation latches on every return to
        # IDLE (typically after PROTOCOL_COMPLETE -> DATA_UPLOAD).
        # Missing this reset would leave _cartridge_loaded True
        # and skip Stage A entirely on the next assay, and would
        # also suppress cartridge_validation_started so the cloud
        # would never see the assay start.
        self._cartridge_loaded = False
        self._last_qr_payload = None
        self._cycle_count = 0
        self._validation_started_emitted = False
        self._validation_ended_emitted = False

        self._set_led(LED_WAITING)
        self._publish_event('device_ready')
        LOG.info(
            'IDLE -- waiting for drawer open',
        )
        self._ensure_monitor_running()
        self.drawer_opened_event.clear()
        self._seed_open_if_level()
        await self.drawer_opened_event.wait()
        LOG.info('Drawer opened')
        self._set_state(
            SystemState.DRAWER_OPEN_LOAD_CARTRIDGE,
        )

    async def _state_drawer_open(self) -> None:
        '''Wait for cartridge load and drawer close.

        We do NOT publish ``cartridge_inserted`` here -- the
        legacy 5 s timer-thread fired blind on every drawer
        open regardless of whether a cartridge was actually
        loaded. The 2-stage SELF_CHECK validator now produces
        the canonical "cartridge loaded" milestone via
        ``cartridge_validation_ended`` (Stage A pass), which is
        only emitted after the QR has actually been read.
        '''
        self._set_led(LED_PROGRESS, stage=1)
        self._publish_event('drawer_open')
        LOG.info('Waiting for cartridge load + close')
        self._ensure_monitor_running()
        self.drawer_closed_event.clear()
        self._seed_closed_if_level()
        await self.drawer_closed_event.wait()
        LOG.info('Drawer closed')
        self._publish_event('drawer_closed')
        await asyncio.sleep(5.0)
        self._set_state(SystemState.SELF_CHECK)

    async def _state_self_check(self) -> None:
        '''Two-stage observation-driven cartridge + tube validation.

        Each cycle runs both vision checks under a single STM32
        session and then applies the decision tree:

            * QR invalid                       -> awaiting_cartridge
              (if previously latched, also publishes a hard
              cartridge_validation_failed because the cartridge
              was lost mid-flow)
            * QR ok, tube absent               -> latch
              _cartridge_loaded; first time this latches we emit
              cartridge_validation_ended (Stage A done -- the
              user must now load the serum tube and close the
              drawer to start the assay)
            * QR ok, tube present, latched     -> RUNNING_PROTOCOL
              (Stage B; _state_running_protocol publishes
              test_started)
            * QR ok, tube present, NOT latched -> awaiting_cartridge
              (tube loaded too early; soft retry, no cloud event)

        Drawer-cycle count is log-only -- the SM is order-free
        over drawer cycles. Only the observed scene drives state.

        Cloud event policy (per
        docs/CLOUD_APP_STATE_RECONCILIATION.md section 8): we
        publish only canonical events the cloud expects.
        ``cartridge_validation_started`` fires once on first
        entry; ``cartridge_validation_ended`` fires once on Stage
        A success; ``cartridge_validation_failed`` fires only on
        hard hardware/detector failures or when a previously
        latched cartridge becomes invalid (cartridge_lost). Soft
        retry conditions (no QR yet, tube too early) are
        local-only via ``self_check_substate`` on the EventBus.
        '''
        self._cycle_count += 1
        LOG.info(
            'SELF_CHECK cycle %d (cartridge_loaded=%s)',
            self._cycle_count,
            self._cartridge_loaded,
        )

        # Emit cartridge_validation_started exactly once per
        # assay (reset in _state_idle). This is the cloud's
        # "validation in progress" anchor.
        if not self._validation_started_emitted:
            self._publish_event(
                'cartridge_validation_started',
                extra={'cycle': self._cycle_count},
            )
            self._validation_started_emitted = True

        # Bench-mode skip handling. We always call _evaluate_scene
        # honoring the run_qr/run_tube flags so the gantry/camera
        # are not exercised when bench-mode disables them.
        run_qr = not self._skip_qr
        run_tube = not self._skip_tube_check

        try:
            qr_ok, qr_payload, tube_present = (
                await self._evaluate_scene(
                    run_qr=run_qr, run_tube=run_tube,
                )
            )
        except _SceneEvalError as exc:
            # Hard hardware / detector failure. Surface it to the
            # cloud as cartridge_validation_failed so the mobile
            # app shows an error, then loop back to drawer-open
            # so the operator can retry.
            LOG.error(
                'SELF_CHECK cycle %d: scene eval failed: %s',
                self._cycle_count, exc,
            )
            self._publish_event(
                'cartridge_validation_failed',
                extra={
                    'reason': str(exc),
                    'cycle': self._cycle_count,
                },
            )
            self._emit_substate(
                'awaiting_cartridge', reason=str(exc),
            )
            await self._loop_back_to_drawer_open()
            return

        if self._skip_qr:
            qr_ok = True
            qr_payload = (
                qr_payload
                or self._last_qr_payload
                or 'BENCH'
            )
        if self._skip_tube_check:
            # Bench auto-advance: cycle 1 (cartridge not yet
            # latched) -> tube_absent so Stage A latches; cycle 2
            # onward -> tube_present so Stage B passes. Lets a
            # single physical drawer cycle still take the bench
            # all the way to RUNNING_PROTOCOL.
            tube_present = self._cartridge_loaded

        # Decision tree.
        if not qr_ok:
            if self._cartridge_loaded:
                LOG.warning(
                    'SELF_CHECK cycle %d: QR invalid AFTER '
                    'Stage A latch -- publishing '
                    'cartridge_validation_failed',
                    self._cycle_count,
                )
                self._publish_event(
                    'cartridge_validation_failed',
                    extra={
                        'reason': (
                            'cartridge_lost_after_validation'
                        ),
                        'cycle': self._cycle_count,
                    },
                )
                # Reset the validation_ended latch so a re-load
                # can publish a fresh validation_ended on the
                # next Stage A pass within this assay.
                self._validation_ended_emitted = False
            self._cartridge_loaded = False
            self._emit_substate(
                'awaiting_cartridge', reason='qr_invalid',
            )
        elif qr_ok and not tube_present:
            first_pass = not self._cartridge_loaded
            self._cartridge_loaded = True
            self._last_qr_payload = qr_payload
            if first_pass and not self._validation_ended_emitted:
                self._publish_event(
                    'cartridge_validation_ended',
                    cartridge_id=qr_payload,
                    extra={'cycle': self._cycle_count},
                )
                self._validation_ended_emitted = True
            self._emit_substate(
                'cartridge_loaded_awaiting_tube',
                qr=qr_payload,
            )
        elif (
            qr_ok and tube_present and self._cartridge_loaded
        ):
            LOG.info(
                'SELF_CHECK cycle %d: Stage B passed -- '
                'starting protocol (qr=%r)',
                self._cycle_count, qr_payload,
            )
            self._set_state(SystemState.RUNNING_PROTOCOL)
            return
        else:
            self._emit_substate(
                'awaiting_cartridge',
                reason=(
                    'tube_present_before_cartridge_validation'
                ),
            )

        # Loop back through DRAWER_OPEN_LOAD_CARTRIDGE so the
        # operator can adjust the carousel and try again.
        # _state_drawer_open already publishes drawer_open on
        # entry and drawer_closed on close, so we do NOT
        # re-publish drawer_open here.
        await self._loop_back_to_drawer_open()

    async def _loop_back_to_drawer_open(self) -> None:
        '''Wait for the next drawer-open edge and re-enter the
        DRAWER_OPEN_LOAD_CARTRIDGE state so the operator can
        adjust the cartridge / tube and try again.'''
        self._ensure_monitor_running()
        self.drawer_opened_event.clear()
        self._seed_open_if_level()
        await self.drawer_opened_event.wait()
        self._set_state(
            SystemState.DRAWER_OPEN_LOAD_CARTRIDGE,
        )

    def _emit_substate(
            self, substate: str, **fields: Any,
    ) -> None:
        '''Publish a self_check_substate event on the local bus.

        Used to drive the engineering GUI banner and any other
        in-process listeners. Never reaches the cloud (cloud
        contract is limited to the canonical event vocabulary).
        '''
        payload: dict[str, Any] = {
            'substate': substate,
            'cycle': self._cycle_count,
            'cartridge_loaded': self._cartridge_loaded,
        }
        payload.update(fields)
        try:
            self._event_bus.emit_sync(
                'self_check_substate', payload,
            )
        except Exception as exc:
            LOG.debug(
                'self_check_substate emit failed: %s', exc,
            )
        LOG.info(
            'SELF_CHECK substate=%s cycle=%d %r',
            substate, self._cycle_count, fields,
        )

    async def _evaluate_scene(
            self,
            *,
            run_qr: bool,
            run_tube: bool,
    ) -> tuple[bool, str | None, bool]:
        '''Run one combined QR + tube observation.

        Acquires the UART (mirroring the prior
        ``_run_validation_checks`` plumbing), runs whichever
        checks are enabled, returns the raw scene tuple
        ``(qr_ok, qr_payload, tube_present)``.

        Cloud events are emitted by the caller
        (``_state_self_check``) based on the latch transition,
        not here -- this helper is purely a detector wrapper.

        Hard failures (STM32 connect failed, an exception in a
        check) raise :class:`_SceneEvalError` so the caller can
        publish ``cartridge_validation_failed``.
        '''
        from ultra.hw.stm32_interface import STM32Interface
        from ultra.hw.stm32_monitor import STM32StatusMonitor
        from ultra.hw.camera_singleton import get_camera
        from ultra.vision import check_runner

        if not (run_qr or run_tube):
            return (False, None, False)

        stm32_cfg = self._config.get('stm32', {}) or {}
        port = stm32_cfg.get('port', '/dev/ttyAMA3')
        baud = stm32_cfg.get('baud', 921600)

        STM32StatusMonitor.stop_active()
        await asyncio.sleep(0.5)

        qr_ok = False
        qr_payload: str | None = None
        tube_present = False
        eval_error: str | None = None

        stm32: STM32Interface | None = None
        try:
            stm32 = STM32Interface(port=port, baud=baud)
            if not stm32.connect():
                raise _SceneEvalError(
                    'stm32_connect_failed',
                )
            if hasattr(
                    stm32, 'apply_motion_defaults_from_config',
            ):
                stm32.apply_motion_defaults_from_config(
                    self._config,
                )

            cam = get_camera(self._config)

            def _get_frame(settle_ms: int):
                import time as _time
                baseline = cam.latest_frame_ts()
                settle_s = max(0.0, settle_ms / 1000.0)
                if settle_s > 0:
                    _time.sleep(settle_s)
                frame, _ts = cam.latest_frame_bgr(
                    newer_than=baseline, wait_s=5.0,
                )
                return frame

            if run_qr:
                try:
                    qr_res = await asyncio.to_thread(
                        check_runner.run_cartridge_qr_check,
                        stm32=stm32,
                        config=self._config,
                        get_frame=_get_frame,
                        cache_frame=None,
                    )
                except Exception as exc:
                    LOG.exception('QR check raised: %s', exc)
                    eval_error = f'qr_exception:{exc}'
                else:
                    qr_ok = bool(qr_res.ok)
                    qr_payload = qr_res.payload
                    if qr_ok:
                        LOG.info(
                            'QR check passed (payload=%r)',
                            qr_payload,
                        )
                    else:
                        LOG.info(
                            'QR check failed: %s',
                            qr_res.reason,
                        )

            if run_tube:
                try:
                    tube_res = await asyncio.to_thread(
                        check_runner.run_serum_tube_check,
                        stm32=stm32,
                        config=self._config,
                        get_frame=_get_frame,
                        cache_frame=None,
                    )
                except Exception as exc:
                    LOG.exception(
                        'Tube check raised: %s', exc,
                    )
                    eval_error = (
                        eval_error or f'tube_exception:{exc}'
                    )
                else:
                    tube_present = bool(tube_res.ok)
                    if tube_present:
                        LOG.info('Tube check: present')
                    else:
                        LOG.info(
                            'Tube check: absent (reason=%s)',
                            tube_res.reason,
                        )
        finally:
            if stm32 is not None:
                try:
                    stm32.disconnect()
                except Exception as exc:
                    LOG.warning(
                        'scene_eval: stm32.disconnect '
                        'failed: %s', exc,
                    )
            await asyncio.sleep(0.5)
            if self._monitor is not None:
                try:
                    self._monitor.start()
                except Exception as exc:
                    LOG.warning(
                        'scene_eval: monitor.start failed: %s',
                        exc,
                    )

        if eval_error is not None:
            raise _SceneEvalError(eval_error)
        return (qr_ok, qr_payload, tube_present)

    async def _state_awaiting_start(self) -> None:
        '''Wait for user to close drawer to start protocol.'''
        self._set_led(LED_PROGRESS, stage=2)
        LOG.info(
            'Awaiting blood sample -- '
            'waiting for close',
        )
        self._ensure_monitor_running()
        self.drawer_closed_event.clear()
        self._seed_closed_if_level()
        await self.drawer_closed_event.wait()
        LOG.info('Drawer closed -- starting protocol')
        self._publish_event('drawer_closed')
        await asyncio.sleep(8.0)
        self._set_state(SystemState.RUNNING_PROTOCOL)

    async def _state_running_protocol(self) -> None:
        '''Start protocol and wait for completion.'''
        self._publish_event('test_started')
        self._set_led(LED_SCANNING)

        from ultra.hw.stm32_monitor import (
            STM32StatusMonitor,
        )
        STM32StatusMonitor.stop_active()
        await asyncio.sleep(0.5)

        self.protocol_done.clear()
        self.protocol_trigger.set()

        LOG.info('Waiting for protocol to complete...')
        while (
            not self.protocol_done.is_set()
            and self._running
        ):
            await asyncio.sleep(2.0)
        LOG.info('Protocol done')

        # Let the protocol's STM32Interface fully release the UART
        # before the monitor reopens the same port. STM32Interface.
        # disconnect() joins its RX/TX threads and closes the fd,
        # but the kernel tty layer needs a beat to release the
        # device -- otherwise we end up with two readers on
        # /dev/ttyAMA3 and the monitor misses MSG_STATUS broadcasts.
        # The same empirical delay (0.5-1.2 s) is used in
        # api_protocol.py /run and api_stm32.py /stm32/connect.
        await asyncio.sleep(0.5)

        if self._monitor is not None:
            self._monitor.start()

        self._set_led(LED_READY)
        self._set_state(SystemState.PROTOCOL_COMPLETE)

    async def _state_protocol_complete(self) -> None:
        '''Post-protocol: publish events, advance to upload.'''
        self._publish_event('test_completed')

        if self._last_analysis_result:
            analytes = self._last_analysis_result.get(
                'analytes', [],
            )
            self._publish_event(
                'analysis_complete',
                analyte_data=analytes,
            )
            LOG.info(
                'Analysis results published to cloud: '
                '%d analytes', len(analytes),
            )
            self._last_analysis_result = None

        LOG.info('Protocol complete')
        self._set_state(SystemState.DATA_UPLOAD)

    async def _state_data_upload(self) -> None:
        '''Upload results then return to idle.'''
        LOG.info('Data upload (placeholder)...')
        await asyncio.sleep(2.0)
        self._set_state(SystemState.IDLE)

    async def _state_error(self) -> None:
        '''Handle error state.'''
        self._set_led(LED_ERROR)
        LOG.error(
            f'State machine error: '
            f'{self._error_message}',
        )
        await asyncio.sleep(self._restart_delay_s)

    def stop(self) -> None:
        '''Stop the state machine and release resources.'''
        LOG.info('Stopping state machine...')
        self._running = False
        if self._nfc_provisioner:
            try:
                self._nfc_provisioner.stop()
            except Exception:
                pass
            self._nfc_provisioner = None
        if self._wifi_provisioner:
            try:
                self._wifi_provisioner.stop()
            except Exception:
                pass
            self._wifi_provisioner = None
        if self._iot_client:
            try:
                self._iot_client.disconnect()
            except Exception as err:
                LOG.warning(
                    'IoT client disconnect error: %s', err,
                )
            self._iot_client = None

    _STATE_HANDLERS = {
        SystemState.INITIALIZING: (
            _state_initializing
        ),
        SystemState.WIFI_PROVISIONING: (
            _state_wifi_provisioning
        ),
        SystemState.AWS_IOT_PROVISIONING: (
            _state_aws_iot
        ),
        SystemState.CLOUD_REGISTRATION: (
            _state_cloud_registration
        ),
        SystemState.IDLE: _state_idle,
        SystemState.DRAWER_OPEN_LOAD_CARTRIDGE: (
            _state_drawer_open
        ),
        SystemState.SELF_CHECK: _state_self_check,
        SystemState.AWAITING_PROTOCOL_START: (
            _state_awaiting_start
        ),
        SystemState.RUNNING_PROTOCOL: (
            _state_running_protocol
        ),
        SystemState.PROTOCOL_COMPLETE: (
            _state_protocol_complete
        ),
        SystemState.DATA_UPLOAD: _state_data_upload,
        SystemState.ERROR: _state_error,
    }
