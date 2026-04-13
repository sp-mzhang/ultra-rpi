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
        '''Initialize the state machine.

        Args:
            config: Application configuration dict.
            event_bus: Application event bus.
            monitor: STM32StatusMonitor instance (optional).
            iot_client: IoT client instance (optional).
        '''
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

        startup = config.get('startup', {})
        self._skip_nfc = startup.get(
            'skip_nfc', False,
        )
        self._skip_qr = startup.get('skip_qr', True)
        self._default_chip_id = startup.get(
            'default_chip_id', 'ULTRA-TEST-001',
        )
        self._self_check_stub_s = startup.get(
            'self_check_stub_s', 5.0,
        )
        self._restart_delay_s = startup.get(
            'restart_delay_s', 5.0,
        )

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
        '''Transition to a new state.

        Publishes state change to event bus.

        Args:
            new_state: Target SystemState.
        '''
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
        '''Send LED pattern to STM32 via monitor.

        Args:
            pattern: LED pattern ID (0-5).
            stage: Progress stage (pattern=4 only).
        '''
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
        '''Publish an event to IoT if available.

        Args:
            event_type: Event type string.
            **kwargs: Additional event data.
        '''
        if self._iot_client is not None:
            try:
                self._iot_client.publish_event(
                    event_type=event_type, **kwargs,
                )
            except Exception as err:
                LOG.warning(
                    f'IoT publish failed: {err}',
                )

    async def run(self) -> None:
        '''Main state machine loop.

        Transitions through states sequentially. After
        protocol completion, loops back to IDLE for the
        next run. Only exits on ERROR or stop().
        '''
        LOG.info('Starting Ultra state machine...')
        self._running = True
        self._set_state(SystemState.INITIALIZING)

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
        '''Initialize hardware and check connectivity.

        Routes through CLOUD_REGISTRATION when an IoT
        client was provided; otherwise skips to IDLE.
        '''
        self.chip_id = self._default_chip_id
        LOG.info('Initialization complete')
        if self._iot_client is not None:
            self._set_state(
                SystemState.CLOUD_REGISTRATION,
            )
        else:
            self._set_state(SystemState.IDLE)

    async def _state_wifi_provisioning(self) -> None:
        '''Wait for WiFi to connect.

        Placeholder -- real BLE/NFC provisioning will be
        wired when wifi_provisioner and nfc_service are
        implemented.
        '''
        LOG.info('WiFi provisioning (placeholder)...')
        await asyncio.sleep(2.0)
        self._set_state(
            SystemState.AWS_IOT_PROVISIONING,
        )

    async def _state_aws_iot(self) -> None:
        '''AWS IoT provisioning placeholder.'''
        LOG.info('AWS IoT provisioning (placeholder)...')
        await asyncio.sleep(1.0)
        self._set_state(
            SystemState.CLOUD_REGISTRATION,
        )

    async def _state_cloud_registration(self) -> None:
        '''Register with cloud via IoT client.

        Publishes a device_ready event so the cloud knows
        this device is online, then transitions to IDLE.
        '''
        LOG.info('Cloud registration...')
        if self._iot_client is not None:
            try:
                self._publish_event('device_ready')
                LOG.info(
                    'Cloud registration: device_ready sent',
                )
            except Exception as exc:
                LOG.warning(
                    'Cloud registration publish failed: %s',
                    exc,
                )
        self._set_state(SystemState.IDLE)

    async def _state_idle(self) -> None:
        '''Wait for drawer to open.'''
        self._set_led(LED_WAITING)
        self._publish_event('device_ready')
        LOG.info(
            'IDLE -- waiting for drawer open',
        )
        self.drawer_opened_event.clear()
        await self.drawer_opened_event.wait()
        LOG.info('Drawer opened')
        self._set_state(
            SystemState.DRAWER_OPEN_LOAD_CARTRIDGE,
        )

    async def _state_drawer_open(self) -> None:
        '''Wait for cartridge load and drawer close.'''
        self._set_led(LED_PROGRESS, stage=1)
        self._publish_event('drawer_open')
        LOG.info('Waiting for cartridge load + close')
        self.drawer_closed_event.clear()
        await self.drawer_closed_event.wait()
        LOG.info('Drawer closed')
        await asyncio.sleep(5.0)
        self._set_state(SystemState.SELF_CHECK)

    async def _state_self_check(self) -> None:
        '''Cartridge validation stub.'''
        LOG.info('Self check (stub)...')
        self._publish_event(
            'cartridge_validation_started',
        )
        await asyncio.sleep(self._self_check_stub_s)
        self._publish_event(
            'cartridge_validation_ended',
        )

        LOG.info(
            'Waiting for 2nd drawer open '
            '(blood sample)',
        )
        self.drawer_opened_event.clear()
        await self.drawer_opened_event.wait()
        self._set_state(
            SystemState.AWAITING_PROTOCOL_START,
        )

    async def _state_awaiting_start(self) -> None:
        '''Wait for user to close drawer to start protocol.'''
        self._set_led(LED_PROGRESS, stage=2)
        LOG.info(
            'Awaiting blood sample -- '
            'waiting for close',
        )
        self.drawer_closed_event.clear()
        await self.drawer_closed_event.wait()
        LOG.info('Drawer closed -- starting protocol')
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

        if self._monitor is not None:
            self._monitor.start()

        self._set_led(LED_READY)
        self._set_state(SystemState.PROTOCOL_COMPLETE)

    async def _state_protocol_complete(self) -> None:
        '''Post-protocol: publish event, advance to upload.'''
        self._publish_event('test_completed')
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
        '''Stop the state machine.'''
        LOG.info('Stopping state machine...')
        self._running = False

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
