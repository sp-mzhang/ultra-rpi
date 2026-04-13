'''ultra.app -- Main entry point for Ultra RPi controller.

Boots the application:
  1. Load configuration
  2. Start STM32StatusMonitor (door/sensor listening)
  3. Start FastAPI GUI server on :8080
  4. Optionally start the StateMachine task
  5. Run asyncio event loop forever

Usage::

    python -m ultra.app
'''
from __future__ import annotations

import asyncio
import logging
import os
import signal
import sys
from typing import Any

from ultra.config import load_config
from ultra.events import EventBus
from ultra.utils.logging import setup_logging

LOG = logging.getLogger(__name__)


class Application:
    '''Top-level application orchestrator.

    Owns the event bus and coordinates all services.

    Attributes:
        config: Loaded configuration dict.
        event_bus: Shared async event bus.
    '''

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.event_bus = EventBus()
        self._monitor = None
        self._state_machine = None
        self._sm_task: asyncio.Task | None = None
        self._sm_trigger_task: asyncio.Task | None = None
        self._egress_svc = None
        self._egress_task: asyncio.Task | None = None
        self._runner = None
        self._stm32 = None
        self._reader = None

    async def start(self) -> None:
        '''Boot all services and run until shutdown.'''
        loop = asyncio.get_event_loop()
        self.event_bus.set_loop(loop)

        use_mock = os.environ.get(
            'ULTRA_MOCK', '',
        ).lower() in ('1', 'true', 'yes')

        if use_mock:
            from ultra.hw.stm32_mock import STM32Mock
            self._stm32 = STM32Mock()
            self._stm32.connect()
            LOG.info('Using STM32Mock (no hardware)')
        else:
            self._start_monitor(loop)

        from ultra.gui.server import create_app
        gui_cfg = self.config.get('gui', {})
        host = gui_cfg.get('host', '0.0.0.0')
        port = gui_cfg.get('port', 8080)

        app = create_app(self)
        self._start_gui(app, host, port)

        try:
            from ultra.services import config_store
            await loop.run_in_executor(
                None,
                config_store.sync_recipes_and_shared_from_s3,
            )
            LOG.info('S3 recipe catalog sync attempted')
        except Exception as exc:
            LOG.warning('S3 recipe sync skipped: %s', exc)

        egress_cfg = self.config.get('egress', {})
        if egress_cfg.get('enabled', False):
            self._start_egress()

        startup_cfg = self.config.get('startup', {})
        if startup_cfg.get('auto_state_machine', False):
            self._start_state_machine()

        LOG.info(
            f'Ultra RPi ready -- '
            f'GUI at http://{host}:{port}',
        )

        stop_event = asyncio.Event()

        def _signal_handler():
            LOG.info('Shutdown signal received')
            stop_event.set()

        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, _signal_handler)

        await stop_event.wait()
        await self.shutdown()

    def _start_monitor(
            self,
            loop: asyncio.AbstractEventLoop,
    ) -> None:
        '''Start the STM32StatusMonitor.'''
        from ultra.hw.stm32_monitor import (
            STM32StatusMonitor,
        )

        stm32_cfg = self.config.get('stm32', {})
        self._monitor = STM32StatusMonitor(
            loop=loop,
            event_bus=self.event_bus,
            port=stm32_cfg.get(
                'port', '/dev/ttyAMA3',
            ),
            baud=stm32_cfg.get('baud', 921600),
        )
        if not self._monitor.start():
            LOG.warning(
                'STM32StatusMonitor failed to start '
                '-- running without hardware status',
            )
            self._monitor = None

    def _start_gui(
            self,
            app: Any,
            host: str,
            port: int,
    ) -> None:
        '''Start the FastAPI GUI server as a background task.'''
        import uvicorn

        uvi_config = uvicorn.Config(
            app=app,
            host=host,
            port=port,
            log_level='warning',
        )
        server = uvicorn.Server(uvi_config)
        asyncio.ensure_future(server.serve())
        LOG.info(
            f'GUI server starting on {host}:{port}',
        )

    def _start_egress(self) -> None:
        '''Start the egress service as a background task.'''
        from ultra.services.egress import EgressService
        self._egress_svc = EgressService(
            config=self.config,
            event_bus=self.event_bus,
        )
        self._egress_task = asyncio.ensure_future(
            self._egress_svc.start(),
        )
        LOG.info('EgressService started')

    def _start_state_machine(self) -> None:
        '''Start the state machine as a background task.

        The state machine creates its own IoT client after
        fleet provisioning completes (in CLOUD_REGISTRATION).
        '''
        from ultra.services.state_machine import (
            UltraStateMachine,
        )

        self._state_machine = UltraStateMachine(
            config=self.config,
            event_bus=self.event_bus,
            monitor=self._monitor,
        )
        self._sm_task = asyncio.ensure_future(
            self._state_machine.run(),
        )
        self._sm_trigger_task = asyncio.ensure_future(
            self._watch_protocol_trigger(),
        )
        LOG.info('State machine started')

    async def _watch_protocol_trigger(self) -> None:
        '''Wait for the state machine to fire protocol_trigger,
        then run the configured recipe through ProtocolRunner.

        Sets protocol_done when the run finishes (success or
        failure) so the state machine can advance.
        '''
        sm = self._state_machine
        while sm and sm._running:
            sm.protocol_trigger.clear()
            await sm.protocol_trigger.wait()
            LOG.info(
                'protocol_trigger received -- '
                'auto-starting protocol',
            )

            proto_cfg = self.config.get('protocol', {})
            recipe = proto_cfg.get(
                'default_recipe', 'crp_ultra',
            )
            startup_cfg = self.config.get('startup', {})
            chip_id = (
                getattr(sm, 'chip_id', '')
                or startup_cfg.get(
                    'default_chip_id', 'ULTRA-TEST-001',
                )
            )

            try:
                runner = self.get_runner()

                from ultra.hw.stm32_interface import (
                    STM32Interface,
                )
                stm32 = self._stm32
                if stm32 is None:
                    stm32_cfg = self.config.get('stm32', {})
                    stm32 = STM32Interface(
                        port=stm32_cfg.get(
                            'port', '/dev/ttyAMA3',
                        ),
                        baud=stm32_cfg.get('baud', 921600),
                    )
                    if not stm32.connect():
                        LOG.error(
                            'STM32 connect failed '
                            '-- cannot run protocol',
                        )
                        sm.protocol_done.set()
                        continue
                runner.stm32 = stm32

                loop = asyncio.get_running_loop()
                self.event_bus.set_loop(loop)

                def _hw_init_and_run():
                    stm32.send_command_wait_done(
                        cmd={'cmd': 'pump_init'},
                        timeout_s=30.0,
                    )
                    stm32.send_command_wait_done(
                        cmd={'cmd': 'home_all'},
                        timeout_s=60.0,
                    )
                    return runner._run_sync(
                        recipe,
                        chip_id=chip_id,
                        note='State machine auto-run',
                    )

                await loop.run_in_executor(
                    None, _hw_init_and_run,
                )
            except asyncio.CancelledError:
                raise
            except Exception as err:
                LOG.error(
                    'Auto-run protocol error: %s', err,
                    exc_info=True,
                )
            finally:
                sm.protocol_done.set()
                LOG.info(
                    'protocol_done set -- '
                    'state machine can advance',
                )

    def _create_reader(self, use_mock: bool) -> Any:
        '''Create the optical reader interface.

        Args:
            use_mock: True to use ReaderMock.

        Returns:
            ReaderInterface or ReaderMock instance, or None
            if connection fails.
        '''
        reader_cfg = self.config.get('reader', {})
        if use_mock:
            from ultra.hw.reader_mock import ReaderMock
            reader = ReaderMock()
            reader.connect()
            LOG.info('Using ReaderMock (no reader hw)')
            return reader

        from ultra.hw.reader_interface import (
            ReaderInterface,
        )
        port = reader_cfg.get('port', 'auto')
        try:
            reader = ReaderInterface(port=port)
            if reader.connect():
                LOG.info('Reader connected: %s', port)
                return reader
            LOG.warning('Reader connect failed: %s', port)
        except Exception as exc:
            LOG.warning('Reader unavailable: %s', exc)
        return None

    def get_runner(self):
        '''Get or create the protocol runner.

        Returns:
            ProtocolRunner instance.
        '''
        if self._runner is None:
            from ultra.protocol.runner import (
                ProtocolRunner,
            )
            stm32 = self._stm32
            if stm32 is None:
                from ultra.hw.stm32_interface import (
                    STM32Interface,
                )
                stm32_cfg = self.config.get('stm32', {})
                stm32 = STM32Interface(
                    port=stm32_cfg.get(
                        'port', '/dev/ttyAMA3',
                    ),
                    baud=stm32_cfg.get('baud', 921600),
                )

            use_mock = os.environ.get(
                'ULTRA_MOCK', '',
            ).lower() in ('1', 'true', 'yes')
            self._reader = self._create_reader(use_mock)

            acquisition = None
            pipeline = None
            if self._reader is not None:
                from ultra.reader.acquisition import (
                    AcquisitionService,
                )
                from ultra.reader.pipeline import (
                    ReaderPipeline,
                )
                acquisition = AcquisitionService(
                    reader=self._reader,
                    event_bus=self.event_bus,
                )
                pipeline = ReaderPipeline(
                    self.event_bus,
                    config=self.config,
                )
                LOG.info(
                    'Reader pipeline ready '
                    '(acquisition + peak detection)',
                )
            else:
                LOG.warning(
                    'No optical reader detected -- '
                    'peak shift chart will be empty. '
                    'Set ULTRA_MOCK=1 for simulated '
                    'reader data.',
                )

            self._runner = ProtocolRunner(
                stm32=stm32,
                event_bus=self.event_bus,
                config=self.config,
                acquisition=acquisition,
                pipeline=pipeline,
            )
        return self._runner

    async def shutdown(self) -> None:
        '''Clean shutdown of all services.'''
        LOG.info('Shutting down...')
        if self._egress_task:
            self._egress_task.cancel()
            try:
                await self._egress_task
            except asyncio.CancelledError:
                pass
        if self._state_machine:
            self._state_machine.stop()
        if self._sm_trigger_task:
            self._sm_trigger_task.cancel()
            try:
                await self._sm_trigger_task
            except asyncio.CancelledError:
                pass
        if self._sm_task:
            self._sm_task.cancel()
            try:
                await self._sm_task
            except asyncio.CancelledError:
                pass
        if self._monitor:
            self._monitor.stop()
        if self._reader:
            try:
                self._reader.disconnect()
            except Exception:
                pass
        if self._stm32:
            self._stm32.disconnect()
        LOG.info('Shutdown complete')


def main() -> None:
    '''Application entry point.'''
    setup_logging()
    config = load_config()
    LOG.info('Ultra RPi starting...')

    app = Application(config)
    try:
        asyncio.run(app.start())
    except KeyboardInterrupt:
        LOG.info('Interrupted')


if __name__ == '__main__':
    main()


def __getattr__(name: str) -> Any:
    if name == '__path__':
        raise AttributeError(name)
    raise AttributeError(
        f'module {__name__!r} has no attribute {name!r}',
    )
