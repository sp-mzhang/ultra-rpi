'''ultra.events -- Lightweight async event bus.

Replaces Qt signals, ZMQ pubsub, and multiprocessing Events
from sway with a single-process async-first event bus.

Usage::

    bus = EventBus()
    bus.on('peak_data', my_handler)
    await bus.emit('peak_data', {'channel': 3, 'shift_pm': -12.4})
'''
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any, Callable, Coroutine

LOG = logging.getLogger(__name__)

EventHandler = Callable[..., Coroutine[Any, Any, None]]
SyncHandler = Callable[..., None]


class EventBus:
    '''Async event bus for intra-process communication.

    Handlers can be either coroutines (awaited) or plain
    callables (called synchronously). All handlers for a
    given event run concurrently via asyncio.gather.

    Attributes:
        _handlers: Mapping of event name to list of handlers.
        _loop: Cached reference to the running event loop.
    '''

    def __init__(self) -> None:
        self._handlers: dict[
            str, list[EventHandler | SyncHandler]
        ] = defaultdict(list)
        self._loop: asyncio.AbstractEventLoop | None = None

    def on(
            self,
            event: str,
            handler: EventHandler | SyncHandler,
    ) -> None:
        '''Register a handler for an event.

        Args:
            event: Event name string.
            handler: Async or sync callable accepting a
                single dict argument.
        '''
        self._handlers[event].append(handler)

    def off(
            self,
            event: str,
            handler: EventHandler | SyncHandler,
    ) -> None:
        '''Unregister a handler for an event.

        Args:
            event: Event name string.
            handler: Previously registered handler.
        '''
        try:
            self._handlers[event].remove(handler)
        except ValueError:
            pass

    async def emit(
            self,
            event: str,
            data: dict[str, Any] | None = None,
    ) -> None:
        '''Emit an event, calling all registered handlers.

        Async handlers are awaited concurrently. Sync handlers
        are called directly. Exceptions in individual handlers
        are logged but do not prevent other handlers from
        running.

        Args:
            event: Event name string.
            data: Event payload dict (default empty).
        '''
        payload = data or {}
        handlers = self._handlers.get(event, [])
        if not handlers:
            return

        tasks = []
        for handler in handlers:
            if asyncio.iscoroutinefunction(handler):
                tasks.append(
                    self._safe_call_async(
                        handler, event, payload,
                    ),
                )
            else:
                self._safe_call_sync(
                    handler, event, payload,
                )

        if tasks:
            await asyncio.gather(*tasks)

    def emit_sync(
            self,
            event: str,
            data: dict[str, Any] | None = None,
    ) -> None:
        '''Thread-safe emit from a non-async context.

        Schedules the async emit on the event loop via
        call_soon_threadsafe. Use this from background reader
        threads (e.g. STM32StatusMonitor).

        Args:
            event: Event name string.
            data: Event payload dict.
        '''
        loop = self._loop or _get_running_loop()
        if loop is None:
            LOG.warning(
                'emit_sync: no event loop available '
                f'for event {event}',
            )
            return
        self._loop = loop
        loop.call_soon_threadsafe(
            asyncio.ensure_future,
            self.emit(event, data),
        )

    async def _safe_call_async(
            self,
            handler: EventHandler,
            event: str,
            data: dict[str, Any],
    ) -> None:
        '''Call an async handler with error isolation.'''
        try:
            await handler(data)
        except Exception:
            LOG.exception(
                f'Error in async handler for '
                f'event "{event}"',
            )

    def _safe_call_sync(
            self,
            handler: SyncHandler,
            event: str,
            data: dict[str, Any],
    ) -> None:
        '''Call a sync handler with error isolation.'''
        try:
            handler(data)
        except Exception:
            LOG.exception(
                f'Error in sync handler for '
                f'event "{event}"',
            )


def _get_running_loop() -> (
    asyncio.AbstractEventLoop | None
):
    '''Get the running event loop or None.'''
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        return None
