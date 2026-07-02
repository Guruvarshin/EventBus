import logging
import threading
from collections import namedtuple
from typing import Any, Callable

from .exceptions import BusClosedError
from .subscription import Subscription

_logger = logging.getLogger(__name__)

_Registration = namedtuple("_Registration", ["id", "handler"])


class EventBus:
    """A thread-safe, in-process publish/subscribe event bus.

    Handlers are invoked synchronously in the publishing thread. The bus is
    safe for concurrent use by any number of publishers and subscribers.
    """

    def __init__(self) -> None:
        self._subscribers: dict[str, tuple[_Registration, ...]] = {}
        self._next_id = 0
        self._closed = False
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._inflight = 0

    def subscribe(self, event_type: str, handler: Callable[[Any], None]) -> Subscription:
        """Register ``handler`` for ``event_type`` and return a Subscription.

        The same handler may be subscribed multiple times; each call produces an
        independent subscription. Raises ``BusClosedError`` if the bus is closed.
        """
        with self._lock:
            if self._closed:
                raise BusClosedError("cannot subscribe on a closed bus")
            self._next_id += 1
            sub_id = self._next_id
            existing = self._subscribers.get(event_type, ())
            self._subscribers[event_type] = existing + (_Registration(sub_id, handler),)
        return Subscription(id=sub_id, event_type=event_type)

    def unsubscribe(self, subscription: Subscription) -> bool:
        """Remove a subscription; return True if one was removed, else False.

        Idempotent and safe to call at any time, including after shutdown.
        """
        with self._lock:
            existing = self._subscribers.get(subscription.event_type, ())
            remaining = tuple(reg for reg in existing if reg.id != subscription.id)
            if len(remaining) == len(existing):
                return False
            if remaining:
                self._subscribers[subscription.event_type] = remaining
            else:
                self._subscribers.pop(subscription.event_type, None)
            return True

    def publish(self, event_type: str, event: Any) -> None:
        """Invoke every handler for ``event_type`` in subscription order.

        Runs synchronously and returns once all handlers have completed. A
        handler exception is logged and neither stops the other handlers nor
        propagates to the caller. Raises ``BusClosedError`` if the bus is closed.
        """
        with self._lock:
            if self._closed:
                raise BusClosedError("cannot publish on a closed bus")
            handlers = self._subscribers.get(event_type, ())
            self._inflight += 1
        try:
            for reg in handlers:
                try:
                    reg.handler(event)
                except Exception:
                    _logger.exception(
                        "event handler %r for %r raised", reg.handler, event_type
                    )
        finally:
            with self._lock:
                self._inflight -= 1
                if self._inflight == 0:
                    self._cond.notify_all()

    def shutdown(self, timeout: float | None = None) -> bool:
        """Close the bus and wait for in-progress publishes to finish.

        Rejects new publish/subscribe calls with ``BusClosedError``. Returns
        True if the bus drained, or False if ``timeout`` elapsed with work still
        running. Idempotent; the bus stays closed permanently.
        """
        with self._cond:
            self._closed = True
            self._subscribers = {}
            return self._cond.wait_for(lambda: self._inflight == 0, timeout)
