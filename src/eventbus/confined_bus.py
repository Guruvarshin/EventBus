import itertools
import logging
import queue
import threading
from typing import Any, Callable

from .exceptions import BusClosedError, QueueFullError
from .subscription import Subscription

_logger = logging.getLogger(__name__)

_SUBSCRIBE, _UNSUBSCRIBE, _PUBLISH, _SHUTDOWN = "sub", "unsub", "pub", "stop"


class ConfinedEventBus:
    """Event bus with NO explicit application lock, via thread confinement.

    A single dispatcher thread exclusively owns the subscriber registry. Callers
    never touch shared state: subscribe / unsubscribe / publish / shutdown each
    enqueue a message onto one thread-safe queue, which the dispatcher processes
    in order. Because only the dispatcher reads or writes the registry, no lock
    (and no copy-on-write) is required -- the queue provides all synchronization.

    Trade-off vs the lock-based buses: subscribe and unsubscribe are asynchronous
    (they take effect once the dispatcher processes them), so unsubscribe cannot
    return a boolean, and a subscription is not visible to other threads' publishes
    until it is processed. This is the actor / "share memory by communicating" model.
    """

    def __init__(self, max_queue_size: int = 0) -> None:
        self._queue: queue.Queue = queue.Queue(maxsize=max_queue_size)
        self._ids = itertools.count(1)     # next() is atomic in CPython -> no lock
        self._closed = threading.Event()   # cross-thread flag, needs no lock
        self._dispatcher = threading.Thread(
            target=self._run, name="eventbus-confined", daemon=True
        )
        self._dispatcher.start()

    def subscribe(self, event_type: str, handler: Callable[[Any], None]) -> Subscription:
        if self._closed.is_set():
            raise BusClosedError("cannot subscribe on a closed bus")
        sub_id = next(self._ids)
        self._put((_SUBSCRIBE, sub_id, event_type, handler))
        return Subscription(id=sub_id, event_type=event_type)

    def unsubscribe(self, subscription: Subscription) -> None:
        # asynchronous: effective once the dispatcher processes it
        self._put((_UNSUBSCRIBE, subscription.id, subscription.event_type, None))

    def publish(self, event_type: str, event: Any) -> None:
        if self._closed.is_set():
            raise BusClosedError("cannot publish on a closed bus")
        self._put((_PUBLISH, None, event_type, event))

    def shutdown(self, timeout: float | None = None) -> bool:
        self._closed.set()
        self._queue.put((_SHUTDOWN, None, None, None))  # blocking; drains backlog first
        self._dispatcher.join(timeout)
        return not self._dispatcher.is_alive()

    def _put(self, message: tuple) -> None:
        try:
            self._queue.put_nowait(message)
        except queue.Full:
            raise QueueFullError("event dispatch queue is full") from None

    def _run(self) -> None:
        # registry lives here, confined to this thread -> no lock, no copy-on-write
        subscribers: dict[str, list[tuple[int, Callable[[Any], None]]]] = {}
        while True:
            kind, sub_id, event_type, payload = self._queue.get()
            if kind == _SHUTDOWN:
                return
            if kind == _SUBSCRIBE:
                subscribers.setdefault(event_type, []).append((sub_id, payload))
            elif kind == _UNSUBSCRIBE:
                regs = subscribers.get(event_type)
                if regs:
                    kept = [r for r in regs if r[0] != sub_id]
                    if kept:
                        subscribers[event_type] = kept
                    else:
                        del subscribers[event_type]
            elif kind == _PUBLISH:
                for _id, handler in subscribers.get(event_type, ()):
                    try:
                        handler(payload)
                    except Exception:
                        _logger.exception(
                            "event handler %r for %r raised", handler, event_type
                        )
