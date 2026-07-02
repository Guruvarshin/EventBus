import threading
from collections import namedtuple
from typing import Any, Callable

from .exceptions import BusClosedError
from .subscription import Subscription

_Registration = namedtuple("_Registration", ["id", "handler"])


class EventBus:
    def __init__(self) -> None:
        self._subscribers: dict[str, tuple[_Registration, ...]] = {}
        self._next_id = 0
        self._closed = False
        self._lock = threading.Lock()
        
    def subscribe(self, event_type: str, handler: Callable[[Any], None]) -> Subscription:
        with self._lock:
            if self._closed:
                raise BusClosedError("cannot subscribe on a closed bus")
            self._next_id += 1
            sub_id = self._next_id
            existing = self._subscribers.get(event_type, ())
            self._subscribers[event_type] = existing + (_Registration(sub_id, handler),)
        return Subscription(id=sub_id, event_type=event_type)
    
    def unsubscribe(self, subscription: Subscription) -> bool:
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