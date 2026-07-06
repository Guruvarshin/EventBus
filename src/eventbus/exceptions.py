class EventBusError(Exception):
    """Base class for all errors raised by this library."""


class BusClosedError(EventBusError):
    """Raised when publishing to or subscribing on a bus that is shut down."""


class QueueFullError(EventBusError):
    """Raised by ConfinedEventBus.publish when the bounded dispatch queue is full."""