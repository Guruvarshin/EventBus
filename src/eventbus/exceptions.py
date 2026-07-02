class EventBusError(Exception):
    """Base class for all errors raised by this library."""


class BusClosedError(EventBusError):
    """Raised when publishing to or subscribing on a bus that is shut down."""