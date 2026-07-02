from dataclasses import dataclass


@dataclass(frozen=True)
class Subscription:
    """
    An opaque receipt returned by ``EventBus.subscribe``.
    """

    id: int
    event_type: str
