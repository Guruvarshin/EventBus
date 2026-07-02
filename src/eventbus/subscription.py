from dataclasses import dataclass


@dataclass(frozen=True)
class Subscription:
    """An opaque receipt for one subscription.
    """

    id: int
    event_type: str