from dataclasses import dataclass


@dataclass(frozen=True)
class Subscription:
    """An opaque receipt returned by ``EventBus.subscribe``.

    Pass it back to ``EventBus.unsubscribe`` to cancel the subscription. Treat
    it as an opaque token: do not rely on or mutate its fields.
    """

    id: int
    event_type: str