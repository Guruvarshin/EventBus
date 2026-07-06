import threading

import pytest

from eventbus import BusClosedError, ConfinedEventBus


def test_confined_delivers_event():
    bus = ConfinedEventBus()
    got = []
    done = threading.Event()
    bus.subscribe("e", lambda x: (got.append(x), done.set()))
    bus.publish("e", 7)
    assert done.wait(2.0)
    assert got == [7]
    bus.shutdown(timeout=2.0)


def test_confined_same_thread_subscribe_then_publish_ordered():
    bus = ConfinedEventBus()
    got = []
    bus.subscribe("e", lambda x: got.append(x))
    for i in range(50):
        bus.publish("e", i)
    assert bus.shutdown(timeout=5.0) is True
    assert got == list(range(50))


def test_confined_unsubscribe_stops_delivery():
    bus = ConfinedEventBus()
    got = []
    sub = bus.subscribe("e", lambda x: got.append(x))
    bus.unsubscribe(sub)
    bus.publish("e", 1)
    bus.shutdown(timeout=2.0)
    assert got == []


def test_confined_publish_after_shutdown_raises():
    bus = ConfinedEventBus()
    bus.shutdown(timeout=2.0)
    with pytest.raises(BusClosedError):
        bus.publish("e", 1)
