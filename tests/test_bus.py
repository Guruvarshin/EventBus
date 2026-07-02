import pytest

from eventbus import BusClosedError, EventBus


def test_bus_constructs_empty():
    bus = EventBus()
    assert bus._subscribers == {}
    assert bus._closed is False
    
    
def test_subscribe_returns_receipt():
    bus = EventBus()
    sub = bus.subscribe("order.placed", lambda e: None)
    assert sub.event_type == "order.placed"
    assert sub.id == 1


def test_subscribe_ids_are_unique_even_for_same_handler():
    bus = EventBus()

    def handler(event):
        pass

    sub1 = bus.subscribe("order.placed", handler)
    sub2 = bus.subscribe("order.placed", handler)
    assert sub1.id != sub2.id


def test_subscribe_on_closed_bus_raises():
    bus = EventBus()
    bus._closed = True  # simulate shutdown until we implement it
    with pytest.raises(BusClosedError):
        bus.subscribe("order.placed", lambda e: None)
        
def test_unsubscribe_removes_handler():
    bus = EventBus()
    sub = bus.subscribe("order.placed", lambda e: None)
    assert bus.unsubscribe(sub) is True
    assert "order.placed" not in bus._subscribers


def test_unsubscribe_returns_false_when_nothing_matched():
    bus = EventBus()
    sub = bus.subscribe("order.placed", lambda e: None)
    bus.unsubscribe(sub)
    assert bus.unsubscribe(sub) is False


def test_unsubscribe_one_of_duplicate_handlers_leaves_the_other():
    bus = EventBus()

    def handler(event):
        pass

    sub1 = bus.subscribe("order.placed", handler)
    sub2 = bus.subscribe("order.placed", handler)
    assert bus.unsubscribe(sub1) is True
    remaining = bus._subscribers["order.placed"]
    assert len(remaining) == 1
    assert remaining[0].id == sub2.id