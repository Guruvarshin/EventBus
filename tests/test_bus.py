import pytest
import logging
from eventbus import BusClosedError, EventBus
import threading
import time


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
    
def test_publish_invokes_handler_with_event():
    bus = EventBus()
    received = []
    bus.subscribe("order.placed", lambda e: received.append(e))
    bus.publish("order.placed", {"id": 42})
    assert received == [{"id": 42}]


def test_publish_with_no_subscribers_is_noop():
    bus = EventBus()
    bus.publish("nobody.listening", 123)  # must not raise


def test_publish_invokes_all_handlers_in_registration_order():
    bus = EventBus()
    calls = []
    bus.subscribe("e", lambda e: calls.append("a"))
    bus.subscribe("e", lambda e: calls.append("b"))
    bus.publish("e", None)
    assert calls == ["a", "b"]


def test_handler_exception_is_isolated_and_logged(caplog):
    bus = EventBus()
    received = []
    bus.subscribe("e", lambda e: (_ for _ in ()).throw(ValueError("boom")))
    bus.subscribe("e", lambda e: received.append(e))
    with caplog.at_level(logging.ERROR):
        bus.publish("e", "payload")   # must NOT raise
    assert received == ["payload"]    # the good handler still ran
    assert "raised" in caplog.text    # the failure was logged


def test_publish_on_closed_bus_raises():
    bus = EventBus()
    bus._closed = True
    with pytest.raises(BusClosedError):
        bus.publish("e", None)
        
def test_shutdown_rejects_new_operations():
    bus = EventBus()
    assert bus.shutdown() is True
    with pytest.raises(BusClosedError):
        bus.publish("e", None)
    with pytest.raises(BusClosedError):
        bus.subscribe("e", lambda e: None)


def test_shutdown_is_idempotent():
    bus = EventBus()
    assert bus.shutdown() is True
    assert bus.shutdown() is True


def test_shutdown_waits_for_inflight_publish():
    bus = EventBus()
    started = threading.Event()
    done = threading.Event()

    def slow(event):
        started.set()
        time.sleep(0.2)
        done.set()

    bus.subscribe("e", slow)
    t = threading.Thread(target=lambda: bus.publish("e", None))
    t.start()
    started.wait(1.0)                    # publish is now mid-dispatch
    assert bus.shutdown(timeout=5.0) is True
    assert done.is_set()                 # shutdown waited for the handler
    t.join()


def test_shutdown_times_out_when_handler_too_slow():
    bus = EventBus()
    started = threading.Event()

    def slow(event):
        started.set()
        time.sleep(1.0)

    bus.subscribe("e", slow)
    t = threading.Thread(target=lambda: bus.publish("e", None))
    t.start()
    started.wait(1.0)
    assert bus.shutdown(timeout=0.1) is False   # handler outlives the timeout
    t.join()