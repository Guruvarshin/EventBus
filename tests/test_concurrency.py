import threading
import time

import pytest

from eventbus import BusClosedError, EventBus


def test_concurrent_publishers_deliver_to_all_handlers():
    bus = EventBus()
    count = 0
    count_lock = threading.Lock()

    def handler(event):
        nonlocal count
        with count_lock:
            count += 1

    num_handlers = 5
    for _ in range(num_handlers):
        bus.subscribe("e", handler)

    num_publishers = 8
    events_each = 200
    barrier = threading.Barrier(num_publishers)

    def publisher():
        barrier.wait()
        for i in range(events_each):
            bus.publish("e", i)

    threads = [threading.Thread(target=publisher) for _ in range(num_publishers)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert count == num_publishers * events_each * num_handlers
    
    
def test_concurrent_churn_during_publish_is_safe():
    bus = EventBus()
    stop = threading.Event()
    errors = []

    def publisher():
        try:
            while not stop.is_set():
                bus.publish("e", 1)
        except Exception as exc:  # noqa: BLE001 - test wants to catch anything
            errors.append(exc)

    def churner():
        try:
            while not stop.is_set():
                sub = bus.subscribe("e", lambda e: None)
                bus.unsubscribe(sub)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=publisher) for _ in range(4)]
    threads += [threading.Thread(target=churner) for _ in range(4)]
    for t in threads:
        t.start()
    time.sleep(0.5)
    stop.set()
    for t in threads:
        t.join()

    assert errors == []
    
    
def test_shutdown_during_concurrent_publishing_is_clean():
    bus = EventBus()
    delivered = 0
    delivered_lock = threading.Lock()

    def handler(event):
        nonlocal delivered
        with delivered_lock:
            delivered += 1

    bus.subscribe("e", handler)

    def publisher():
        for i in range(2000):
            try:
                bus.publish("e", i)
            except BusClosedError:
                return  # expected once shutdown flips the gate

    threads = [threading.Thread(target=publisher) for _ in range(4)]
    for t in threads:
        t.start()
    time.sleep(0.05)
    assert bus.shutdown(timeout=5.0) is True  # drains without deadlock
    for t in threads:
        t.join()

    with pytest.raises(BusClosedError):
        bus.publish("e", 1)  # closed for good afterward