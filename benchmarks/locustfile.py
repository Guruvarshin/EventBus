"""Load test of the in-process EventBus using Locust.

Locust is the industry-standard load-testing framework. It normally drives HTTP,
but a custom `User` can exercise any Python code path -- here each simulated user
repeatedly calls bus.publish and reports the call latency to Locust, which
aggregates RPS and p50/p95/p99 exactly as it would for a web service.

Run headless (avoids the blocked locust.exe launcher):
    python -m locust -f benchmarks/locustfile.py --headless -u 500 -r 500 -t 15s --only-summary
"""

import time

from locust import User, constant, task

from eventbus import EventBus

# One shared bus with a realistic fan-out, created once for the whole test.
bus = EventBus()
for _ in range(10):
    bus.subscribe("e", lambda _e: None)


class BusUser(User):
    wait_time = constant(0)  # hammer as fast as possible

    @task
    def publish(self):
        start = time.perf_counter()
        try:
            bus.publish("e", 1)
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            self.environment.events.request.fire(
                request_type="BUS",
                name="publish",
                response_time=elapsed_ms,
                response_length=0,
                exception=None,
                context={},
            )
        except Exception as exc:  # report failures to Locust
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            self.environment.events.request.fire(
                request_type="BUS",
                name="publish",
                response_time=elapsed_ms,
                response_length=0,
                exception=exc,
                context={},
            )
