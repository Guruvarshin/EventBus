"""Rigorous publish-latency microbenchmark using pyperf.

pyperf is the library behind CPython's official `pyperformance` suite: it
calibrates loop counts, warms up, runs many worker processes, and removes
outliers -- the production-standard way to measure Python latency.

Run:
    python benchmarks/bench_pyperf.py -o benchmarks/pyperf_results.json
    python -m pyperf stats benchmarks/pyperf_results.json
"""

import pyperf

from eventbus import EventBus


def make_bus(fanout: int) -> EventBus:
    bus = EventBus()
    for _ in range(fanout):
        bus.subscribe("e", lambda _e: None)
    return bus


runner = pyperf.Runner()

for _fanout in (1, 10, 100, 1000, 10000):
    _bus = make_bus(_fanout)
    runner.bench_func(f"publish_fanout_{_fanout}", _bus.publish, "e", None)
