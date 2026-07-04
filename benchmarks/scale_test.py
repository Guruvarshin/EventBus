"""Scale test: many concurrent publisher threads x many subscribers.

Tooling: pure Python standard library (threading, time, itertools) driving the
eventbus library directly. No Kafka, no external load generator -- an in-process
library is measured in-process.
"""

import itertools
import os
import platform
import sys
import threading
import time

from eventbus import EventBus

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scale_10k_results.txt")


def run(requested_pubs: int, n_sub: int, lines: list) -> None:
    bus = EventBus()
    counter = itertools.count()
    inc = counter.__next__  # next() on itertools.count is atomic in CPython

    def handler(_event):
        inc()

    # --- register subscribers (each subscribe is copy-on-write -> O(n) per call)
    t0 = time.perf_counter()
    for _ in range(n_sub):
        bus.subscribe("e", handler)
    subscribe_secs = time.perf_counter() - t0

    # --- create publisher threads, all gated on one Event so they fire together
    start = threading.Event()

    def publisher():
        start.wait()
        bus.publish("e", 1)  # one event each

    threads = []
    create_error = None
    for _ in range(requested_pubs):
        t = threading.Thread(target=publisher)
        try:
            t.start()
        except (RuntimeError, OSError) as exc:  # OS refuses more threads
            create_error = repr(exc)
            break
        threads.append(t)

    created = len(threads)  # actual number of concurrent publisher threads alive

    t0 = time.perf_counter()
    start.set()  # release all publishers simultaneously
    for t in threads:
        t.join()
    publish_secs = time.perf_counter() - t0

    total = bus.shutdown()  # drain (nothing in flight; synchronous)
    delivered = next(counter)  # value == number of handler invocations
    expected = created * n_sub

    lines.append(f"  requested publishers : {requested_pubs:,}")
    lines.append(f"  subscribers          : {n_sub:,}")
    lines.append(f"  concurrent publisher THREADS actually created : {created:,}")
    if create_error:
        lines.append(f"  (OS refused more threads at {created:,}: {create_error})")
    lines.append(f"  subscribe() setup time (O(n^2) copy-on-write) : {subscribe_secs:8.3f} s")
    lines.append(f"  all-publish wall time                         : {publish_secs:8.3f} s")
    lines.append(f"  expected deliveries  : {expected:,}")
    lines.append(f"  actual deliveries    : {delivered:,}")
    lines.append(f"  MATCH (no lost/dup)  : {delivered == expected}")
    if publish_secs > 0:
        lines.append(f"  delivery throughput  : {delivered / publish_secs:,.0f} deliveries/s")
        lines.append(f"  publish throughput   : {created / publish_secs:,.0f} publishes/s")
    lines.append("")


def main() -> None:
    threading.stack_size(512 * 1024)  # smaller stacks -> more threads fit
    lines = []
    lines.append("EVENT BUS SCALE TEST")
    lines.append("=" * 60)
    lines.append(f"machine     : {platform.platform()}")
    lines.append(f"python      : {sys.version.split()[0]}")
    lines.append(f"tooling     : stdlib threading + perf_counter (NO Kafka)")
    lines.append("")

    lines.append("[A] 1,000 publishers x 1,000 subscribers")
    run(1_000, 1_000, lines)

    lines.append("[B] 10,000 publishers x 10,000 subscribers")
    run(10_000, 10_000, lines)

    text = "\n".join(lines)
    print(text)
    with open(OUT, "w", encoding="utf-8") as f:
        f.write(text + "\n")
    print(f"\nResults written to: {OUT}")


if __name__ == "__main__":
    main()
