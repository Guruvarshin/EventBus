"""Head-to-head benchmark: synchronous EventBus vs lock-free ConfinedEventBus.

  EventBus         - synchronous in-line dispatch, one lock + copy-on-write.
  ConfinedEventBus - queue + a dedicated dispatcher thread that exclusively owns
                     the registry, so there is no application lock at all.

Tooling: pure Python stdlib. Writes comparison_results.txt.
"""

import itertools
import os
import statistics
import sys
import threading
import time

from eventbus import ConfinedEventBus, EventBus

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "comparison_results.txt")
_lines = []


def log(m=""):
    print(m)
    _lines.append(m)


def pctl(s, q):
    return s[min(int(q / 100 * len(s)), len(s) - 1)] / 1000.0  # ns -> us


def publish_latency(make_bus, fanout, confined, n=3000):
    bus = make_bus()
    for _ in range(fanout):
        bus.subscribe("e", lambda _e: None)
    if confined:
        time.sleep(0.3)  # let async subscribe messages be processed
    pc = time.perf_counter_ns
    for _ in range(500):
        bus.publish("e", None)
    lat = []
    for _ in range(n):
        t0 = pc()
        bus.publish("e", None)
        lat.append(pc() - t0)
    bus.shutdown(timeout=90)
    lat.sort()
    return pctl(lat, 50), pctl(lat, 99), statistics.mean(lat) / 1000


def throughput(make_bus, confined, n=500_000):
    bus = make_bus()
    bus.subscribe("e", lambda _e: None)
    if confined:
        time.sleep(0.1)
    t0 = time.perf_counter()
    for i in range(n):
        bus.publish("e", i)
    enq = time.perf_counter() - t0
    bus.shutdown(timeout=120)
    return n / enq


def end_to_end(make_bus, confined, n=2000):
    bus = make_bus()
    deltas = []
    bus.subscribe("e", lambda sent: deltas.append(time.perf_counter_ns() - sent))
    if confined:
        time.sleep(0.1)
    for _ in range(200):
        bus.publish("e", time.perf_counter_ns())
        time.sleep(0.0005)
    deltas.clear()
    for _ in range(n):
        bus.publish("e", time.perf_counter_ns())
        time.sleep(0.0005)
    time.sleep(0.2)
    bus.shutdown(timeout=5)
    deltas.sort()
    return pctl(deltas, 50), pctl(deltas, 99)


def correctness(make_bus, confined, pubs=8, each=5000, handlers=5):
    bus = make_bus()
    c = itertools.count()
    inc = c.__next__
    for _ in range(handlers):
        bus.subscribe("e", lambda _e: inc())
    if confined:
        time.sleep(0.2)
    barrier = threading.Barrier(pubs)

    def w():
        barrier.wait()
        for i in range(each):
            bus.publish("e", i)

    ts = [threading.Thread(target=w) for _ in range(pubs)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    drained = bus.shutdown(timeout=90)
    return next(c), pubs * each * handlers, drained


def sync_hanging_blocks():
    bus = EventBus()
    started = threading.Event()
    bus.subscribe("e", lambda e: (started.set(), time.sleep(10)))
    returned = threading.Event()
    threading.Thread(target=lambda: (bus.publish("e", None), returned.set()), daemon=True).start()
    started.wait(2.0)
    return returned.wait(1.0)  # did publish return within 1s? (False = blocked)


def confined_hanging_latency():
    bus = ConfinedEventBus()
    started = threading.Event()
    bus.subscribe("e", lambda e: (started.set(), time.sleep(3600)))
    time.sleep(0.1)
    bus.publish("e", None)
    started.wait(2.0)
    pc = time.perf_counter_ns
    times = []
    for _ in range(1000):
        t0 = pc()
        bus.publish("e", None)
        times.append(pc() - t0)
    times.sort()
    return pctl(times, 50)


def main():
    log("SYNCHRONOUS vs CONFINED (LOCK-FREE) -- HEAD TO HEAD")
    log("=" * 66)
    log(f"python : {sys.version.split()[0]}")
    log("")

    log("[1] Publish latency vs fan-out (p50 us)")
    log(f"  {'fan-out':>8} | {'SYNC (runs handlers)':>22} | {'CONFINED (enqueue)':>20}")
    for f in (1, 100, 10_000):
        s50, s99, sm = publish_latency(EventBus, f, confined=False)
        c50, c99, cm = publish_latency(ConfinedEventBus, f, confined=True)
        log(f"  {f:>8} | {s50:>22.2f} | {c50:>20.2f}")
    log("")

    log("[2] Enqueue / publish throughput (fan-out 1)")
    log(f"  SYNC     : {throughput(EventBus, False):>12,.0f} publish/s (runs handler inline)")
    log(f"  CONFINED : {throughput(ConfinedEventBus, True):>12,.0f} publish/s (enqueue only)")
    log("")

    log("[3] End-to-end delivery latency, empty queue (p50 / p99 us)")
    s50, s99 = end_to_end(EventBus, False)
    c50, c99 = end_to_end(ConfinedEventBus, True)
    log(f"  SYNC     : {s50:.2f} / {s99:.2f}   (delivered before publish returns)")
    log(f"  CONFINED : {c50:.2f} / {c99:.2f}   (queue handoff to dispatcher)")
    log("")

    log("[4] Correctness: 8 publishers x 5000 events x 5 handlers")
    sd, se, sdr = correctness(EventBus, False)
    cd, ce, cdr = correctness(ConfinedEventBus, True)
    log(f"  SYNC     : delivered {sd:,} / expected {se:,}  match={sd == se}")
    log(f"  CONFINED : delivered {cd:,} / expected {ce:,}  match={cd == ce}  drained={cdr}")
    log("")

    log("[5] Handler hangs forever -- is the publisher blocked?")
    sync_returned = sync_hanging_blocks()
    conf_p50 = confined_hanging_latency()
    log(f"  SYNC     : publish returned within 1s = {sync_returned}  (False = BLOCKED forever)")
    log(f"  CONFINED : publish still returns in p50 {conf_p50:.2f} us  (NOT blocked)")
    log("")

    log("DONE")
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(_lines) + "\n")
    print(f"\nwritten: {OUT}")


if __name__ == "__main__":
    main()
