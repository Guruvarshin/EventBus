"""Behavioural + latency/throughput evaluation of the synchronous EventBus.

Tooling: pure Python standard library driving the eventbus library directly.
Writes a results file next to this script.
"""

import os
import statistics
import sys
import threading
import time

from eventbus import EventBus

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "evaluate_results.txt")
_lines: list[str] = []


def log(msg: str = "") -> None:
    print(msg)
    _lines.append(msg)


def pctl(sorted_ns, q):
    idx = min(int(q / 100 * len(sorted_ns)), len(sorted_ns) - 1)
    return sorted_ns[idx] / 1000.0  # ns -> us


def latency(fanout, n):
    bus = EventBus()
    for _ in range(fanout):
        bus.subscribe("e", lambda _e: None)
    for _ in range(min(2000, n)):
        bus.publish("e", None)  # warmup
    lat = []
    ap = lat.append
    pc = time.perf_counter_ns
    for _ in range(n):
        t0 = pc()
        bus.publish("e", None)
        ap(pc() - t0)
    bus.shutdown()
    lat.sort()
    log(f"  fanout={fanout:>6} (n={n:>7,}): p50={pctl(lat,50):9.2f}  p95={pctl(lat,95):9.2f}  "
        f"p99={pctl(lat,99):9.2f}  p99.9={pctl(lat,99.9):10.2f}  mean={statistics.mean(lat)/1000:9.2f}  (us)")


def throughput(pubs, events_each=100_000):
    bus = EventBus()
    bus.subscribe("e", lambda _e: None)
    b = threading.Barrier(pubs)

    def w():
        b.wait()
        for i in range(events_each):
            bus.publish("e", i)

    ts = [threading.Thread(target=w) for _ in range(pubs)]
    t0 = time.perf_counter()
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    el = time.perf_counter() - t0
    bus.shutdown()
    total = pubs * events_each
    log(f"  publishers={pubs:>3}: {total:>10,} publishes in {el:6.3f}s -> {total/el:>12,.0f} publish/s")


def reentrant_publish():
    bus = EventBus()
    seen = []

    def outer(e):
        if e < 5:
            bus.publish("e", e + 1)
        seen.append(e)

    bus.subscribe("e", outer)
    t0 = time.perf_counter()
    bus.publish("e", 0)
    drained = bus.shutdown(timeout=2.0)
    log(f"  handler-calls-publish finished in {(time.perf_counter()-t0)*1000:.3f} ms, NO deadlock. "
        f"seen={seen}, shutdown_drained={drained}")


def first_handler_hangs_forever():
    """If the FIRST handler in the list hangs forever, do the others still run,
    and does the publisher thread just keep hanging?"""
    bus = EventBus()
    started = threading.Event()
    order = []

    def hang(e):
        order.append("hang-started")
        started.set()
        while True:            # hang forever
            time.sleep(3600)

    def second(e):
        order.append("second-ran")

    def third(e):
        order.append("third-ran")

    bus.subscribe("e", hang)     # FIRST in the subscriber list
    bus.subscribe("e", second)   # SECOND
    bus.subscribe("e", third)    # THIRD

    pub_thread = threading.Thread(target=lambda: bus.publish("e", None), daemon=True)
    pub_thread.start()
    started.wait(2.0)
    time.sleep(1.0)  # give the other handlers ample time to run if they were going to

    log(f"  order observed          : {order}")
    log(f"  2nd/3rd handlers ran?   : {('second-ran' in order)} / {('third-ran' in order)}  "
        f"(False = STARVED by the hanging first handler)")
    log(f"  publisher thread alive? : {pub_thread.is_alive()}  (True = it just keeps hanging, cannot be killed)")

    # is the rest of the bus still usable while that handler hangs?
    other = []
    bus.subscribe("other", lambda e: other.append(True))
    t0 = time.perf_counter()
    bus.publish("other", None)
    log(f"  a DIFFERENT event still delivered = {bool(other)} in {(time.perf_counter()-t0)*1000:.3f} ms "
        f"(the hang only blocks its OWN publisher)")

    st = time.perf_counter()
    drained = bus.shutdown(timeout=1.0)
    log(f"  shutdown(timeout=1) drained = {drained} after {(time.perf_counter()-st)*1000:.0f} ms "
        f"(False = the hung handler can never be drained; in-flight stays > 0 forever)")


def human_interrupt():
    bus = EventBus()
    ran = []
    bus.subscribe("e", lambda e: (_ for _ in ()).throw(KeyboardInterrupt("simulated Ctrl-C")))
    bus.subscribe("e", lambda e: ran.append(True))
    try:
        bus.publish("e", None)
        log("  UNEXPECTED: KeyboardInterrupt was swallowed")
    except KeyboardInterrupt:
        log(f"  KeyboardInterrupt PROPAGATED out of publish; later handler ran = {bool(ran)} "
            f"(False = remaining handlers starved by the interrupt)")
    log(f"  bus still shuts down cleanly = {bus.shutdown(timeout=1.0)} "
        f"(in-flight counter restored by finally)")


def main():
    log("EVENT BUS EVALUATION")
    log("=" * 70)
    log(f"python : {sys.version.split()[0]}")
    log("")
    log("[1] PUBLISH LATENCY vs fan-out (including large fan-out)")
    latency(1, 100_000)
    latency(10, 100_000)
    latency(100, 100_000)
    latency(1_000, 20_000)
    latency(10_000, 3_000)
    log("")
    log("[2] THROUGHPUT vs concurrent publishers")
    for p in (1, 2, 4, 8, 16):
        throughput(p)
    log("")
    log("[3] DEADLOCK when a handler calls publish?")
    reentrant_publish()
    log("")
    log("[4] FIRST handler in the list HANGS FOREVER -- are the others starved?")
    first_handler_hangs_forever()
    log("")
    log("[5] HUMAN INTERRUPT (Ctrl-C) inside a handler")
    human_interrupt()
    log("")
    log("DONE")

    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(_lines) + "\n")
    print(f"\nResults written to: {OUT}")


if __name__ == "__main__":
    main()
