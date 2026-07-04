"""Publishers AND subscribers running fully concurrently at 10,000 scale.

Test A: 10,000 subscribers registered, then 10,000 concurrent publishers, so
        every publish fans out to all 10,000 handlers (deterministic count).
Test B: 10,000 subscribers AND 10,000 publishers launched at the SAME instant,
        interleaved, to stress subscribe-while-publishing (copy-on-write path).
"""

import itertools
import os
import random
import sys
import threading
import time

from eventbus import EventBus

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "concurrent_full_results.txt")
_lines = []


def log(m=""):
    print(m)
    _lines.append(m)


def start_all(threads):
    created = 0
    for t in threads:
        try:
            t.start()
            created += 1
        except (RuntimeError, OSError) as ex:
            log(f"      (OS refused more threads at {created:,}: {ex!r})")
            break
    return created


def test_a(n_sub, n_pub):
    bus = EventBus()
    c = itertools.count()
    inc = c.__next__
    for _ in range(n_sub):
        bus.subscribe("e", lambda _e: inc())

    gate = threading.Event()
    errors = []

    def pub():
        gate.wait()
        try:
            bus.publish("e", 1)
        except Exception as ex:
            errors.append(repr(ex))

    threads = [threading.Thread(target=pub) for _ in range(n_pub)]
    created = start_all(threads)
    t0 = time.perf_counter()
    gate.set()
    for t in threads[:created]:
        t.join()
    el = time.perf_counter() - t0
    bus.shutdown()
    delivered = next(c)
    expected = created * n_sub
    log(f"  [A] {created:,} concurrent publishers, each publish -> {n_sub:,} handlers")
    log(f"      crash / exceptions : {len(errors)}  ({'CRASH' if errors else 'NO CRASH'})")
    log(f"      deliveries         : {delivered:,}  (expected {expected:,})  match={delivered == expected}")
    log(f"      wall time          : {el:.3f}s  ({delivered/el:,.0f} deliveries/s)")


def test_b(n_sub, n_pub):
    bus = EventBus()
    c = itertools.count()
    inc = c.__next__
    gate = threading.Event()
    errors = []

    def sub():
        gate.wait()
        try:
            bus.subscribe("e", lambda _e: inc())
        except Exception as ex:
            errors.append(repr(ex))

    def pub():
        gate.wait()
        try:
            bus.publish("e", 1)
        except Exception as ex:
            errors.append(repr(ex))

    threads = [threading.Thread(target=sub) for _ in range(n_sub)]
    threads += [threading.Thread(target=pub) for _ in range(n_pub)]
    random.shuffle(threads)  # interleave subscribe/publish
    created = start_all(threads)
    t0 = time.perf_counter()
    gate.set()
    for t in threads[:created]:
        t.join()
    el = time.perf_counter() - t0
    bus.shutdown()
    delivered = next(c)
    log(f"  [B] {n_sub:,} subscribers + {n_pub:,} publishers started SIMULTANEOUSLY ({created:,} threads)")
    log(f"      crash / exceptions : {len(errors)}  ({'CRASH' if errors else 'NO CRASH'})")
    log(f"      deliveries         : {delivered:,}  (non-deterministic: subs register while publishing)")
    log(f"      wall time          : {el:.3f}s")


def main():
    threading.stack_size(256 * 1024)
    log("FULLY CONCURRENT PUBLISHERS + SUBSCRIBERS @ 10,000")
    log("=" * 60)
    log(f"python : {sys.version.split()[0]}")
    log("")
    log("[A] steady state: each publish fans out to all 10,000 handlers")
    test_a(10_000, 10_000)
    log("")
    log("[B] subscribe and publish happening at the same time")
    test_b(10_000, 10_000)
    log("")
    log("DONE")
    with open(OUT, "w", encoding="utf-8") as f:
        f.write("\n".join(_lines) + "\n")
    print(f"\nwritten: {OUT}")


if __name__ == "__main__":
    main()
