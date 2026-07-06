# Synchronous vs Confined (lock-free) — benchmark results

Two implementations of the same interface, measured head-to-head:

- **`EventBus`** — synchronous in-line dispatch; one `Lock` + copy-on-write.
- **`ConfinedEventBus`** — a queue + a single dedicated dispatcher thread that
  **exclusively owns the registry**, so there is **no application lock at all**
  (thread confinement / "share memory by communicating"). `publish` enqueues and
  returns; the dispatcher delivers in FIFO order.

**Machine:** Windows 11, Python 3.14.6. **Tooling:** pure Python stdlib.
Reproduce: `python benchmarks/bench_compare.py` (writes `comparison_results.txt`).

---

## Head-to-head

**1. Publish latency vs fan-out (p50)**

| Fan-out (handlers) | Synchronous (runs handlers) | Confined (enqueue) |
|---|---|---|
| 1 | 0.60 µs | 0.60 µs |
| 100 | 4.00 µs | 1.40 µs |
| **10,000** | **329.4 µs** | **0.60 µs** (~549x faster) |

**2. Throughput (fan-out 1)**

| | publish/s |
|---|---|
| Synchronous (runs handler inline) | **1,634,126** |
| Confined (enqueue only) | 808,453 |

**3. End-to-end delivery latency, empty queue (p50 / p99)**

| | latency |
|---|---|
| Synchronous (delivered before publish returns) | **7.1 / 59.3 µs** |
| Confined (queue handoff to dispatcher) | 53.8 / 544.7 µs |

**4. Correctness — 8 publishers x 5000 events x 5 handlers**

| | delivered / expected |
|---|---|
| Synchronous | 200,000 / 200,000 (match) |
| Confined | 200,000 / 200,000 (match, drained) |

**5. Handler hangs forever — is the publisher blocked?**

| | result |
|---|---|
| Synchronous | publish **BLOCKED forever** (did not return within 1s) |
| Confined | publish still returns in **p50 0.60 µs — NOT blocked** |

---

## Where Confined is better than Synchronous

- **Publish latency is O(1) and flat (~0.6 µs) regardless of fan-out**, vs O(fan-out)
  for synchronous — at 10,000 handlers, ~549x faster, because publish only
  enqueues instead of running every handler inline.
- **A slow or hanging handler never blocks the publisher** — the exact failure of
  synchronous dispatch. This is the headline win.
- **No application lock** — the registry is owned by one thread, so there is no
  lock to reason about, no copy-on-write, and no lock contention.

## Where Synchronous is still better

- **Higher throughput for fast handlers** (~1.63 M/s vs ~0.81 M/s) — the queue and
  the dispatcher hop add overhead that inline dispatch avoids.
- **Lower end-to-end delivery latency** (~7 µs vs ~54 µs) — synchronous delivers
  before `publish` returns; confined pays a queue handoff.
- **Stronger guarantees:** delivered-when-publish-returns, and **synchronous
  subscribe/unsubscribe** with a boolean result. In the confined bus,
  subscribe/unsubscribe are **asynchronous** (queued), so `unsubscribe` returns
  `None` and a subscription is not visible to other threads until processed.

---

## Takeaway

Both are correct and thread-safe (both delivered all 200,000 events under 8
concurrent publishers). Choose by requirement:

- **Confined** when publish latency must stay flat under large fan-out, when
  handlers may block or hang and the publisher must not be frozen, or when the
  simplest thread-safety story (single owner, no lock) is worth asynchronous
  subscribe/unsubscribe.
- **Synchronous** when you want delivered-on-return, the highest throughput for
  fast handlers, and synchronous subscribe/unsubscribe semantics.

"No lock" is a slight misnomer: the queue itself is internally locked, so
confinement *relocates* synchronization into the queue rather than removing it —
but it does eliminate the application-level lock and its contention.
