# Production benchmark results

**Machine:** Windows 11 (10.0.26200), Python 3.14.6
**Bus under test:** the synchronous `EventBus` in `src/eventbus/`
**Tooling:** industry-standard libraries only — `pyperf` (latency), `locust`
(load), plus a stdlib scale harness. **No Kafka / k6 / Grafana** — an in-process
library is measured in-process.

---

## 1. Latency — `pyperf` (CPython's official benchmarking library)

`pyperf` calibrates loop counts, warms up, runs 20 worker processes, and removes
outliers. Command:

```
python benchmarks/bench_pyperf.py -o benchmarks/pyperf_results.json
python -m pyperf stats benchmarks/pyperf_results.json
```

| Fan-out (handlers) | Mean ± stddev | Median | p95 | Max |
|---|---|---|---|---|
| 1   | **601 ns ± 9 ns** | 600 ns | 617 ns | 640 ns |
| 10  | 938 ns ± 8 ns | – | – | – |
| 100 | 3.87 µs ± 0.06 µs | – | – | – |

**Reading:** publish is ~600 ns at 1 handler and scales linearly with fan-out
(the documented `O(fan-out)` synchronous cost). The distribution is extremely
tight (max only +7% over the mean), i.e. very predictable per-call latency.

---

## 2. Load / concurrency — `locust` (industry-standard load framework)

500 concurrent simulated users call `bus.publish` as fast as possible for 15 s.
Command:

```
python -m locust -f benchmarks/locustfile.py --headless -u 500 -r 500 -t 15s --only-summary
```

| Metric | 500 users | 10,000 users |
|---|---|---|
| Command | `-u 500 -r 500 -t 15s` | `-u 10000 -r 2000 -t 20s` |
| Total requests | 772,532 | 526,065 |
| **Failures** | **0 (0.00%)** | **0 (0.00%)** |
| Throughput | 57,039 req/s | 26,114 req/s |
| Latency p50 / p95 / p99 | 9 / 10 / 10 ms | 400 / 420 / 460 ms |
| Latency p99.9 / max | 19 / 180 ms | 470 / 910 ms |

**Important interpretation:** the *real* per-call latency is the `pyperf` number
(~600 ns). The millisecond figures here are dominated by **Locust's own
per-request accounting and gevent greenlet scheduling**, not the bus — Locust is
calibrated for network calls in the millisecond range, so for a sub-microsecond
in-process call its harness overhead dwarfs the operation.

At **10,000 users, throughput *dropped* (57k -> 26k req/s) and latency *rose*
(9 ms -> 400 ms), and Locust logged `CPU usage above 90%`.** That is the classic
signature of the **load generator itself becoming the bottleneck**: 10,000
greenlets plus per-request accounting saturated a single CPU core, so the numbers
measure Locust's ceiling, not the bus. When adding users makes throughput fall
and the generator's CPU maxes out, you are benchmarking the tool, not the system
(the fix is to distribute Locust across cores/machines).

What both runs prove is **robustness under sustained concurrent load**: 500 and
**10,000 concurrent users, hundreds of thousands to ~772 k operations, zero
failures / zero errors**.

Takeaway: use `pyperf` for *latency*, `locust` for *"does it survive concurrent
load without breaking"* — and always check the generator's CPU before trusting
its latency numbers.

---

## 3. Scale — many publishers x many subscribers (stdlib harness)

```
python benchmarks/scale_test.py     # writes scale_10k_results.txt
```

| Publishers (threads) | Subscribers | Deliveries | Result | Wall time |
|---|---|---|---|---|
| 1,000 | 1,000 | 1,000,000 | exact match, 0 lost/dup | 0.10 s |
| **10,000** | **10,000** | **100,000,000** | **exact match, 0 lost/dup** | 7.39 s |

**Reading:** 10,000 concurrent publisher threads delivered to 10,000 subscribers
with all 100,000,000 deliveries accounted for and none lost or duplicated —
proof the locking is correct at scale. The limit on simultaneous publishers is
the OS thread count, not the bus.

---

## 4. Behavioural proofs (from `evaluate.py`)

| Scenario | Result |
|---|---|
| Handler calls `publish` (re-entrant) | **No deadlock**; recursion unwinds, shutdown drains |
| A handler hangs forever | Only its own publisher blocks; `shutdown(timeout=0.5)` returns `False` after ~511 ms; other publishes unaffected |
| Ctrl-C (`KeyboardInterrupt`) in a handler | Propagates out of `publish` (not swallowed); in-flight counter restored by `finally`; bus still shuts down cleanly |

---

## Throughput vs. publisher count (the GIL, measured)

| Publishers | publish/s |
|---|---|
| 1 | 1,709,992 |
| 2 | 2,285,477 |
| 8 | 2,312,886 |
| 16 | 2,593,427 |

Throughput plateaus at ~2.3–2.6 M publish/s regardless of thread count — the
GIL serialises the CPU-bound publish path, so adding threads doesn't add
parallelism. This is the empirical basis for choosing synchronous dispatch: a
thread pool would not parallelise CPU-bound handlers.
