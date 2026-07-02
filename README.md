# eventbus

A small, thread-safe, in-process event bus for Python. It lets independent
parts of a program communicate by publishing and subscribing to named events,
without holding direct references to one another.

The library is intentionally minimal: it has **no runtime dependencies** (pure
standard library), a four-method API, and a set of guarantees that are simple
enough to state precisely and reason about.

```python
from eventbus import EventBus

bus = EventBus()

def on_order_placed(event):
    print("order placed:", event)

sub = bus.subscribe("order.placed", on_order_placed)
bus.publish("order.placed", {"order_id": 42})   # -> prints: order placed: {'order_id': 42}
bus.unsubscribe(sub)
bus.shutdown()
```

---

## Contents

- [Setup](#setup)
- [Usage](#usage)
- [API reference](#api-reference)
- [Design overview](#design-overview)
- [Concurrency model](#concurrency-model)
- [Guarantees](#guarantees)
- [Known limitations and tradeoffs](#known-limitations-and-tradeoffs)
- [Testing](#testing)

---

## Setup

Requires **Python 3.11+**. The package uses a `src/` layout and installs with
any PEP 517 build frontend.

```bash
# create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate        # Windows: .\.venv\Scripts\Activate.ps1

# install the library plus test dependencies (editable)
pip install -e ".[dev]"
```

The library itself pulls in nothing beyond the standard library. The optional
`dev` extra installs the test tooling (`pytest`, `pytest-repeat`) only.

---

## Usage

### Subscribe and publish

A handler is any callable that accepts a single argument, the event payload.
The payload can be any object.

```python
from eventbus import EventBus

bus = EventBus()

def send_email(event):
    print(f"emailing customer for order {event['order_id']}")

def update_inventory(event):
    print(f"decrementing stock for order {event['order_id']}")

bus.subscribe("order.placed", send_email)
bus.subscribe("order.placed", update_inventory)

bus.publish("order.placed", {"order_id": 1001})
```

`publish` invokes every handler registered for the event type, in the order the
handlers were subscribed, and returns only once they have all run.

### Unsubscribe

`subscribe` returns a `Subscription` receipt. Pass it back to `unsubscribe` to
stop receiving events. `unsubscribe` returns `True` if it removed a
registration and `False` if there was nothing to remove (for example, if it was
already unsubscribed), so it is safe to call more than once.

```python
sub = bus.subscribe("order.placed", send_email)
...
removed = bus.unsubscribe(sub)   # True the first time, False afterwards
```

The same function may be subscribed multiple times; each subscription is
independent and is identified by its own receipt, not by the function object.

### Publishing to an event with no subscribers

Publishing an event type that nobody listens to is a no-op, not an error:

```python
bus.publish("nobody.is.listening", 123)   # returns normally, does nothing
```

### Handler errors are isolated

If a handler raises, the exception is logged and the remaining handlers still
run. The exception never propagates back to the publisher, so one misbehaving
subscriber cannot break an unrelated publisher or starve the other handlers.

```python
import logging
logging.basicConfig(level=logging.ERROR)   # see handler errors, if any

def flaky(event):
    raise ValueError("boom")

def reliable(event):
    print("still delivered")

bus.subscribe("thing.happened", flaky)
bus.subscribe("thing.happened", reliable)
bus.publish("thing.happened", None)
# logs the ValueError with a traceback, then prints "still delivered"
```

Errors are reported through the standard `logging` module under the logger name
`eventbus.bus`. The library never prints to stdout/stderr directly; the
application decides how (and whether) to surface these logs.

### Graceful shutdown

`shutdown` closes the bus: it rejects any new `publish`/`subscribe` calls with
`BusClosedError`, then waits for publishes that are already in progress to
finish. It returns `True` if everything drained, or `False` if the optional
timeout elapsed while work was still running.

```python
drained = bus.shutdown(timeout=5.0)
if not drained:
    print("some handlers were still running after 5s")
```

After shutdown, the bus stays closed permanently:

```python
bus.publish("order.placed", {})   # raises BusClosedError
```

---

## API reference

```python
class EventBus:
    def subscribe(self, event_type: str, handler: Callable[[Any], None]) -> Subscription
    def publish(self, event_type: str, event: Any) -> None
    def unsubscribe(self, subscription: Subscription) -> bool
    def shutdown(self, timeout: float | None = None) -> bool
```

| Method | Returns | Raises | Notes |
|--------|---------|--------|-------|
| `subscribe` | `Subscription` receipt | `BusClosedError` if closed | Same handler may be subscribed many times. |
| `publish` | `None` | `BusClosedError` if closed | Runs all handlers synchronously, in subscription order. |
| `unsubscribe` | `bool` (removed or not) | — | Idempotent; never raises for state reasons. |
| `shutdown` | `bool` (drained or timed out) | — | Idempotent; permanently closes the bus. |

Supporting types (all importable from the top-level `eventbus` package):

- `Subscription` — an opaque, immutable receipt returned by `subscribe`. Treat
  it as a token; do not depend on its internal fields.
- `EventBusError` — base class for every exception the library raises.
- `BusClosedError` — subclass of `EventBusError`, raised on `publish`/`subscribe`
  after shutdown.

### Notes on the interface

This is a Python-native adaptation of a common event-bus shape (the kind often
written in Go with error return values and a `context.Context` for shutdown).
Two deliberate adaptations were made to fit Python idioms:

- **Errors are raised, not returned.** Misuse (publishing or subscribing on a
  closed bus) raises `BusClosedError` rather than returning an error value,
  because exceptions cannot be silently ignored and keep the normal return
  types clean. Return values are reserved for *facts* the caller may want to
  act on (whether `unsubscribe` removed something, whether `shutdown` drained
  in time), which is why those two methods return `bool` instead of raising.
- **Shutdown takes a `timeout: float`, not a cancellation object.** A single
  timeout captures the one capability shutdown actually needs — a deadline —
  and matches the rest of the standard library (`Thread.join`, `Event.wait`,
  `Lock.acquire`, `Condition.wait`).

---

## Design overview

The bus keeps a single mapping from event type to the handlers registered for
it:

```
_subscribers : dict[str, tuple[_Registration, ...]]
```

Each event type maps to an **immutable tuple** of registrations, where a
registration is an internal `(id, handler)` pair. The `id` is a monotonically
increasing integer handed out per subscription; it makes each subscription
uniquely identifiable even when the same handler is registered more than once.

The design is shaped by the expected workload:

- publishing is frequent,
- subscription changes are comparatively rare,
- publishers and subscribers may operate concurrently.

That is a read-heavy access pattern, so the read path (publish) is kept as cheap
and contention-free as possible, while the write paths (subscribe/unsubscribe)
carry the cost of keeping the structure consistent.

**Dispatch is synchronous**: `publish` runs handlers directly, in the caller's
own thread, and returns once they have all completed. This is the same model
used by Python's `logging` handlers and by signal libraries such as `blinker`.
It was chosen because it yields the clearest guarantees (when `publish` returns,
delivery is done), the simplest failure and shutdown semantics, and the
smallest amount of concurrency machinery to get right. Concurrency between
*different* publishers is preserved — each runs on its own thread and they do
not serialize on one another — while the delivery for a single publish stays
simple and ordered.

**Writes use copy-on-write.** Subscribing or unsubscribing does not mutate an
existing tuple; it builds a new tuple with the change applied and swaps it into
the dictionary. This is what makes lock-free iteration safe (see below) and is
the reason an in-flight publish is never disturbed by a concurrent subscription
change.

---

## Concurrency model

All shared state — the subscriber map, the id counter, the in-flight counter,
and the closed flag — is guarded by a single `threading.Lock`. A
`threading.Condition` built on that same lock coordinates shutdown.

The key idea is that **the lock is never held while user code runs.**

**Publish (the hot path).** Under the lock, `publish` checks that the bus is
open, takes a reference to the current handler tuple, and increments the
in-flight counter — then releases the lock. It iterates that tuple and calls the
handlers with **no lock held**. Because the tuple is immutable and was captured
under the lock, it cannot change underneath the iteration even if another thread
subscribes or unsubscribes concurrently. Holding the lock only for the brief
snapshot (not for the handler calls) is what lets multiple publishers run their
handlers at the same time and prevents a slow handler from blocking the whole
bus.

**Subscribe / unsubscribe (the cold path).** These take the lock, build a new
tuple for the affected event type (copy-on-write), swap it in, and release. They
are serialized against each other and against the publish snapshot, but since
they are rare their cost is unimportant.

**Why a plain lock and not a reader/writer lock.** A read-heavy structure is the
textbook case for a reader/writer lock, but Python's standard library does not
provide one. Rather than add a dependency or hand-roll one (a common source of
subtle bugs such as writer starvation), the library gets the read-heavy benefit
a different way: copy-on-write plus immutable snapshots make reads effectively
uncontended in practice, because the lock is held only for a dictionary lookup
and a reference grab. This keeps correctness easy to reason about without relying
on interpreter-specific behavior.

**Shutdown.** `shutdown` acquires the lock, sets the closed flag (so subsequent
publishes/subscribes are rejected), drops the subscriber references, and then
waits on the condition variable until the in-flight counter reaches zero or the
timeout expires. The correctness of the drain rests on one invariant: the
closed-flag check and the in-flight increment in `publish` happen in the *same*
critical section that `shutdown` uses to set the flag and read the counter.
Therefore either shutdown observes an in-progress publish and waits for it, or
the publish observes the closed flag and is rejected before it ever counts. A
publish can never begin dispatching after shutdown has started draining.

---

## Guarantees

- **Thread safety.** All four operations are safe to call concurrently from any
  number of threads.
- **Synchronous delivery.** When `publish` returns, every handler that was
  registered at snapshot time has finished executing.
- **Ordering.** For a single `publish`, handlers are invoked in the order they
  were subscribed. For a single publisher, events reach a handler in the order
  they were published.
- **At-most-once per registration.** Each registration is invoked exactly once
  per matching publish. Subscribing the same handler twice creates two
  registrations and therefore two invocations.
- **Handler isolation.** An exception raised by one handler is logged and does
  not stop the other handlers or propagate to the publisher.
- **Unsubscribe visibility.** After `unsubscribe` returns, the subscription will
  not receive events from any publish that *begins* afterward. A publish already
  in progress may still deliver to it (see limitations).
- **Shutdown.** Once `shutdown` is called, new `publish`/`subscribe` calls raise
  `BusClosedError`. In-progress publishes are allowed to complete, bounded by the
  timeout. `shutdown` reports whether the bus drained fully.

---

## Known limitations and tradeoffs

- **A slow handler blocks its publisher.** Because dispatch is synchronous, a
  handler that blocks (for example on slow I/O) holds up the thread that called
  `publish`. Other publishers are unaffected, but that one publisher waits. If
  publish latency for I/O-heavy handlers matters, the natural extension is an
  asynchronous dispatch mode backed by a bounded thread pool, which would trade
  the strong "delivered when publish returns" guarantee for decoupling. (In
  CPython the GIL means such a pool would not parallelize CPU-bound handlers
  anyway; its value is decoupling from blocking I/O.)
- **Handlers cannot be forcibly stopped.** Python provides no safe way to kill a
  running thread, so `shutdown`'s timeout bounds how long it *waits*, not how
  long a handler may run. If the timeout expires, `shutdown` returns `False` and
  any still-running handlers continue to completion.
- **Delivery may occur during a concurrent unsubscribe.** A publish that has
  already taken its snapshot will call handlers that were unsubscribed after the
  snapshot. Handlers should therefore tolerate being invoked once more after
  their unsubscribe returns (for example, avoid freeing a resource on
  unsubscribe and then assuming the handler can never touch it again).
- **`unsubscribe`'s boolean is a point-in-time fact.** In concurrent use the
  returned value describes the state at the moment of the call; another thread
  may change things immediately afterward.
- **Exact event-type matching only.** Event types are matched by exact string
  equality; there is no wildcard or topic-pattern matching.
- **No buffering, persistence, or replay.** The bus delivers to whoever is
  subscribed at publish time and then forgets the event. There is no queue,
  retry, dead-lettering, or history. This is a deliberate scope choice: it is an
  in-process notification mechanism, not a message broker.

---

## Testing

Run the full suite:

```bash
python -m pytest
```

(Invoking pytest as `python -m pytest` runs the copy installed in the active
virtual environment and avoids depending on a launcher script being on `PATH`.)

The suite has two parts:

- **Unit tests** (`tests/test_bus.py`, `tests/test_subscription.py`) cover the
  behavior of each operation, including error isolation, idempotent
  unsubscribe, and shutdown draining and timeout.
- **Concurrency stress tests** (`tests/test_concurrency.py`) drive the bus from
  many threads at once — concurrent publishers, subscribe/unsubscribe churn
  during publishing, and shutdown racing live publishers — and assert
  invariants that can only hold if the locking is correct.

Because race conditions are intermittent, the stress tests are most meaningful
when repeated. With `pytest-repeat` installed they can be run many times over,
each with different thread interleavings:

```bash
python -m pytest tests/test_concurrency.py --count=200
```
