# Real vs simulated

We volunteer this before anyone asks. Being caught at a boundary is far worse than naming it.

This document is written as we build, not reconstructed at the end. It is completed and read
aloud once at the feature freeze.

## Real

- **CPU burned by the quadratic validator.** `auth` v2.4.1 is a genuine second implementation of
  `verifyToken`, not a flag that adds a delay. It runs a nested loop over the token's claims with
  an ambiguous regex whose segment group and inner quantifier can consume the same characters, so
  rejecting an unversioned claim requires trying every possible segmentation. Measured on this
  machine: **4.42 ms mean per call** at `workUnits: 18`, against 0.002 ms for v2.4.0, and **+4.21 ms
  end to end through real HTTP**. The two versions return identical results for every input —
  v2.4.1's pattern accepts a strict subset of what the legacy rule accepts — which is precisely why
  a regression like this passes functional tests in staging.
- **Pool exhaustion, queueing, and queue wait times.** The twelve-permit semaphore and its bounded
  FIFO queue are real concurrency primitives with real handoff semantics: a freed permit is passed
  directly to the head of the queue and never returns to the pool while anyone is waiting, so a
  newly arriving request cannot barge past one that has been waiting. Verified by 49 assertions,
  including 3000 concurrent callers against 12 permits with occupancy sampled every millisecond.
- **Retry amplification**, computed from attempt and call counters rather than sampled events.
- **Every latency, count, queue depth, arrival rate, and utilisation figure.** ρ is computed from
  the arrival rate and the service time that actually elapsed, measured per window.
- **The telemetry spike** — a genuine consequence of retries and error logging, not an animation.

## Simulated, and why

- **Datastore storage is a stub.** There is no disk and no query engine. The pool semaphore and its
  FIFO queue are the object of study, not the storage layer.
- **Datastore service time is a simulated I/O wait**, implemented as an awaited interval while the
  permit is held. See the note below on why this is an interval rather than a CPU burn.
- **Deploy markers are synthetic.** We have no CI.

## Implementation deviations from the build plan

Recorded here because they are departures from the written design, made for measured reasons.

### 1. Outbound HTTP uses `node:http`, not `fetch`

**The plan specified** native `fetch` with `AbortSignal.timeout()` for per-request deadlines,
explicitly ruling out axios and undici as dependencies.

**What we found.** Under sustained request-cancellation churn, Node 24.15.0's `fetch` — which is
undici — terminates the entire process with a native Windows fast-fail, exit code `3221226505`
(`0xC0000409`). No stack trace, no stderr, and no diagnostic report even under
`--report-on-fatalerror`: the process never reaches Node's own fatal-error handler.

This is exactly the traffic our mesh generates. Once the gateway starts timing out against a slow
`auth`, it cancels hundreds of requests per second, and processes died after 100–300 seconds of it.
The victim was always whichever process was cancelling the most requests.

We eliminated the alternatives in order: service logic (17,910 requests at 300 concurrency with no
cancellations — clean), `readline` in the process supervisor (replaced — still crashed), stdio pipes
entirely (children run with no pipes — still crashed), memory (heap oscillates normally, RSS stable),
and ephemeral port exhaustion (~220 sockets in `TIME_WAIT` against a 16,384-port range).

**The isolating measurement.** A forty-line script containing no project code — a slow local HTTP
server, and a client running 64 workers that cancel at a 300 ms deadline — reproduces it:

```
t+ 10s  completed=0 aborted=2003 heap=32MB rss=104MB
CLIENT EXITED after 17s  code=3221226505
```

The identical script issuing the same cancellations through `node:http` with a keep-alive agent
survived 120 seconds and 16,273 cancellations with stable memory and no crash.

**What changed.** `callJson()` in `services/_shared/service.js` is now built on `node:http` with one
shared keep-alive agent per process, and `loadgen` uses the same client. Deadlines, return shapes and
error codes (`UPSTREAM_TIMEOUT`, `UPSTREAM_UNREACHABLE`, HTTP 503) are unchanged. Retry logic,
backoff, jitter, bulkheads, the semaphore, the contracts and every tuning value are untouched.

**Why this honours the original intent.** The plan's constraint was zero HTTP dependencies.
`node:http` is the standard library — one layer *below* `fetch`, which is built on it. We added no
dependency; the backend still has exactly three.

### 2. Datastore service time is an awaited interval, not a synchronous CPU burn

**The plan specified** `acquire → burn serviceTimeMs of REAL CPU → release`, while also specifying a
twelve-permit pool and utilisation `ρ = λW/N`.

**Why both cannot hold.** On a single-threaded runtime a synchronous burn occupies the event loop
for the entire interval, so permits can never overlap: twelve permits behave exactly like one, and
capacity is `1/W = 50 queries/s` rather than `N/W = 600`. Reaching ρ ≈ 1.04 at N=12, W=20 ms would
require 624 queries/s of 20 ms CPU each — 12.5 CPU-seconds per wall-clock second on one thread.

**Measured before the change:** throughput flat at 44–60 queries/s from 1 to 48 concurrent callers,
queue wait constantly zero, the semaphore never queuing a single waiter. The pool was inert.

**Measured after:** throughput scales linearly to exactly 12 concurrent (32 → 387 q/s), saturates
there, and queue wait becomes non-zero for the first time (29.8 ms at 24 concurrent, 93.8 ms at 48).

Service time was already declared simulated in the original plan — "a CPU burn standing in for I/O".
This changes the mechanism of an already-declared simulation from a spin to an awaited interval, and
in doing so makes the item declared **real** — the pool and its queue — actually function. A
connection pool's service time is I/O wait; an interval models it more faithfully than a spin.

### 3. Service time is measured, not assumed

Windows' system timer granularity is 15.6 ms, so a requested 20 ms interval actually costs 22–31 ms.
Computing ρ from the configured value would understate it by up to 1.56×. The datastore therefore
measures the service interval that actually elapsed and publishes `{arrivalRate, serviceTimeMs,
utilisation}` per window from that measurement. `serviceTimeMs` in `config/tuning.json` remains the
value we request; the aggregate reports the value we got.

A consequence worth knowing: **`serviceTimeMs` cannot be tuned below roughly 16 ms on this machine**,
because the timer floor dominates. `poolSize` is the usable capacity control.

## Test harnesses — never on the demo path

### The pipeline-pressure harness

Real user traffic through the mesh produces about 1,900 telemetry events per second. The first
shed threshold is 4,000. To exercise admission and shedding at all, a harness posts synthetic
batches directly at the collector's `/ingest`.

It injects **only pri-1 latency samples and pri-2 debug logs** — precisely the classes the pipeline
is permitted to shed. It never emits pri-0, so every count, error rate, edge, amplification figure
and pool reading in the aggregates during a pressure test is genuine data from the running mesh.
That is deliberate: it means the exactness guarantee is tested against real counters rather than
against the harness's own fabrication.

The consequence to state plainly: during a pressure run the per-service latency **quantiles**
include synthetic samples. Counts, edges, amplification and resources do not.

The harness lives outside the repository, is never started by `npm run mesh`, and is not part of
any demo. Nothing a judge sees is produced by it.

**Measured result, CP-B, all seven gates passing.** Peak offered load 56,531 events/s.

| | baseline | under pressure | recovered |
|---|---|---|---|
| ingest | ~1.3k/s | 56.5k/s | ~1.3k/s |
| queue depth | 0 | 15,000 (cap 20,000) | 0 |
| shed level | 0 | **2** | 0 |
| pri-0 dropped | 0 | **0** | 0 |
| pri-1 / pri-2 dropped | 0 / 0 | 145,374 / 320,100 | 0 / 0 |
| observation confidence | 0.997 | **0.432** (min 0.341) | 0.996 |

Across 60 windows: `pri-0 dropped` was zero in every one; `amplification === attempts/calls` held in
every one with no violations; 19 of 20 pressure windows still carried live observed edges, with
14,518 genuine `auth→datastore` calls counted while 465,474 sheddable events were being discarded.
Every aggregate validated against the frozen contract. Window ids were contiguous throughout.

**Reproducing it.** The harness is not part of the repository, by design. To rebuild it: post batches
of 500 contract-valid events directly to `POST http://127.0.0.1:4100/ingest` from ~24 concurrent
workers, using only `pri: 1` samples (`kind: "sample"`, with `selfMs === totalMs - downstreamMs`) and
`pri: 2` logs (`kind: "log"`), with `svc` drawn from the five real service names and `seq` starting
above 50,000,000 so the dedup LRU sees distinct keys and masks nothing real. Run four phases against
a live mesh at 40 VUs: ~12 s baseline, ~20 s ignoring the `shedLevel` in the 202 ack, ~15 s obeying
it via `applyShed`, ~12 s recovery — subscribing to `ws://127.0.0.1:4100/stream` throughout and
validating each aggregate with `validateWindowAggregate`.

### No recorded cascade fixtures exist

`fixtures/` is empty, and that is a statement of fact rather than an oversight.

`tools/physics-cascade.js` records every WindowAggregate it observes, so producing
`cascade-f1.jsonl` and `cascade-f2.jsonl` is a single command. What blocks it is that every
run which actually produces an incident loses a process to the `3221226505` abort before
enough windows accumulate: F1 at 100, 120 and 140 virtual users, and F2 at 120, each ended
with the gateway or the datastore gone. At 80 virtual users the mesh survives comfortably
but the knee is never crossed, so the recording contains no incident at all.

Two partial recordings of 14 windows each were produced and then **deleted deliberately**.
They captured the moments before the process died — amplification just reaching x1.50, no
checkout degradation — and a fixture that looks real but contains no incident is worse than
no fixture, because a scorer calibrated against it would be calibrated against nothing.

The consequence is that the control plane's scorer has never been calibrated against
measured data, only against the synthetic development generator, and `tools/calibrate.js`
labels every such run PROVISIONAL for exactly this reason.

### Known gap between this harness and the demo

The demo's opening beat raises virtual users from 40 to 200. That produces roughly 1,900 events per
second — **below the 4,000 threshold** — so a VU spike alone does not currently drive shedding.
Backpressure and priority shedding are proven under real pressure by the harness, but not yet by a
traffic spike. Closing that gap is a tuning decision, not an implementation one.

---

*Status: written during the build. To be completed and verified at the feature freeze, including a
grep confirming no component reads from `fixtures/` on the demo path.*
