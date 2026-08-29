'use strict';

const { AsyncLocalStorage } = require('node:async_hooks');

// Request-scoped instrumentation.
//
// TASK #5 — selfMs.
//
//   selfMs = totalMs - sum(downstreamMs)
//
// This is the single most important derived value in the system, because it separates
// "this service is slow" from "this service is waiting on something slow". F1 raises
// auth's selfP99 while its downstreamP99 stays moderate; F2 does the opposite. Both look
// identical from the outside — same saturated pool, same amplified retries, same failing
// bystanders — and selfMs is the only thing that tells them apart. It is what P2's probe
// measures, so a service that computes it wrong breaks the signature feature silently.
//
// The accumulation uses AsyncLocalStorage so that every outbound call made while handling
// a request adds to that request's downstream total without any call site having to pass
// context around. The gateway's retry loop nests several attempts inside one logical
// call, and all of them correctly land against the request that caused them.

const store = new AsyncLocalStorage();

// Called by the shared HTTP client for every outbound attempt, with the interval the call
// actually occupied.
//
// Intervals rather than a running sum, because the gateway issues auth and checkout
// concurrently: adding their durations would charge the request for 300ms of waiting when
// it only ever waited 150ms, and selfMs would go negative. What selfMs means is "time this
// service was NOT waiting on anything downstream", so what has to be subtracted is the
// union of the downstream intervals, not their sum.
function recordDownstream(startedAt, endedAt) {
  const ctx = store.getStore();
  if (ctx) ctx.intervals.push([startedAt, endedAt]);
}

// Total wall-clock time during which at least one downstream call was outstanding.
function unionMs(intervals) {
  if (intervals.length === 0) return 0;
  const sorted = [...intervals].sort((a, b) => a[0] - b[0]);
  let total = 0;
  let [start, end] = sorted[0];
  for (let i = 1; i < sorted.length; i++) {
    const [s, e] = sorted[i];
    if (s > end) {
      total += end - start;
      start = s;
      end = e;
    } else if (e > end) {
      end = e;
    }
  }
  return total + (end - start);
}

function currentContext() {
  return store.getStore();
}

// Express middleware. Opens a context per request, and on response emits one pri-1 latency
// sample and updates the pri-0 request counters.
function middleware({ emitter, counters, op }) {
  return (req, res, next) => {
    const started = performance.now();
    const ctx = { intervals: [] };
    counters.inflight++;

    res.on('finish', () => {
      const totalMs = performance.now() - started;
      // Clamped so a downstream call that outlives the response by a fraction of a
      // millisecond cannot produce a negative selfMs from a rounding artefact.
      const downstreamMs = Math.min(totalMs, unionMs(ctx.intervals));
      const selfMs = Math.max(0, totalMs - downstreamMs);
      counters.inflight--;
      counters.request(res.statusCode);

      emitter.enqueue({
        pri: 1,
        kind: 'sample',
        op: typeof op === 'function' ? op(req) : op,
        totalMs: Number(totalMs.toFixed(3)),
        selfMs: Number(selfMs.toFixed(3)),
        downstreamMs: Number(downstreamMs.toFixed(3)),
        status: res.statusCode,
      });
    });

    store.run(ctx, next);
  };
}

// One pri-0 counter snapshot per second, plus the emitter's own flush cadence.
function startCounterFlush(emitter, counters) {
  const timer = setInterval(() => emitter.enqueue(counters.snapshot()), 1000);
  timer.unref();
  return timer;
}

function emitState(emitter, from, to) {
  emitter.enqueue({ pri: 0, kind: 'state', from, to });
}

module.exports = { middleware, recordDownstream, currentContext, startCounterFlush, emitState, store };
