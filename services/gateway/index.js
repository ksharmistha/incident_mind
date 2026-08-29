'use strict';

const { startService, callJson } = require('../_shared/service');
const { PORTS, MAX_PROBE_FRACTION, MAX_PROBE_DURATION_MS } = require('../../packages/contracts');
const tuning = require('../../config/tuning.json');

// The gateway is the amplification source. It is not misbehaving: a short deadline plus
// bounded retries with jitter is ordinary production practice. The damage comes from
// composition — when auth slows past the 300ms deadline, every logical call becomes up
// to three network attempts, and auth's downstream query rate triples with it.

const { timeoutMs, retries, backoffBaseMs, bulkhead } = tuning.gateway;

const UPSTREAM = {
  auth: `http://127.0.0.1:${PORTS.auth}/verify`,
  checkout: `http://127.0.0.1:${PORTS.checkout}/checkout`,
};

// Per-upstream concurrency limit, fail-fast with no queue. A slow auth must not consume
// every worker the gateway has, or checkout requests never even get issued.
const inFlight = { auth: 0, checkout: 0 };

// TASK #7 — calls vs attempts.
//
// calls increments once per LOGICAL call; attempts increments once per NETWORK attempt,
// including every retry. amplification is attempts/calls, and getting the distinction
// right is the whole headline number: counting retries as calls would report x1.0 during
// the exact incident the retries are causing.
//
// Both ride pri-0 counter snapshots (via telemetryCounters below), so amplification stays
// exact at every shed level. The local mirror is kept only for the 1Hz log line.
const counters = {
  auth: { calls: 0, attempts: 0, timeouts: 0 },
  checkout: { calls: 0, attempts: 0, timeouts: 0 },
};

// Set once startService hands us the pri-0 counter object.
let telemetryCounters = null;

let lastWindow = { auth: { calls: 0, attempts: 0, amplification: 0, timeouts: 0 },
                   checkout: { calls: 0, attempts: 0, amplification: 0, timeouts: 0 } };

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// Circuit breaker per upstream. Both a failure mechanism and a recovery action: opening
// the breaker on auth is how the control plane isolates a bad dependency, which frees the
// datastore permits auth was consuming.
const breakers = { auth: false, checkout: false };

// Probe shed. Drop a bounded fraction of calls to one upstream for a bounded time, serving
// a degraded response instead. The timer that ends it lives HERE, on the server, so the
// probe expires even if the control plane dies mid-experiment. That auto-expiry is the
// safety property that makes probing a live system defensible, and it is worth saying out
// loud on stage.
const probeShed = { auth: null, checkout: null };

function endProbe(name, emitter, reason) {
  const active = probeShed[name];
  if (!active) return;
  clearTimeout(active.timer);
  probeShed[name] = null;
  emitter?.enqueue({ pri: 0, kind: 'probe', probeId: active.probeId, phase: 'end' });
  console.log(`admin: probe ${active.probeId} ENDED on ${name} (${reason})`);
}

function startProbe({ probeId, upstream, fraction, durationMs }, emitter) {
  // Clamped to the contract's rails. The control plane clamps too; both sides do it so
  // neither can be widened by trusting the other.
  const safeFraction = Math.min(Math.max(fraction, 0), MAX_PROBE_FRACTION);
  const safeDuration = Math.min(Math.max(durationMs, 0), MAX_PROBE_DURATION_MS);

  endProbe(upstream, emitter, 'superseded');
  const timer = setTimeout(() => endProbe(upstream, emitter, 'auto-expired'), safeDuration);
  probeShed[upstream] = { probeId, fraction: safeFraction, timer, startedAt: Date.now(), durationMs: safeDuration };

  emitter?.enqueue({ pri: 0, kind: 'probe', probeId, phase: 'start' });
  console.log(`admin: probe ${probeId} STARTED — shedding ${safeFraction} of ${upstream} calls for ${safeDuration}ms`);
  return { ok: true, probeId, upstream, fraction: safeFraction, durationMs: safeDuration, expiresAt: Date.now() + safeDuration };
}

async function callUpstream(name, body) {
  if (breakers[name]) {
    const err = new Error(`${name} breaker is open`);
    err.status = 503;
    err.code = 'BREAKER_OPEN';
    throw err;
  }

  // Shed decided before the counters move: a call we deliberately never made is not a
  // call, so amplification stays attempts/calls over the calls actually attempted.
  const shed = probeShed[name];
  if (shed && Math.random() < shed.fraction) {
    return { status: 200, body: { degraded: true, probeId: shed.probeId, upstream: name } };
  }

  const limit = bulkhead[name];
  if (limit !== undefined && inFlight[name] >= limit) {
    const err = new Error(`${name} bulkhead full (${limit} in flight)`);
    err.status = 503;
    err.code = 'BULKHEAD_FULL';
    throw err;
  }

  counters[name].calls++;
  telemetryCounters?.call(name, name === 'auth' ? 'verify' : 'checkout');
  inFlight[name]++;
  try {
    let lastError = null;
    for (let attempt = 0; attempt <= retries; attempt++) {
      if (attempt > 0) {
        // Full jitter: uniform in [0, base * 2^attempt). Spreads the retry storm out
        // instead of synchronising every retrying caller onto the same instant.
        await sleep(Math.random() * backoffBaseMs * 2 ** attempt);
      }
      counters[name].attempts++;
      telemetryCounters?.attempt(name, name === 'auth' ? 'verify' : 'checkout');
      try {
        const res = await callJson(UPSTREAM[name], body, timeoutMs[name]);
        if (res.status < 500) return res;
        lastError = new Error(`${name} returned ${res.status}`);
      } catch (err) {
        if (err.code === 'UPSTREAM_TIMEOUT') {
          counters[name].timeouts++;
          telemetryCounters?.timeout(name, name === 'auth' ? 'verify' : 'checkout');
        }
        lastError = err;
      }
    }
    const err = new Error(`${name} failed after ${retries + 1} attempts: ${lastError?.message}`);
    err.status = 503;
    err.code = 'UPSTREAM_EXHAUSTED';
    throw err;
  } finally {
    inFlight[name]--;
  }
}

startService({
  svc: 'gateway',

  routes(app, { counters: telemetry }) {
    telemetryCounters = telemetry;

    // auth /verify is called on EVERY request, which is why auth is critical.
    app.post('/api/login', async (req, res) => {
      const verified = await callUpstream('auth', { token: req.body?.token });
      res.json({ ok: true, session: verified.body });
    });

    app.post('/api/browse', async (req, res) => {
      await callUpstream('auth', { token: req.body?.token });
      res.json({ ok: true, items: 20 });
    });

    // auth and checkout are independent downstream calls, and they are issued
    // independently. allSettled rather than sequential awaits is the whole point: the
    // checkout call must go out whatever auth does, or checkout's failure during an
    // incident would be a consequence of this control flow rather than of contention for
    // the datastore pool — and "there is no rule linking auth to checkout" would be false.
    app.post('/api/checkout', async (req, res) => {
      const [verify, order] = await Promise.allSettled([
        callUpstream('auth', { token: req.body?.token }),
        callUpstream('checkout', { userId: req.body?.userId, orderId: req.body?.orderId }),
      ]);

      // Both outcomes are reported, neither suppresses the other's call.
      if (order.status === 'rejected') throw order.reason;
      if (verify.status === 'rejected') throw verify.reason;
      res.json({ ok: true, verified: verify.value.body?.valid ?? null, order: order.value.body });
    });
  },

  chaos: {
    reset: (_body, { emitter } = {}) => {
      for (const c of Object.values(counters)) Object.assign(c, { calls: 0, attempts: 0, timeouts: 0 });
      for (const name of Object.keys(breakers)) breakers[name] = false;
      for (const name of Object.keys(probeShed)) endProbe(name, emitter, 'reset');
      return { ok: true };
    },
  },

  admin: {
    // Isolate an upstream. Reversible, which is why the Planner may run it autonomously.
    breaker: ({ upstream, open }) => {
      if (!(upstream in breakers)) throw Object.assign(new Error(`unknown upstream "${upstream}"`), { status: 400, code: 'UNKNOWN_UPSTREAM' });
      breakers[upstream] = open !== false;
      console.log(`admin: breaker on ${upstream} ${breakers[upstream] ? 'OPEN' : 'closed'}`);
      return { ok: true, upstream, open: breakers[upstream] };
    },

    probe: ({ probeId, action, upstream, fraction, durationMs }, { emitter } = {}) => {
      if (action !== 'shed') throw Object.assign(new Error(`unsupported probe action "${action}"`), { status: 400, code: 'UNSUPPORTED_ACTION' });
      if (!(upstream in probeShed)) throw Object.assign(new Error(`unknown upstream "${upstream}"`), { status: 400, code: 'UNKNOWN_UPSTREAM' });
      if (breakers[upstream]) throw Object.assign(new Error(`breaker already open on ${upstream}`), { status: 409, code: 'BREAKER_OPEN' });
      // One probe in flight per upstream; a second one supersedes the first rather than
      // stacking two sheds on the same edge.
      return startProbe({ probeId, upstream, fraction, durationMs }, emitter);
    },

    restart: (_body, { emitter } = {}) => {
      for (const c of Object.values(counters)) Object.assign(c, { calls: 0, attempts: 0, timeouts: 0 });
      for (const name of Object.keys(breakers)) breakers[name] = false;
      for (const name of Object.keys(probeShed)) endProbe(name, emitter, 'restart');
      console.log('admin: restart — in-memory state cleared');
      return { ok: true, svc: 'gateway', restarted: 'state', at: Date.now() };
    },
  },

  health: () => ({
    inFlight: { ...inFlight },
    window: lastWindow,
    breakers: { ...breakers },
    faults: Object.entries(breakers).filter(([, open]) => open).map(([n]) => `breaker:${n}`),
    probes: Object.fromEntries(Object.entries(probeShed).filter(([, p]) => p)
      .map(([n, p]) => [n, { probeId: p.probeId, fraction: p.fraction, msRemaining: Math.max(0, p.startedAt + p.durationMs - Date.now()) }])),
  }),
});

// Amplification, visible in the logs before any pipeline exists, and readable over
// /health so the physics harness reads the same counters the log line prints.
setInterval(() => {
  const parts = [];
  const window = {};
  for (const [name, c] of Object.entries(counters)) {
    const amplification = c.calls > 0 ? c.attempts / c.calls : 0;
    window[name] = { calls: c.calls, attempts: c.attempts, amplification: Number(amplification.toFixed(3)), timeouts: c.timeouts };
    parts.push(`${name} calls=${c.calls} attempts=${c.attempts} amp=x${c.calls > 0 ? amplification.toFixed(2) : '-'} timeouts=${c.timeouts}`);
    Object.assign(c, { calls: 0, attempts: 0, timeouts: 0 });
  }
  lastWindow = window;
  console.log(parts.join('  |  '));
}, 1000).unref();
