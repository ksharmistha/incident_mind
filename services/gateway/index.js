'use strict';

const { startService, callJson } = require('../_shared/service');
const { PORTS } = require('../../packages/contracts');
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

async function callUpstream(name, body) {
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
    reset: () => {
      for (const c of Object.values(counters)) Object.assign(c, { calls: 0, attempts: 0, timeouts: 0 });
      return { ok: true };
    },
  },

  admin: {},

  health: () => ({ inFlight: { ...inFlight }, window: lastWindow }),
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
