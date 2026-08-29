'use strict';

// The control plane's ONLY write path into the data plane.
//
// Every outbound call the Executor or Experimenter makes goes through here, and the URL is
// composed from a four-entry allowlist. The operator-only fault-injection routes are not
// merely un-called — they are unrepresentable: no code path can produce those URLs. A judge
// who sees the control plane inject or clear a fault has caught the whole project cheating.
//
// This is the mock/real boundary. In development the ports are answered by
// tools/dev-windows.js; at integration P1's real services bind the same ports and nothing
// here changes. The adapter never imports the mock — it only ever speaks HTTP.

const { PORTS, MAX_PROBE_FRACTION, MAX_PROBE_DURATION_MS } = require('./contracts');

// POST /admin/* — the complete set from the frozen contract §8.6. Nothing else is callable.
const ADMIN_PATHS = {
  version: '/admin/version',
  breaker: '/admin/breaker',
  restart: '/admin/restart',
  probe: '/admin/probe',
};

const DEFAULT_TIMEOUT_MS = 2000;

// Breakers we opened ourselves. The WindowAggregate carries no breaker state, so this plus
// GET /health.faults[] is the only way the Experimenter can honour "never probe an edge
// whose breaker is already open".
const openBreakers = new Set();

function baseUrl(service) {
  const port = PORTS[service];
  if (!port) throw new Error(`unknown service "${service}" — not in the frozen PORTS map`);
  return `http://127.0.0.1:${port}`;
}

async function call(service, action, body, timeoutMs = DEFAULT_TIMEOUT_MS) {
  const path = ADMIN_PATHS[action];
  if (!path) throw new Error(`"${action}" is not an admin action — allowed: ${Object.keys(ADMIN_PATHS).join(', ')}`);

  const url = `${baseUrl(service)}${path}`;
  const started = Date.now();
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(body ?? {}),
      signal: AbortSignal.timeout(timeoutMs),
    });
    const text = await res.text();
    let parsed = null;
    try { parsed = text ? JSON.parse(text) : null; } catch { /* body is not JSON; keep the text */ }
    console.log(`[admin] POST ${service}${path} -> ${res.status} (${Date.now() - started}ms)`);
    return { ok: res.ok, httpStatus: res.status, body: parsed, raw: text, url };
  } catch (err) {
    // A transport failure is not a success and must never be reported as one.
    console.error(`[admin] POST ${service}${path} FAILED after ${Date.now() - started}ms: ${err.message}`);
    return { ok: false, httpStatus: 0, body: null, error: err.message, url };
  }
}

// Bounded, reversible, auto-expiring. The rails are clamped here as well as validated by
// the contract, so a caller cannot widen them by passing a larger number.
async function probe(target, { probeId, upstream, fraction, durationMs }, timeoutMs) {
  const safeFraction = Math.min(fraction, MAX_PROBE_FRACTION);
  const safeDuration = Math.min(durationMs, MAX_PROBE_DURATION_MS);
  if (safeFraction !== fraction || safeDuration !== durationMs) {
    console.warn(`[admin] probe clamped to the contract rails: fraction ${fraction}->${safeFraction}, durationMs ${durationMs}->${safeDuration}`);
  }
  return call(target, 'probe', {
    probeId, action: 'shed', upstream, fraction: safeFraction, durationMs: safeDuration,
  }, timeoutMs);
}

async function setBreaker(target, upstream, open, timeoutMs) {
  const res = await call(target, 'breaker', { upstream, open }, timeoutMs);
  const key = `${target}→${upstream}`;
  if (res.ok) { open ? openBreakers.add(key) : openBreakers.delete(key); }
  return res;
}

function isBreakerOpen(target, upstream) {
  return openBreakers.has(`${target}→${upstream}`);
}

async function health(service) {
  try {
    const res = await fetch(`${baseUrl(service)}/health`, { signal: AbortSignal.timeout(DEFAULT_TIMEOUT_MS) });
    return res.ok ? await res.json() : null;
  } catch {
    return null;
  }
}

function reset() {
  openBreakers.clear();
}

module.exports = {
  ADMIN_PATHS, call, probe, setBreaker, isBreakerOpen, health, reset,
  openBreakers: () => [...openBreakers],
};
