'use strict';

const { PORTS } = require('../packages/contracts');
const tuning = require('../config/tuning.json');

// npm run reset — back to a clean, running system in well under twenty seconds.
//
// The budget is the feature. It is what lets a judge say "do it again with a different
// fault" and get a second complete run inside the five minutes, so this is treated as part
// of the demo rather than as a maintenance script.
//
// Everything that can happen in parallel does. The only ordering that matters is that the
// data plane stops being faulted before the collector's window history is cleared —
// otherwise the first "clean" window still describes the incident.

const SERVICES = ['gateway', 'auth', 'checkout', 'payments', 'datastore'];
const DEADLINE_MS = 20000;
const HEALTH_TIMEOUT_MS = 15000;

const started = Date.now();
const elapsed = () => `${((Date.now() - started) / 1000).toFixed(1)}s`;

function post(port, path, body) {
  return fetch(`http://127.0.0.1:${port}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body ?? {}),
    signal: AbortSignal.timeout(4000),
  })
    .then((r) => ({ ok: r.ok, status: r.status }))
    .catch((err) => ({ ok: false, error: err.message }));
}

async function healthy(name, port) {
  try {
    const res = await fetch(`http://127.0.0.1:${port}/health`, { signal: AbortSignal.timeout(1000) });
    if (!res.ok) return false;
    const body = await res.json();
    // Green means up AND carrying no faults. A service still reporting a fault has not
    // finished resetting, and reporting READY then would be a lie the demo depends on.
    return body.up === true && (body.faults ?? []).length === 0;
  } catch {
    return false;
  }
}

async function waitAllGreen() {
  const deadline = Date.now() + HEALTH_TIMEOUT_MS;
  const targets = [...SERVICES.map((s) => [s, PORTS[s]]), ['collector', PORTS.collector], ['loadgen', PORTS.loadgen]];
  while (Date.now() < deadline) {
    const states = await Promise.all(targets.map(async ([n, p]) => [n, await healthy(n, p)]));
    const down = states.filter(([, ok]) => !ok).map(([n]) => n);
    if (down.length === 0) return { green: true };
    await new Promise((r) => setTimeout(r, 250));
  }
  const states = await Promise.all(targets.map(async ([n, p]) => [n, await healthy(n, p)]));
  return { green: false, down: states.filter(([, ok]) => !ok).map(([n]) => n) };
}

(async () => {
  // 1. Clear every injected fault. This is the operator surface, which is exactly right:
  //    resetting the demo is an operator action, and the control plane must never be the
  //    thing that un-breaks the system.
  const cleared = await Promise.all(SERVICES.map((s) => post(PORTS[s], '/chaos/reset')));
  const failedClear = SERVICES.filter((s, i) => !cleared[i].ok);
  console.log(`[reset] chaos cleared on ${SERVICES.length - failedClear.length}/${SERVICES.length} services${failedClear.length ? ` (failed: ${failedClear.join(', ')})` : ''}  ${elapsed()}`);

  // 2. Only now clear observation history: windows, dedup LRU and the observed graph. The
  //    data plane is already clean, so the next window describes the system as it now is.
  const collector = await post(PORTS.collector, '/chaos/reset');
  console.log(`[reset] collector windows, dedup LRU and graph cleared: ${collector.ok ? 'ok' : `FAILED (${collector.error ?? collector.status})`}  ${elapsed()}`);

  // 3. Control-plane incident state. Optional: the control plane may legitimately not be
  //    running yet, and a reset must not fail because of that.
  const control = await post(PORTS.control, '/reset');
  console.log(`[reset] control incident state: ${control.ok ? 'cleared' : 'not running (skipped)'}  ${elapsed()}`);

  // 4. Back to the baseline load.
  const vus = tuning.loadgen.vusDefault;
  const load = await post(PORTS.loadgen, '/load', { vus });
  console.log(`[reset] loadgen -> ${vus} VUs: ${load.ok ? 'ok' : `FAILED (${load.error ?? load.status})`}  ${elapsed()}`);

  // 5. Do not claim ready until it is.
  const result = await waitAllGreen();
  const total = Date.now() - started;

  if (!result.green) {
    console.log(`\n\u001b[31m[reset] NOT READY after ${elapsed()} — not green: ${result.down.join(', ')}\u001b[0m\n`);
    process.exit(1);
  }

  const budget = total < DEADLINE_MS ? `\u001b[32mwithin the ${DEADLINE_MS / 1000}s budget\u001b[0m` : `\u001b[31mOVER the ${DEADLINE_MS / 1000}s budget\u001b[0m`;
  console.log(`\n\u001b[32m${'='.repeat(54)}\n  READY — clean system at ${vus} VUs in ${(total / 1000).toFixed(1)}s\n${'='.repeat(54)}\u001b[0m`);
  console.log(`  ${budget}\n`);
  process.exit(total < DEADLINE_MS ? 0 : 1);
})();
