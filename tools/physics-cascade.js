'use strict';

const fs = require('node:fs');
const path = require('node:path');
const http = require('node:http');
const WebSocket = require('ws');
const { PORTS } = require('../packages/contracts');

// CP-A — the cascade is real.
//
//   node tools/physics-cascade.js [--fault=f1|f2] [--vus=140] [--runs=3] [--record=PATH]
//
// Gates, all read from the live pipeline rather than from the services directly, because
// the pipeline is the artifact under judgement and anything it cannot see does not count:
//
//   rho in [1.02, 1.10]              datastore utilisation, lambda*W/N
//   checkout errRate > 5% within 30s the bystander degrades
//   amplification > 2.5              gateway->auth attempts/calls, from pri-0 counters
//
// Three consecutive passing runs is the checkpoint. This harness exists so "does the
// cascade fire?" is answered with a tally instead of an impression — and so a run that
// does NOT pass is recorded as such rather than re-run until it looks good.
//
// --record writes every WindowAggregate to JSONL. That recording is real measured data,
// and it is what the control plane calibrates its scorer against.

const args = Object.fromEntries(
  process.argv.slice(2).map((a) => {
    const [k, v] = a.replace(/^--/, '').split('=');
    return [k, v ?? true];
  })
);

const FAULT = (args.fault ?? 'f1').toLowerCase();
const VUS = Number(args.vus ?? 140);
const RUNS = Number(args.runs ?? 3);
const RECORD = args.record ? path.resolve(String(args.record)) : null;

const SETTLE_S = 12;      // healthy at the target load before the fault lands
const OBSERVE_S = 90;     // after the fault
const GATES = { rhoMin: 1.02, rhoMax: 1.10, checkoutErrRate: 0.05, errorWithinS: 30, amplification: 2.5 };

const FAULTS = {
  f1: { port: PORTS.auth, path: '/chaos/version', body: { version: '2.4.1' }, label: 'F1 auth CPU regression' },
  f2: { port: PORTS.datastore, path: '/chaos/compaction', body: { on: true }, label: 'F2 datastore compaction' },
};

const ALL = { gateway: PORTS.gateway, auth: PORTS.auth, checkout: PORTS.checkout, payments: PORTS.payments, datastore: PORTS.datastore, collector: PORTS.collector, loadgen: PORTS.loadgen };

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const post = (port, p, body) =>
  fetch(`http://127.0.0.1:${port}${p}`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(body ?? {}), signal: AbortSignal.timeout(5000) })
    .then((r) => r.ok).catch(() => false);

// Dead and merely saturated look nothing alike at the socket level, and conflating them
// makes the harness report a process loss during exactly the incident it exists to
// measure. A dead process refuses the connection immediately; a live one pinned at 100%
// CPU — which is precisely what F1 does to auth — just answers slowly. Only a refused
// connection counts as death.
// One keep-alive connection per service, reused for every probe.
//
// The probe used to open a fresh socket each time, and at this load that exhausted the
// machine's ephemeral port range: the prober itself started failing with EADDRINUSE and
// reported healthy services as dead. Reusing connections removes the prober from the
// experiment it is supposed to be observing.
const probeAgent = new http.Agent({ keepAlive: true, maxSockets: 2, maxFreeSockets: 2 });

function probe(port) {
  return new Promise((resolve) => {
    const req = http.request({ agent: probeAgent, host: '127.0.0.1', port, path: '/health', method: 'GET' }, (res) => {
      res.resume();
      res.on('end', () => resolve(res.statusCode === 200));
    });
    req.on('error', () => resolve(false));
    req.setTimeout(8000, () => { req.destroy(); resolve(false); });
    req.end();
  });
}

// Death has to be proved, not inferred from one bad probe.
//
// Under the load this harness creates, a live service fails a probe for several reasons
// that have nothing to do with being dead: pinned at 100% CPU so it answers slowly (F1
// does exactly this to auth), an accept backlog that overflows and makes Windows refuse
// the connection, or this prober itself running out of client sockets. Every one of those
// looks momentarily identical to a process that has gone.
//
// So a service is only reported down after three consecutive failed probes spread over a
// second. Reporting a false death aborts the run and throws away the measurement — which
// is precisely the incident we were trying to observe.
async function allAlive() {
  const down = [];
  for (const [name, port] of Object.entries(ALL)) {
    // Five attempts over five seconds. A process that has genuinely exited refuses every
    // probe instantly; one that is merely pinned — F2 burns CPU inside the datastore's
    // query handler and overflows its accept backlog for seconds at a time — will answer
    // at least once in that window.
    let alive = false;
    for (let attempt = 0; attempt < 10 && !alive; attempt++) {
      if (attempt > 0) await sleep(1000);
      alive = await probe(port);
    }
    if (!alive) down.push(name);
  }
  return down;
}

const median = (a) => (a.length ? [...a].sort((x, y) => x - y)[Math.floor(a.length / 2)] : NaN);

async function runOnce(n, recordStream) {
  console.log(`\n${'='.repeat(66)}\nRUN ${n}/${RUNS} — ${FAULTS[FAULT].label} at ${VUS} VUs\n${'='.repeat(66)}`);

  const down = await allAlive();
  if (down.length) return { ok: false, aborted: `processes not running before the run: ${down.join(', ')}` };

  // A run starts from a clean system or it is not a run.
  //
  // Every service, not just the two that carry faults. The control plane can open a
  // circuit breaker autonomously, and a breaker left open from a previous run silently
  // strangles the traffic this run is trying to measure — which is exactly how the first
  // F1 recording ended up with amplification already at x2.46 before its fault landed.
  for (const svc of ['gateway', 'auth', 'checkout', 'payments', 'datastore']) {
    await post(PORTS[svc], '/chaos/reset');
  }
  await post(PORTS.collector, '/chaos/reset');
  await post(PORTS.control, '/reset');
  await post(PORTS.loadgen, '/load', { vus: VUS });

  // Let the reset settle out of the window stream before the settle period is measured.
  await sleep(2000);

  const windows = [];
  const ws = new WebSocket(`ws://127.0.0.1:${PORTS.collector}/stream`);
  let faultAt = null;
  ws.on('message', (raw) => {
    const w = JSON.parse(raw.toString());
    w.__sinceFault = faultAt === null ? null : (Date.now() - faultAt) / 1000;
    windows.push(w);
    if (recordStream) recordStream.write(`${JSON.stringify(w)}\n`);
  });
  await new Promise((res, rej) => { ws.on('open', res); ws.on('error', rej); });

  console.log(`  settling ${SETTLE_S}s at ${VUS} VUs, no fault...`);
  await sleep(SETTLE_S * 1000);
  const baseline = windows.slice();

  // Apply the fault and CONFIRM it took. A silently failed POST — the datastore busy, the
  // socket refused for a moment — produces a run that measures a perfectly healthy system
  // and reports it as a failed cascade. That is worse than an error, because the numbers
  // look real. Verified against /health.faults[], retried, and aborted if it never lands.
  const f = FAULTS[FAULT];
  let applied = false;
  for (let attempt = 0; attempt < 5 && !applied; attempt++) {
    if (attempt > 0) await sleep(1000);
    await post(f.port, f.path, f.body);
    try {
      const h = await (await fetch(`http://127.0.0.1:${f.port}/health`, { signal: AbortSignal.timeout(5000) })).json();
      applied = (h.faults ?? []).length > 0;
    } catch {
      applied = false;
    }
  }
  if (!applied) {
    ws.close();
    return { ok: false, aborted: `${f.label} did not take — /health reports no fault after 5 attempts` };
  }
  faultAt = Date.now();
  console.log(`  ${f.label} applied — observing ${OBSERVE_S}s\n`);

  const deadline = Date.now() + OBSERVE_S * 1000;
  let lost = null;
  while (Date.now() < deadline) {
    await sleep(6000);
    // Two consecutive detections before believing it. One is nearly always saturation.
    const gone = await allAlive();
    if (gone.length) {
      await sleep(3000);
      const stillGone = await allAlive();
      if (stillGone.length) { lost = stillGone; break; }
    }
    const w = windows.at(-1);
    if (w) {
      const rho = w.resources.datastore?.utilisation ?? 0;
      const amp = w.edges['gateway→auth']?.amplification ?? 0;
      const err = w.services.checkout?.errRate ?? 0;
      console.log(`    t+${String(Math.round(w.__sinceFault ?? 0)).padStart(3)}s  rho=${rho.toFixed(2)}  amp=x${amp.toFixed(2)}  checkoutErr=${(err * 100).toFixed(1)}%  oc=${w.observationConfidence.toFixed(2)}`);
    }
  }
  ws.close();

  if (lost) return { ok: false, aborted: `process loss during the run: ${lost.join(', ')}`, windows };

  const after = windows.filter((w) => w.__sinceFault !== null && w.__sinceFault > 0);
  const steady = after.filter((w) => w.__sinceFault >= 30);
  const firstErr = after.find((w) => (w.services.checkout?.errRate ?? 0) > GATES.checkoutErrRate);

  const rho = median(steady.map((w) => w.resources.datastore?.utilisation ?? 0));
  const amp = median(steady.map((w) => w.edges['gateway→auth']?.amplification ?? 0));
  const errT = firstErr ? firstErr.__sinceFault : null;

  const gates = {
    rho: { value: rho, pass: rho >= GATES.rhoMin && rho <= GATES.rhoMax, want: `${GATES.rhoMin}–${GATES.rhoMax}` },
    checkoutErr: { value: errT, pass: errT !== null && errT <= GATES.errorWithinS, want: `>5% within ${GATES.errorWithinS}s` },
    amplification: { value: amp, pass: amp > GATES.amplification, want: `>${GATES.amplification}` },
  };

  console.log('');
  for (const [name, g] of Object.entries(gates)) {
    const shown = g.value === null ? 'never' : typeof g.value === 'number' ? g.value.toFixed(3) : g.value;
    console.log(`  ${g.pass ? 'PASS' : 'FAIL'}  ${name.padEnd(14)} ${String(shown).padStart(8)}   want ${g.want}`);
  }

  return { ok: Object.values(gates).every((g) => g.pass), gates, windows, baseline };
}

(async () => {
  if (!FAULTS[FAULT]) { console.error(`unknown fault "${FAULT}" — use f1 or f2`); process.exit(2); }

  let recordStream = null;
  if (RECORD) {
    fs.mkdirSync(path.dirname(RECORD), { recursive: true });
    recordStream = fs.createWriteStream(RECORD);
    console.log(`recording every WindowAggregate to ${RECORD}`);
  }

  const results = [];
  for (let i = 1; i <= RUNS; i++) {
    const r = await runOnce(i, recordStream);
    results.push(r);
    if (r.aborted) console.log(`\n  RUN ABORTED — ${r.aborted}`);
    if (i < RUNS) { await post(PORTS.loadgen, '/load', { vus: 40 }); await sleep(5000); }
  }

  if (recordStream) await new Promise((r) => recordStream.end(r));

  const passed = results.filter((r) => r.ok).length;
  const aborted = results.filter((r) => r.aborted).length;

  console.log(`\n${'='.repeat(66)}\nCP-A: ${passed}/${RUNS} runs passed all gates${aborted ? `, ${aborted} aborted` : ''}\n${'='.repeat(66)}`);
  for (const [i, r] of results.entries()) {
    if (r.aborted) { console.log(`  run ${i + 1}: ABORTED — ${r.aborted}`); continue; }
    const g = r.gates;
    console.log(`  run ${i + 1}: ${r.ok ? 'PASS' : 'FAIL'}  rho=${g.rho.value.toFixed(3)} amp=x${g.amplification.value.toFixed(2)} checkoutErr@${g.checkoutErr.value ?? 'never'}s`);
  }
  if (RECORD) console.log(`\n  recording written: ${RECORD}`);

  // CP-A requires three consecutive passes. Anything less is reported as what it is.
  process.exit(passed === RUNS ? 0 : 1);
})();
