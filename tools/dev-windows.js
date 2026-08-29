'use strict';

// DEVELOPMENT ONLY — synthetic WindowAggregate source for the control plane.
//
// P2's chain consumes WindowAggregate, which does not exist until P1's collector runs.
// This process impersonates the collector: it binds the collector's port and speaks the
// same WebSocket, so control/adapters/telemetry.js and every agent behind it cannot tell
// the difference. At integration this process is stopped and the real collector takes the
// port — no control-plane code changes.
//
// WHAT THIS IS NOT: it is not a simulation of the physics. The numbers are a shaped ramp
// chosen to exercise the contract and the control-plane logic. Nothing measured here is
// evidence of anything. Every threshold derived against this generator is provisional
// until re-measured on P1's machine against the real mesh.
//
// Deleted at the H8 freeze. Nothing under control/ or console/ may import it.

const http = require('http');
const { WebSocketServer } = require('ws');
const {
  PORTS, SERVICES, windowAggregateProblems,
} = require('../packages/contracts');
const tuning = require('../config/tuning.json');

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

const SCENARIOS = ['healthy', 'spike', 'f1', 'f2'];

function parseArgs(argv) {
  const args = { scenario: 'f1', seed: 1, intervalMs: 1000, port: PORTS.collector,
                 epoch: null, lateDropped: true, maxWindows: Infinity, dev: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--dev') args.dev = true;
    else if (a === '--scenario') args.scenario = argv[++i];
    else if (a === '--seed') args.seed = Number(argv[++i]);
    else if (a === '--interval') args.intervalMs = Number(argv[++i]);
    else if (a === '--port') args.port = Number(argv[++i]);
    else if (a === '--epoch') args.epoch = Number(argv[++i]);
    else if (a === '--windows') args.maxWindows = Number(argv[++i]);
    else if (a === '--omit-late-dropped') args.lateDropped = false;
    else { console.error(`unknown argument ${a}`); process.exit(2); }
  }
  return args;
}

// ---------------------------------------------------------------------------
// Deterministic noise. Same (seed, window) always yields the same value, so two runs of
// the same scenario are byte-identical and a failing case can be replayed exactly.
// ---------------------------------------------------------------------------

function jitter(seed, n, salt) {
  let t = (seed * 2654435761 + n * 40503 + salt * 2246822519) >>> 0;
  t = Math.imul(t ^ (t >>> 15), t | 1);
  t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
  return (((t ^ (t >>> 14)) >>> 0) % 1000) / 1000;   // [0,1)
}

const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));
const lerp = (a, b, t) => a + (b - a) * t;
const round1 = (v) => Math.round(v * 10) / 10;

// ---------------------------------------------------------------------------
// Scenario timeline, in windows since start.
//   0..9    steady state
//   10..14  fault deployed, load still low — nothing visible ("it passed staging")
//   15..24  load ramps and the knee is crossed; severity climbs 0 -> 1
//   25+     sustained
// ---------------------------------------------------------------------------

const FAULT_AT = 10;
const RAMP_AT = 15;
const RAMP_WINDOWS = 10;

function severity(n) {
  return clamp((n - RAMP_AT) / RAMP_WINDOWS, 0, 1);
}

// Smallest window index at which severity exceeds a threshold — used to place
// firstDegradedAt without carrying state, so the builder stays a pure function of n.
function windowSeverityExceeds(threshold) {
  return Math.ceil(RAMP_AT + threshold * RAMP_WINDOWS) + 1;
}

function stateFor(scenario, n, degradeAt, criticalAt) {
  if (scenario === 'healthy' || scenario === 'spike') return 'HEALTHY';
  const s = severity(n);
  if (s > criticalAt) return 'CRITICAL';
  if (s > degradeAt) return 'DEGRADED';
  return 'HEALTHY';
}

// degradeAt thresholds per service. The gateway never degrades: it is behaving correctly
// by retrying, which is exactly why the scorer must not blame it.
const DEGRADE = {
  gateway:   { degradeAt: 2, criticalAt: 3 },      // never
  auth:      { degradeAt: 0.25, criticalAt: 0.70 },
  checkout:  { degradeAt: 0.45, criticalAt: 0.80 },
  payments:  { degradeAt: 0.45, criticalAt: 0.85 },
  datastore: { degradeAt: 0.50, criticalAt: 0.95 },
};

function firstDegradedAt(scenario, n, epoch, svc) {
  if (scenario === 'healthy' || scenario === 'spike') return null;
  const at = windowSeverityExceeds(DEGRADE[svc].degradeAt);
  if (n < at) return null;
  return epoch + at * 1000;
}

// ---------------------------------------------------------------------------
// The builder. Pure: (n, opts) -> WindowAggregate.
// ---------------------------------------------------------------------------

// Load model. One place, used by both the aggregate builder and the cumulative counters,
// so the two can never drift apart.
function loadAt(n, opts) {
  const { scenario } = opts;
  const s = scenario === 'f1' || scenario === 'f2' ? severity(n) : 0;
  const spiking = scenario === 'spike' && n >= FAULT_AT;

  // Closed-loop: offered load falls as the system slows. F2 collapses throughput further
  // because the datastore's service time more than doubles.
  const peakCalls = scenario === 'f2' ? 65 : 150;
  const gwAuthCalls = spiking ? 200 : Math.round(lerp(120, peakCalls, s));
  const amp = lerp(1.0, 2.9, s);
  const gwAuthAttempts = Math.max(gwAuthCalls, Math.round(gwAuthCalls * amp));
  const checkoutCalls = Math.round(gwAuthCalls * 0.7);

  return {
    s, spiking,
    vus: spiking ? 200 : Math.round(lerp(40, 140, s)),
    gwAuthCalls, gwAuthAttempts, checkoutCalls, paymentsCalls: checkoutCalls,
  };
}

// Pipeline pressure model. Telemetry volume rises with load and again with severity,
// because error paths log more than success paths — the incident generates its own spike.
function pipelineAt(n, opts) {
  const { s, spiking, gwAuthAttempts } = loadAt(n, opts);
  const loadFactor = clamp(gwAuthAttempts / 120, 0.5, 2);
  const ingestRate = Math.round(1200 * (1 + 4 * s) * (spiking ? 5 : 1) * loadFactor);

  const queueMax = tuning.collector.queueMax;
  const queueDepth = Math.round(clamp((ingestRate - 1500) * 1.2, 0, queueMax));
  const [t1, t2, t3] = tuning.collector.shedThresholds;
  const shedLevel = queueDepth >= t3 ? 3 : queueDepth >= t2 ? 2 : queueDepth >= t1 ? 1 : 0;

  // What the emitters discard at each level. pri-0 is never dropped at any level — that is
  // the guarantee the exactness claim rests on.
  const pri1SampleRate = [1, 1, 0.35, 0][shedLevel];
  const offeredPri1 = Math.round(ingestRate * 0.30);
  const offeredPri2 = Math.round(ingestRate * 0.25);
  const droppedPri1 = Math.round(offeredPri1 * (1 - pri1SampleRate));
  const droppedPri2 = shedLevel >= 1 ? offeredPri2 : 0;
  const offered = offeredPri1 + offeredPri2;
  const shedRate = offered === 0 ? 0 : clamp((droppedPri1 + droppedPri2) / offered, 0, 1);

  return {
    ingestRate, queueDepth, shedLevel, pri1SampleRate, shedRate,
    droppedPri1, droppedPri2,
    dedupHits: Math.round(jitter(opts.seed, n, 5) * 4),
    // Late events appear once the collector is far enough behind that the watermark closes
    // a window before a straggler arrives.
    lateDropped: shedLevel >= 2 ? Math.round(jitter(opts.seed, n, 7) * 12) : 0,
  };
}

// ---------------------------------------------------------------------------
// The builder. Pure: (n, opts) -> WindowAggregate.
// ---------------------------------------------------------------------------

function buildWindow(n, opts) {
  const { scenario, seed, epoch, lateDropped: withLateDropped } = opts;
  const load = loadAt(n, opts);
  const pipe = pipelineAt(n, opts);
  const { s, spiking, gwAuthCalls, gwAuthAttempts, checkoutCalls, paymentsCalls } = load;

  const tStart = epoch + n * 1000;
  const tEnd = tStart + 1000;

  // --- latency ----------------------------------------------------------
  // The discriminator. F1 puts auth's time in its own event loop; F2 puts it downstream.
  // Both raise auth's total p99 by a similar amount, which is why observation alone cannot
  // separate them and the probe is necessary.
  const authSelfP99 = scenario === 'f1'
    ? lerp(8, 300, Math.pow(s, 1.6))          // superlinear: the event loop saturates
    : lerp(8, 14, s);                          // F2: self time was never the constraint
  const dsServiceTime = scenario === 'f2' ? lerp(20, 45, s) : 20;
  const authDownstreamP99 = scenario === 'f2'
    ? lerp(15, 300, Math.pow(s, 1.3))
    : lerp(15, 60, s);

  // --- shared resource --------------------------------------------------
  // One token lookup per verify, so the datastore sees auth's amplified attempts plus
  // checkout's and payments' own queries. This is the only coupling in the system.
  const arrivalRate = gwAuthAttempts + checkoutCalls + paymentsCalls;
  const poolSize = tuning.datastore.poolSize;
  const utilisation = (arrivalRate * (dsServiceTime / 1000)) / poolSize;   // rho = lambda*W/N
  const poolInUse = Math.min(poolSize, Math.round(lerp(4, poolSize, Math.min(1, utilisation))));
  const queueDepth = Math.round(clamp((utilisation - 0.85) * 1400, 0, tuning.datastore.queueMax));
  const queueWaitP99 = round1(clamp((utilisation - 0.85) * 1800, 2, 2000));

  // --- pipeline counters ------------------------------------------------
  // Cumulative since process start, matching the magnitudes in the contract's own example.
  // A consumer that wants a rate differences consecutive windows.
  let droppedPri1 = 0, droppedPri2 = 0, dedupHits = 0, lateDroppedTotal = 0;
  for (let k = 0; k <= n; k++) {
    const past = pipelineAt(k, opts);
    droppedPri1 += past.droppedPri1;
    droppedPri2 += past.droppedPri2;
    dedupHits += past.dedupHits;
    lateDroppedTotal += past.lateDropped;
  }

  // Staleness of the newest closed window. Grows as the collector falls behind.
  const watermarkLagMs = Math.round(
    lerp(300, 1500, Math.max(s, spiking ? 0.8 : 0)) + jitter(seed, n, 3) * 60
  );
  const watermarkLag = clamp(watermarkLagMs / 5000, 0, 1);
  const silentServices = 0;
  const observationConfidence = clamp(
    1 - 0.5 * pipe.shedRate - 0.3 * watermarkLag - 0.2 * silentServices, 0, 1
  );

  // --- assemble ---------------------------------------------------------
  const services = {};
  for (const svc of SERVICES) {
    services[svc] = buildService(svc, {
      n, s, seed, scenario, epoch, gwAuthCalls, checkoutCalls,
      authSelfP99, authDownstreamP99, dsServiceTime, queueWaitP99,
      pri1SampleRate: pipe.pri1SampleRate,
    });
  }

  const edges = {};
  addEdge(edges, 'gateway→auth', gwAuthCalls, gwAuthAttempts, Math.round(gwAuthCalls * s * 0.08));
  addEdge(edges, 'gateway→checkout', checkoutCalls, Math.round(checkoutCalls * lerp(1, 1.4, s)), Math.round(checkoutCalls * s * 0.05));
  addEdge(edges, 'auth→datastore', gwAuthAttempts, gwAuthAttempts, 0);
  addEdge(edges, 'checkout→payments', checkoutCalls, checkoutCalls, 0);
  addEdge(edges, 'checkout→datastore', checkoutCalls, checkoutCalls, 0);
  addEdge(edges, 'payments→datastore', paymentsCalls, paymentsCalls, 0);

  const pipeline = {
    ingestRate: pipe.ingestRate,
    queueDepth: pipe.queueDepth,
    shedLevel: pipe.shedLevel,
    dropped: { pri0: 0, pri1: droppedPri1, pri2: droppedPri2 },
    watermarkLagMs,
    dedupHits,
  };
  // Optional in the frozen contract. The flag exists so the control plane is exercised
  // against an aggregate that carries it and one that does not.
  if (withLateDropped) pipeline.lateDropped = lateDroppedTotal;

  return {
    windowId: n,
    tStart,
    tEnd,
    closedAt: tEnd + watermarkLagMs,
    complete: true,
    services,
    edges,
    resources: {
      datastore: {
        poolSize,
        poolInUse,
        queueDepth,
        queueWaitP99,
        arrivalRate,
        serviceTimeMs: round1(dsServiceTime),
        utilisation: Math.round(utilisation * 100) / 100,
      },
    },
    pipeline,
    observationConfidence: Math.round(observationConfidence * 100) / 100,
    observationTerms: {
      shedRate: Math.round(pipe.shedRate * 100) / 100,
      watermarkLag: Math.round(watermarkLag * 100) / 100,
      silentServices,
    },
  };
}

function addEdge(edges, key, calls, attempts, timeouts) {
  const a = Math.max(calls, Math.round(attempts));
  edges[key] = {
    calls,
    attempts: a,
    // attempts/calls from pri-0 counter deltas and nothing else. Derived here rather than
    // set independently so it can never disagree with the counters it claims to summarise.
    amplification: calls > 0 ? Math.round((a / calls) * 1000) / 1000 : 0,
    timeouts: Math.max(0, timeouts),
  };
}

function buildService(svc, ctx) {
  const { n, s, seed, scenario, epoch, gwAuthCalls, checkoutCalls,
          authSelfP99, authDownstreamP99, dsServiceTime, queueWaitP99,
          shedLevel, pri1SampleRate } = ctx;

  let selfP99, downstreamP99, rps, errRate;
  switch (svc) {
    case 'auth':
      selfP99 = authSelfP99; downstreamP99 = authDownstreamP99;
      rps = gwAuthCalls; errRate = clamp(s * 0.12, 0, 1);
      break;
    case 'gateway':
      // The gateway's own time is small; it is waiting on auth and checkout.
      selfP99 = lerp(3, 6, s); downstreamP99 = authSelfP99 + authDownstreamP99;
      rps = gwAuthCalls; errRate = clamp(s * 0.18, 0, 1);
      break;
    case 'checkout':
      // A bystander. Nothing touches it; it queues behind auth's amplified queries.
      selfP99 = lerp(6, 12, s); downstreamP99 = lerp(18, 40, s) + queueWaitP99;
      rps = checkoutCalls; errRate = clamp(Math.pow(s, 1.4) * 0.30, 0, 1);
      break;
    case 'payments':
      selfP99 = lerp(5, 10, s); downstreamP99 = lerp(15, 35, s) + queueWaitP99;
      rps = checkoutCalls; errRate = clamp(Math.pow(s, 1.4) * 0.22, 0, 1);
      break;
    default: // datastore
      selfP99 = dsServiceTime + queueWaitP99; downstreamP99 = 0;
      rps = gwAuthCalls * 2; errRate = clamp(s * 0.05, 0, 1);
  }

  const noise = 1 + (jitter(seed, n, svc.length) - 0.5) * 0.04;
  const p99 = round1((selfP99 + downstreamP99) * noise);
  const inclusionProbability = Math.max(0.05, pri1SampleRate || 0.05);

  return {
    rps: round1(rps),
    errRate: Math.round(errRate * 1000) / 1000,
    inflight: Math.round(clamp(rps * (p99 / 1000), 0, 5000)),
    // Quantile ordering must hold or every latency number downstream is wrong.
    p50: round1(p99 * 0.30),
    p95: round1(p99 * 0.72),
    p99,
    selfP99: round1(selfP99 * noise),
    downstreamP99: round1(downstreamP99 * noise),
    sampleCount: Math.round(tuning.collector.reservoirSize * inclusionProbability),
    inclusionProbability: Math.round(inclusionProbability * 100) / 100,
    state: stateFor(scenario, n, DEGRADE[svc].degradeAt, DEGRADE[svc].criticalAt),
    firstDegradedAt: firstDegradedAt(scenario, n, epoch, svc),
  };
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

function start(args) {
  const epoch = args.epoch !== null ? args.epoch : Math.floor(Date.now() / 1000) * 1000;
  const opts = { scenario: args.scenario, seed: args.seed, epoch, lateDropped: args.lateDropped };

  const server = http.createServer((req, res) => {
    res.writeHead(200, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ svc: 'dev-windows', up: true, synthetic: true, ...opts }));
  });
  const wss = new WebSocketServer({ server, path: '/stream' });

  let n = 0;
  let invalid = 0;

  wss.on('connection', (ws) => {
    console.log(`[dev-windows] subscriber connected (${wss.clients.size} total)`);
    ws.on('close', () => console.log(`[dev-windows] subscriber left (${wss.clients.size} total)`));
    ws.on('error', () => {});
  });

  const timer = setInterval(() => {
    if (n >= args.maxWindows) { clearInterval(timer); shutdown(); return; }
    const w = buildWindow(n, opts);

    // The generator validates its own output against the frozen contract. A synthetic
    // source that emits an invalid aggregate would send the control plane chasing a bug
    // that does not exist.
    const problems = windowAggregateProblems(w);
    if (problems.length) {
      invalid++;
      console.error(`[dev-windows] window ${n} INVALID (${problems.length}):`);
      for (const p of problems) console.error(`  - ${p}`);
    }

    const msg = JSON.stringify(w);
    for (const ws of wss.clients) if (ws.readyState === ws.OPEN) ws.send(msg);

    if (n % 5 === 0 || w.pipeline.shedLevel > 0) {
      const a = w.services.auth;
      console.log(
        `[dev-windows] win ${String(n).padStart(3)} ` +
        `auth p99=${String(a.p99).padStart(6)} self=${String(a.selfP99).padStart(6)} down=${String(a.downstreamP99).padStart(6)} ` +
        `${a.state.padEnd(8)} amp=${w.edges['gateway→auth'].amplification.toFixed(2)} ` +
        `rho=${w.resources.datastore.utilisation.toFixed(2)} ` +
        `shed=${w.pipeline.shedLevel} oc=${w.observationConfidence.toFixed(2)}`
      );
    }
    n++;
  }, args.intervalMs);

  function shutdown() {
    console.log(`[dev-windows] stopping after ${n} windows (${invalid} invalid)`);
    clearInterval(timer);
    for (const ws of wss.clients) ws.close();
    server.close(() => process.exit(invalid === 0 ? 0 : 1));
    setTimeout(() => process.exit(invalid === 0 ? 0 : 1), 500).unref();
  }
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);

  server.listen(args.port, '127.0.0.1', () => {
    console.log('='.repeat(78));
    console.log('  DEVELOPMENT ONLY — SYNTHETIC TELEMETRY. NOT MEASURED. NOT EVIDENCE.');
    console.log('  Delete before the freeze. Nothing on the demo path may read this.');
    console.log('='.repeat(78));
    console.log(`[dev-windows] ws://127.0.0.1:${args.port}/stream`);
    console.log(`[dev-windows] scenario=${args.scenario} seed=${args.seed} epoch=${epoch} ` +
                `interval=${args.intervalMs}ms lateDropped=${args.lateDropped}`);
  });
}

module.exports = { buildWindow, loadAt, pipelineAt, severity, SCENARIOS };

if (require.main === module) {
  const args = parseArgs(process.argv.slice(2));
  if (!args.dev) {
    console.error('refusing to run without --dev: this is a development-only synthetic source');
    process.exit(2);
  }
  if (!SCENARIOS.includes(args.scenario)) {
    console.error(`--scenario must be one of ${SCENARIOS.join(', ')}`);
    process.exit(2);
  }
  start(args);
}
