'use strict';

// IncidentMind V2 control plane - :4200.
//
// Inbound seams, frozen with P1:
//   GET  /health   -> { svc: "control", up: true }     tools/mesh.js waits on this
//   POST /reset    -> clears state, aborts an in-flight probe   tools/reset.js calls this
//   POST /approve  -> the console's approval gate for irreversible actions
//   WS   /stream   -> ControlState on every change, full snapshot on connect
//
// Outbound, the control plane touches the data plane only through POST /admin/* and never
// through /chaos/*. Chaos endpoints are operator-only.

const http = require('http');
const express = require('express');
const { WebSocketServer } = require('ws');

const state = require('./state');
const supervisor = require('./supervisor');
const telemetry = require('./adapters/telemetry');
const { createDetector } = require('./agents/detector');
const { createScorer } = require('./agents/scorer');
const { createExperimenter } = require('./agents/experimenter');
const { createPlanner } = require('./agents/planner');
const admin = require('./adapters/admin');
const { PORTS, validationEnabled } = require('./adapters/contracts');
const { tuning, control, derived } = require('./adapters/tuning');

const PORT = Number(process.env.IM_CONTROL_PORT || PORTS.control);
const HOST = '127.0.0.1';

const app = express();
app.use(express.json());

app.get('/health', (req, res) => {
  res.json({ svc: 'control', up: true });
});

app.post('/reset', (req, res) => {
  const cleared = state.reset();
  telemetry.clear();
  detector.reset();
  scorer.reset();
  experimenter.reset();
  planner.reset();
  admin.reset();
  supervisor.resetStats();
  res.json({ ok: true, cleared });
});

app.post('/approve', (req, res) => {
  const { optionId } = req.body || {};
  if (!optionId) return res.status(400).json({ error: 'optionId required' });

  const plan = state.get().plan;
  if (!plan) return res.status(409).json({ error: 'no plan awaiting approval' });

  const option = plan.options.find((o) => o.id === optionId);
  if (!option) return res.status(404).json({ error: `unknown optionId "${optionId}"` });

  // Wired to the Executor in M10. Approving is recorded now so the seam exists and the
  // console can be built against it.
  console.log(`[control] approved ${optionId} (${option.actionType} on ${option.target})`);
  res.json({ ok: true, optionId });
});

// Operational visibility for the build. Not part of any contract and not on the demo path.
app.get('/debug/status', (req, res) => {
  res.json({
    contracts: 'packages/contracts (frozen)',
    tuning: 'config/tuning.json',
    validate: validationEnabled,
    telemetry: telemetry.status(),
    supervisor: supervisor.stats(),
    detector: detector.stats(),
    scorer: scorer.last(),
    experimenter: { inFlight: experimenter.isInFlight(), probe: experimenter.current()?.id ?? null },
    openBreakers: admin.openBreakers(),
    planner: state.get().plan ? { options: state.get().plan.options.length, effectiveConfidence: state.get().plan.effectiveConfidence, recommended: state.get().plan.recommendedOptionId } : null,
    state: state.get(),
  });
});

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/stream' });

function send(ws, payload) {
  if (ws.readyState !== ws.OPEN) return;
  // A stalled console must not be able to grow the control plane's memory without bound.
  if (ws.bufferedAmount > 1_000_000) return;
  ws.send(JSON.stringify(payload));
}

wss.on('connection', (ws) => {
  console.log(`[control] console connected (${wss.clients.size} total)`);
  send(ws, state.get());                       // full snapshot, so a late console is never blank
  ws.on('close', () => console.log(`[control] console disconnected (${wss.clients.size} total)`));
});

state.subscribe((current) => {
  for (const ws of wss.clients) send(ws, current);
});

// The Detector runs on its own interval, reading the newest closed window from the ring.
// It is deliberately not driven by the telemetry callback: detection must keep running at
// its own cadence even while everything downstream is waiting on a human approval.
const detector = createDetector();
const scorer = createScorer();
const experimenter = createExperimenter({ windowMs: tuning.collector.windowMs });
const planner = createPlanner();

// POST /reset must be able to stop an experiment from any state.
state.onProbeAbort(() => experimenter.abort());

// Kicked off from the detector tick but deliberately NOT awaited: a probe takes seconds,
// and detection must keep running throughout. Telemetry never stalls behind an experiment.
// The Planner describes; it never acts. Execution is the Executor's, behind the approval
// gate the plan itself declares.
async function replan(probeResult) {
  const current = state.get();
  if (!current.incident) return;
  const [newest] = telemetry.recent(1);
  const run = await supervisor.run('planner', () => planner.build({
    incident: current.incident,
    hypotheses: current.hypotheses,
    probeResult,
    observationConfidence: newest?.observationConfidence,
  }));
  if (!run.ok) return;
  state.update({ plan: run.value }, run.value
    ? `planner: ${run.value.options.length} options, effConf ${run.value.effectiveConfidence}, recommending ${run.value.recommendedOptionId || 'nothing'}`
    : 'planner: no supported action');
}

function maybeExperiment(incident, ranked, window) {
  if (!ranked.ambiguous || experimenter.isInFlight()) return;

  supervisor.run('experimenter', () => experimenter.run({
    hypotheses: ranked.hypotheses,
    incident,
    observationConfidence: window?.observationConfidence,
    telemetry,
    // Step 3 of §11.2. The probe reaches the console, predictions and timestamp included,
    // before the intervention fires. Never reorder this.
    publish: async (probe) => {
      state.update({ probe: { ...probe, phase: 'published' } }, `probe ${probe.id} PUBLISHED before execution`);
    },
  })).then((run) => {
    if (!run.ok || !run.value || !run.value.probe) return;
    const { probe, result } = run.value;
    const hypotheses = state.get().hypotheses.map((h) => (
      result && result.posteriors[h.id] !== undefined ? { ...h, posterior: result.posteriors[h.id] } : h
    ));
    state.update(
      { probe: { ...probe, phase: result?.inconclusive ? 'inconclusive' : 'measured', result }, hypotheses },
      `probe ${probe.id} ${result?.matched || 'INCONCLUSIVE'} (${result?.measuredDeltaPct}%)`
    );
    replan(result);
  });
}

supervisor.startInterval('detector', tuning.collector.windowMs, async () => {
  // Every window since the detector's cursor, not just the newest: the detector runs on
  // its own clock and must not lose EWMA samples when the two cadences drift.
  let incident = null;
  for (const w of telemetry.since(detector.stats().lastWindowId)) {
    incident = detector.observe(w);
  }
  if (!incident) return;
  state.update({ incident }, `detector: ${incident.id} @ window ${incident.lastWindowId}`);

  // Diagnosis follows detection in the same tick. The Scorer is pure, so it re-ranks from
  // scratch every window rather than carrying a verdict forward.
  const [newest] = telemetry.recent(1);
  const ranked = await supervisor.run('scorer', () => scorer.score(incident, newest));
  if (!ranked.ok) return;
  state.update({ hypotheses: ranked.value.hypotheses },
    `scorer: ${ranked.value.hypotheses.length} hypotheses, margin ${ranked.value.margin}` +
    `${ranked.value.ambiguous ? ' (AMBIGUOUS)' : ''}`);

  maybeExperiment(incident, ranked.value, newest);
  replan(state.get().probe?.result ?? null);
});

telemetry.start();

server.listen(PORT, HOST, () => {
  console.log(`[control] listening on http://${HOST}:${PORT}`);
  console.log(`[control]   GET  /health   POST /reset   POST /approve   WS /stream`);
  console.log(`[control] thresholds: margin<${control.probeMarginThreshold} autonomy>=${control.autonomyConfidence} probeFloor>=${derived.probeMinObservationConfidence}`);
});

function shutdown(signal) {
  console.log(`[control] ${signal} - shutting down`);
  telemetry.stop();
  for (const ws of wss.clients) ws.close();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000).unref();
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
