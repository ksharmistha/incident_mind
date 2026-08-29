'use strict';

// IncidentMind V2 control plane - :4200.
//
// Inbound seams, frozen with P1:
//   GET  /health   -> { svc: "control", up: true }     tools/mesh.js waits on this
//   POST /reset    -> clears state, aborts an in-flight probe   tools/reset.js calls this
//   POST /approve  -> the console's approval gate for irreversible actions
//   WS   /stream   -> ControlState on every change, full snapshot on connect
//
// Outbound, the control plane touches the data plane only through POST /admin/*. The
// fault-injection routes are operator-only and unreachable from here by construction.

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
const { createExecutor } = require('./agents/executor');
const { createVerifier } = require('./agents/verifier');
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
  executor.reset();
  verifier.reset();
  verifyCursor = null;
  verifyDeadline = 0;
  admin.reset();
  supervisor.resetStats();
  res.json({ ok: true, cleared });
});

// The console's approval gate. Approving records consent for one option on one incident,
// then hands it to the Executor — the only component that can act.
app.post('/approve', async (req, res) => {
  const { optionId } = req.body || {};
  if (!optionId) return res.status(400).json({ error: 'optionId required' });

  const { incident, plan } = state.get();
  if (!incident) return res.status(409).json({ error: 'no active incident' });
  if (!plan) return res.status(409).json({ error: 'no plan awaiting approval' });

  const approval = executor.approve(incident, plan, optionId);
  if (!approval.ok) {
    const status = approval.code === 'UNKNOWN_OPTION' ? 404 : 409;
    return res.status(status).json({ error: approval.reason, code: approval.code });
  }

  const outcome = await runAction(optionId, 'approved by operator');
  if (outcome.record) {
    // A retried approval returns the record of what already happened rather than doing it
    // again — idempotent from the console's point of view, and explicit about which it was.
    const duplicate = outcome.refusal?.code === 'ALREADY_EXECUTED';
    return res.status(outcome.record.outcome === 'APPLIED' ? 200 : 502)
      .json({ ok: outcome.record.outcome === 'APPLIED', optionId, duplicate, actionRecord: outcome.record });
  }
  return res.status(409).json({ error: outcome.refusal.reason, code: outcome.refusal.code });
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
    executor: executor.stats(),
    verifier: verifier.status(),
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
let verifyCursor = null;
let verifyDeadline = 0;

const detector = createDetector();
const scorer = createScorer();
const experimenter = createExperimenter({ windowMs: tuning.collector.windowMs });
const planner = createPlanner();
const executor = createExecutor();
const verifier = createVerifier();

// POST /reset must be able to stop an experiment from any state.
state.onProbeAbort(() => experimenter.abort());

// Kicked off from the detector tick but deliberately NOT awaited: a probe takes seconds,
// and detection must keep running throughout. Telemetry never stalls behind an experiment.
// An APPLIED action is a claim that the data plane accepted an instruction. Whether
// anything actually got better is a separate question, answered only by telemetry — so the
// Verifier starts here and reads nothing from the record but which action to watch.
function beginVerification(record) {
  if (record.outcome !== 'APPLIED') return;
  const { incident, plan } = state.get();
  const option = plan?.options.find((o) => o.id === record.optionId);
  const started = verifier.start({
    incident, actionRecord: record, predicted: option?.predicted, telemetry,
  });
  if (!started.ok) {
    console.log(`[control] verification not started: ${started.reason}`);
    // Some refusals are themselves a conclusion — an unverifiable prediction, for
    // instance. Publish it rather than leaving the console with a silent gap.
    if (started.verdict) {
      state.update({ verdict: started.verdict },
        `verdict ${started.verdict.actionId} ${started.verdict.verdict}: ${started.verdict.reason}`);
    }
    return;
  }
  verifyCursor = null;
  verifyDeadline = Date.now() + supervisor.AGENTS.verifier.timeoutMs;
  state.update({ plan: { ...state.get().plan, verification: started.verification } },
    `verifying ${record.actionId}`);
}

// Driven from the telemetry tick rather than a blocking await, so verification cannot stall
// detection — and a stalled stream ends as INCONCLUSIVE rather than hanging forever.
function pumpVerification() {
  if (!verifier.isVerifying()) return;
  const windows = telemetry.since(verifyCursor);
  if (windows.length) verifyCursor = windows[windows.length - 1].windowId;

  let verdict = verifier.observe(windows);
  if (!verdict && Date.now() > verifyDeadline) {
    verdict = verifier.timeout(`no verdict within the ${supervisor.AGENTS.verifier.timeoutMs}ms verification budget`);
  }
  if (verdict) {
    state.update({ verdict, plan: { ...state.get().plan, verification: null } },
      `verdict ${verdict.actionId} ${verdict.verdict}: ${verdict.reason}`);
    return;
  }
  const view = verifier.current();
  if (view) state.update({ plan: { ...state.get().plan, verification: view } },
    `verifying ${view.actionId} (${view.observed}/${view.needed})`);
}

// One execution path, whether the trigger was a human approval or the autonomy gate.
// Runs under the supervisor's executor budget: a hang becomes EXECUTION_FAILED rather
// than a stalled control plane.
async function runAction(optionId, why) {
  const { incident, plan } = state.get();
  const run = await supervisor.run('executor', () => executor.execute({
    incident, plan, optionId,
    publish: async (pending) => {
      state.update({ plan: { ...state.get().plan, executing: pending } },
        `executing ${pending.actionType} on ${pending.target} (${why})`);
    },
  }));

  // The supervisor exhausted its budget. The Executor never claims success, and neither
  // does the record we write on its behalf.
  if (!run.ok) {
    const record = {
      actionId: `ACT-${incident?.id ?? 'UNKNOWN'}-${optionId}`,
      optionId, issuedAt: Date.now(), target: plan?.options.find((o) => o.id === optionId)?.target ?? 'unknown',
      httpStatus: null, outcome: 'EXECUTION_FAILED',
      error: `executor exceeded its ${supervisor.AGENTS.executor.timeoutMs}ms budget`,
    };
    state.update({ plan: { ...state.get().plan, executing: null, action: record } }, `executor budget exceeded for ${optionId}`);
    return { record, refusal: null };
  }

  const { record, refusal } = run.value;
  if (record && refusal) return { record, refusal };   // already executed: nothing to re-broadcast
  if (record) {
    state.update({ plan: { ...state.get().plan, executing: null, action: record } },
      `action ${record.actionId} ${record.outcome}`);
    beginVerification(record);
  } else {
    state.update({ plan: { ...state.get().plan, executing: null } }, `execution refused: ${refusal.reason}`);
  }
  return { record, refusal };
}

// An AUTONOMOUS action may run without a human, but only the one the Planner actually
// recommends. Acting on a non-recommended option — mitigating while a human is still
// considering the fix — is not something the specification authorises.
async function maybeAutonomous() {
  const { incident, plan } = state.get();
  if (!incident || !plan || !plan.recommendedOptionId) return;
  const option = plan.options.find((o) => o.id === plan.recommendedOptionId);
  if (!option || option.autonomy !== 'AUTONOMOUS') return;
  if (!executor.eligible(incident, plan, option.id).ok) return;
  console.log(`[control] autonomous execution of ${option.id} (${option.gateReason})`);
  await runAction(option.id, 'autonomy gate');
}

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
  await maybeAutonomous();
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
  pumpVerification();
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
