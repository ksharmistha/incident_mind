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

supervisor.startInterval('detector', tuning.collector.windowMs, () => {
  // Every window since the detector's cursor, not just the newest: the detector runs on
  // its own clock and must not lose EWMA samples when the two cadences drift.
  let incident = null;
  for (const w of telemetry.since(detector.stats().lastWindowId)) {
    incident = detector.observe(w);
  }
  if (!incident) return;
  state.update({ incident }, `detector: ${incident.id} @ window ${incident.lastWindowId}`);
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
