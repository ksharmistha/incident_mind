'use strict';

const express = require('express');
const http = require('node:http');

const { PORTS, SERVICES, validateIngestBatch, validateIngestAck, validateWindowAggregate } = require('../packages/contracts');
const tuning = require('../config/tuning.json');
const { Admission } = require('./admission');
const { Dedup } = require('./dedup');
const { WindowStore } = require('./windows');
const { Aggregator } = require('./aggregate');
const { Graph } = require('./graph');
const { observationConfidence } = require('./confidence');
const { Fanout } = require('./fanout');

// The pipeline, end to end:
//   admission + shed -> dedup -> 1s event-time windows -> aggregate + graph
//   -> confidence -> WS fan-out
// The shed level computed at admission rides back to the emitters in the 202 body, which
// is the loop that keeps the queue bounded under a flood.

const cfg = tuning.collector;

const admission = new Admission({ queueMax: cfg.queueMax, shedThresholds: cfg.shedThresholds });
const dedup = new Dedup(cfg.dedupLru);
const windows = new WindowStore({
  windowMs: cfg.windowMs,
  watermarkLagMs: cfg.watermarkLagMs,
  latenessMs: cfg.latenessMs,
});
const aggregator = new Aggregator({ reservoirSize: cfg.reservoirSize });
const graph = new Graph();

// Per-window tallies of what the emitters told us they discarded, plus what we discarded
// at admission. Both feed the shed rate in observation confidence.
let offered = { pri1: 0, pri2: 0 };
let droppedThisWindow = { pri0: 0, pri1: 0, pri2: 0 };
let lastAdmissionDropped = { pri0: 0, pri1: 0, pri2: 0 };
const reportingServices = new Set();

const app = express();
app.use(express.json({ limit: '8mb' }));

// Always 202, never 5xx, even while shedding. A 5xx here would make every emitter retry
// at exactly the moment the pipeline is least able to absorb it.
app.post('/ingest', (req, res) => {
  const batch = req.body;
  validateIngestBatch(batch);

  const events = Array.isArray(batch?.events) ? batch.events : [];
  for (const evt of events) {
    evt.svc = batch.svc;
    if (evt.pri === 1) offered.pri1++;
    if (evt.pri === 2) offered.pri2++;
  }
  if (batch?.svc) reportingServices.add(batch.svc);

  const fresh = dedup.filter(events);
  const { accepted, rejected } = admission.admit(fresh);

  const ack = {
    accepted,
    rejected,
    queueDepth: admission.depth,
    shedLevel: admission.shedLevel,
    pri1SampleRate: admission.pri1SampleRate,
    watermarkLagMs: Math.round(windows.watermarkLag()),
  };
  validateIngestAck(ack);
  res.status(202).json(ack);
});

app.get('/health', (req, res) => {
  res.json({
    svc: 'collector',
    up: true,
    version: null,
    faults: [],
    queueDepth: admission.depth,
    shedLevel: admission.shedLevel,
    windowId: lastWindowId,
    subscribers: fanout ? fanout.subscribers : 0,
  });
});

// Operator-only. The control plane never calls this.
app.post('/chaos/reset', (req, res) => {
  admission.reset();
  dedup.reset();
  windows.reset();
  aggregator.reset();
  graph.reset();
  offered = { pri1: 0, pri2: 0 };
  droppedThisWindow = { pri0: 0, pri1: 0, pri2: 0 };
  lastAdmissionDropped = { pri0: 0, pri1: 0, pri2: 0 };
  reportingServices.clear();
  res.json({ ok: true });
});

const server = http.createServer(app);
const fanout = new Fanout(server, '/stream');
let lastWindowId = null;

// Drain admission into the window store continuously. Event time decides the bucket, so
// draining rate does not affect which window an event lands in.
function drainIntoWindows() {
  const events = admission.drain(5000);
  for (const evt of events) windows.add(evt);
}

// One aggregate per second, whether or not the window had events: P2's detector runs on
// its own interval and must never stall waiting for us.
function tick() {
  drainIntoWindows();

  const before = { ...admission.dropped };
  const closed = windows.advanceWatermark();
  const ingestRate = admission.sampleRate();

  for (const pri of ['pri0', 'pri1', 'pri2']) {
    droppedThisWindow[pri] += before[pri] - lastAdmissionDropped[pri];
  }
  lastAdmissionDropped = before;

  if (closed.length === 0) return;

  // Drops and offers are counted in real time, but a window is only closed about four
  // seconds after the events in it happened, and several windows can become closable in
  // the same tick. Attributing the whole accumulated total to whichever window happens to
  // close first would make the numbers alternate between a large value and zero — and the
  // confidence gauge would visibly flicker once a second while telling the truth on
  // average. Spreading the interval's totals across the windows it covers reports the
  // same aggregate quantity with the timing artefact removed.
  const share = closed.length;
  const perWindowDropped = {
    pri0: Math.round(droppedThisWindow.pri0 / share),
    pri1: Math.round(droppedThisWindow.pri1 / share),
    pri2: Math.round(droppedThisWindow.pri2 / share),
  };
  const perWindowOffered = { pri1: offered.pri1 / share, pri2: offered.pri2 / share };

  for (const bucket of closed) {
    const { services, edges, resources } = aggregator.build(bucket, SERVICES);
    graph.observe(bucket.windowId, Object.keys(edges));

    const silent = SERVICES.filter((s) => services[s].state === 'UNKNOWN').length;
    const watermarkLagMs = Math.round(windows.watermarkLag());
    const oc = observationConfidence({
      droppedPri1: perWindowDropped.pri1,
      droppedPri2: perWindowDropped.pri2,
      offeredPri1: perWindowOffered.pri1,
      offeredPri2: perWindowOffered.pri2,
      watermarkLagMs,
      silentServices: silent,
      totalServices: SERVICES.length,
    });

    const aggregate = {
      windowId: bucket.windowId,
      tStart: bucket.tStart,
      tEnd: bucket.tEnd,
      closedAt: Date.now(),
      complete: true,
      services,
      edges,
      resources,
      pipeline: {
        ingestRate,
        queueDepth: admission.depth,
        shedLevel: admission.shedLevel,
        dropped: { ...perWindowDropped },
        watermarkLagMs,
        dedupHits: dedup.hits,
        lateDropped: windows.lateDropped,
      },
      observationConfidence: oc.value,
      observationTerms: oc.terms,
    };

    validateWindowAggregate(aggregate);
    fanout.broadcast(aggregate);
    lastWindowId = bucket.windowId;
  }

  offered = { pri1: 0, pri2: 0 };
  droppedThisWindow = { pri0: 0, pri1: 0, pri2: 0 };
}

setInterval(tick, 250).unref();

setInterval(() => {
  console.log(
    `win=${lastWindowId ?? '-'} ingest=${admission.ingestRate}/s q=${admission.depth} shed=${admission.shedLevel} ` +
      `dropped p0=${admission.dropped.pri0} p1=${admission.dropped.pri1} p2=${admission.dropped.pri2} ` +
      `lag=${Math.round(windows.watermarkLag())}ms dedup=${dedup.hits} late=${windows.lateDropped} subs=${fanout.subscribers}`
  );
}, 1000).unref();

server.listen(PORTS.collector, '127.0.0.1', () =>
  console.log(`collector up on 127.0.0.1:${PORTS.collector} (ws /stream)`)
);
