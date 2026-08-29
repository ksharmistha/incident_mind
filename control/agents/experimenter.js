'use strict';

// Experimenter — the active causal probe. The signature feature.
//
// When passive scoring cannot separate two hypotheses, this publishes what it expects to
// see, performs a bounded reversible intervention on the live system, measures the result,
// and updates belief. Correlation becomes causation by intervention.
//
// Sequence, from P2 handoff §11.2. Step 3 is never reordered:
//   1  refuse if the margin is already decisive, or we are too blind to measure, or a
//      probe is in flight, or the edge's breaker is open
//   2  build the Probe with magnitude-class predictions
//   3  PUBLISH it, predictions and publishedAt included, BEFORE executing
//   4  capture the baseline from the most recent complete windows
//   5  POST /admin/probe — bounded fraction, bounded duration, server-side auto-expiry
//   6  wait for probe end plus settling, then read the windows that overlapped the probe
//   7  measuredDeltaPct against the baseline
//   8  posteriors, or INCONCLUSIVE with posteriors unchanged
//
// Why the level of selfP99 cannot answer this on its own: under a downstream fault the
// probed service's self time is also elevated, by queueing behind a saturated dependency.
// What separates the two is how self time RESPONDS when load is removed. That response is
// only observable by intervening.

const { validateProbe, validateProbeResult, parseEdgeKey, MAX_PROBE_FRACTION, MAX_PROBE_DURATION_MS } =
  require('../adapters/contracts');
const { control, derived } = require('../adapters/tuning');
const admin = require('../adapters/admin');

const PROBE_FRACTION = MAX_PROBE_FRACTION;      // the rail is also the operating point
const PROBE_DURATION_MS = MAX_PROBE_DURATION_MS;

function createExperimenter(opts = {}) {
  const marginThreshold = opts.probeMarginThreshold ?? control.probeMarginThreshold;
  const minConfidence = opts.probeMinObservationConfidence ?? derived.probeMinObservationConfidence;
  const h1MinDrop = opts.probeH1MinDropPct ?? derived.probeH1MinDropPct;
  const h2MaxDrop = opts.probeH2MaxDropPct ?? derived.probeH2MaxDropPct;
  const settlingWindows = opts.probeSettlingWindows ?? derived.probeSettlingWindows;
  const baselineCount = opts.probeBaselineWindows ?? derived.probeBaselineWindows;
  const windowMs = opts.windowMs ?? 1000;
  const adminClient = opts.admin ?? admin;

  let inFlight = null;          // the Probe currently executing, or null
  let aborted = false;
  let probeSeq = 0;

  // -------------------------------------------------------------------------
  // Step 1 — the gate. Deciding not to experiment is a feature.
  // -------------------------------------------------------------------------
  function shouldProbe(hypotheses, observationConfidence, incident) {
    if (inFlight) {
      return { probe: false, reason: `probe ${inFlight.id} already in flight` };
    }
    if (!incident) {
      return { probe: false, reason: 'no active incident' };
    }
    if (!Array.isArray(hypotheses) || hypotheses.length < 2) {
      return { probe: false, reason: 'fewer than two hypotheses to discriminate' };
    }
    const margin = hypotheses[0].score - hypotheses[1].score;
    if (!(margin < marginThreshold)) {
      return {
        probe: false,
        reason: `margin ${round3(margin)} >= ${marginThreshold} — observation already separates them`,
      };
    }
    if (!(typeof observationConfidence === 'number' && observationConfidence >= minConfidence)) {
      return {
        probe: false,
        reason: `observationConfidence ${observationConfidence} < ${minConfidence} — too blind to measure a delta`,
      };
    }
    const design = designProbe(hypotheses, incident);
    if (!design.ok) return { probe: false, reason: design.reason };
    if (adminClient.isBreakerOpen(design.target, design.upstream)) {
      return { probe: false, reason: `breaker already open on ${design.target}→${design.upstream}` };
    }
    return { probe: true, reason: `margin ${round3(margin)} < ${marginThreshold}`, design, margin };
  }

  // -------------------------------------------------------------------------
  // Step 2 — design. Derived from the graph, never from a service name.
  //
  // The top two hypotheses are only separable by experiment when one of them names a
  // service that CALLS the other's service. Then shedding load off the caller answers a
  // single question: was the caller's own time the constraint, or its dependency's?
  // -------------------------------------------------------------------------
  function designProbe(hypotheses, incident) {
    const [h1, h2] = hypotheses;
    const edges = (incident && incident.edges) || {};
    const calls = new Set();
    for (const key of Object.keys(edges)) {
      const e = parseEdgeKey(key);
      if (e) calls.add(`${e.from}→${e.to}`);
    }

    // Which of the pair is the caller of the other?
    let caller = null, callee = null;
    if (calls.has(`${h1.rootCauseService}→${h2.rootCauseService}`)) { caller = h1; callee = h2; }
    else if (calls.has(`${h2.rootCauseService}→${h1.rootCauseService}`)) { caller = h2; callee = h1; }
    if (!caller) {
      return {
        ok: false,
        reason: `no discriminating intervention: ${h1.rootCauseService} and ${h2.rootCauseService} do not call each other`,
      };
    }

    // Shed the caller's own busiest amplified inbound edge. Retry amplification is the
    // signal that this edge carries the load worth removing.
    let best = null;
    for (const [key, edge] of Object.entries(edges)) {
      const e = parseEdgeKey(key);
      if (!e || e.to !== caller.rootCauseService) continue;
      const amp = Number(edge?.amplification);
      if (!Number.isFinite(amp)) continue;
      if (!best || amp > best.amplification) best = { from: e.from, amplification: amp };
    }
    if (!best) {
      return { ok: false, reason: `no inbound edge into ${caller.rootCauseService} to shed` };
    }

    return {
      ok: true,
      target: best.from,                       // the service we POST /admin/probe to
      upstream: caller.rootCauseService,       // whose calls get shed
      amplification: best.amplification,
      selfHypothesis: caller,                  // its own time is the constraint
      dependencyHypothesis: callee,            // its dependency is the constraint
      discriminator: `${caller.rootCauseService}.selfP99`,
    };
  }

  function buildProbe(design, baselineWindows, publishedAt) {
    probeSeq += 1;
    return {
      id: `P${probeSeq}`,
      forHypotheses: [design.selfHypothesis.id, design.dependencyHypothesis.id],
      intervention: {
        target: design.target,
        action: 'shed',
        upstream: design.upstream,
        fraction: PROBE_FRACTION,
        durationMs: PROBE_DURATION_MS,
      },
      discriminator: design.discriminator,
      predictions: {
        // If the shed service's own time was the constraint, removing load unsaturates it.
        [design.selfHypothesis.id]: { direction: 'drop', magnitude: `>${pct(h1MinDrop)}` },
        // If its dependency was the constraint, self time was never the bottleneck.
        [design.dependencyHypothesis.id]: { direction: 'drop', magnitude: `<${pct(h2MaxDrop)}` },
      },
      baselineWindows: baselineWindows.map((w) => w.windowId),
      measureWindows: [],
      publishedAt,
    };
  }

  // -------------------------------------------------------------------------
  // Steps 3–8. `publish` is called with the Probe BEFORE the intervention fires.
  // -------------------------------------------------------------------------
  async function run({ hypotheses, incident, observationConfidence, telemetry, publish, now = Date.now }) {
    const gate = shouldProbe(hypotheses, observationConfidence, incident);
    if (!gate.probe) {
      console.log(`[experimenter] no probe: ${gate.reason}`);
      return { probe: null, result: null, skipped: gate.reason };
    }

    const design = gate.design;

    // Step 4 — baseline from the most recent complete windows, captured before publishing
    // so the numbers the prediction is judged against are already fixed.
    const baseline = telemetry.recent(baselineCount).filter((w) => w && w.complete !== false);
    const baselineSelfP99 = meanSelfP99(baseline, design.upstream);
    if (baselineSelfP99 === null || baselineSelfP99 <= 0) {
      return { probe: null, result: null, skipped: `no usable baseline for ${design.discriminator}` };
    }

    const publishedAt = now();
    const probe = buildProbe(design, baseline, publishedAt);

    if (!validateProbe(probe)) {
      return { probe: null, result: null, skipped: 'probe failed contract validation — not executed' };
    }

    // Step 3 — PUBLISH BEFORE EXECUTING. This ordering is the falsifiability claim: the
    // predictions and their timestamp are on the console before anything is done to the
    // live system, so the winner cannot have been chosen after seeing the result.
    // Nothing may be inserted between this call and the POST below.
    inFlight = probe;
    aborted = false;
    await publish(probe);
    console.log(`[experimenter] ${probe.id} PUBLISHED at ${publishedAt} — shed ${probe.intervention.fraction} of ` +
      `${probe.intervention.target}→${probe.intervention.upstream} for ${probe.intervention.durationMs}ms, ` +
      `discriminator ${probe.discriminator}, baseline ${round1(baselineSelfP99)}ms`);

    // Step 5 — execute.
    //
    // Anchor the measurement to the STREAM's clock, not ours. Windows are closed on event
    // time and arrive a watermark behind, so comparing their tStart against the control
    // plane's Date.now() silently selects the wrong windows whenever the two clocks are
    // offset — which is every real run, because the collector publishes a window only
    // after its watermark passes.
    const anchor = telemetry.recent(1)[0]?.tEnd ?? now();
    const res = await adminClient.probe(probe.intervention.target, {
      probeId: probe.id,
      upstream: probe.intervention.upstream,
      fraction: probe.intervention.fraction,
      durationMs: probe.intervention.durationMs,
    });

    if (!res.ok) {
      inFlight = null;
      const result = inconclusive(probe, hypotheses, `intervention failed: ${res.error || `HTTP ${res.httpStatus}`}`);
      return { probe, result, skipped: null };
    }

    // Step 6 — the probe expires server-side; wait for it plus settling so the windows that
    // overlapped it have actually been closed and delivered by the collector.
    const waitMs = probe.intervention.durationMs + settlingWindows * windowMs;
    await sleep(waitMs);

    if (aborted) {
      inFlight = null;
      return { probe, result: inconclusive(probe, hypotheses, 'probe aborted before measurement'), skipped: null };
    }

    // Step 7 — measure the windows whose EVENT TIME overlapped the intervention.
    const probeEndEventTime = anchor + probe.intervention.durationMs;
    const measure = telemetry.all().filter(
      (w) => w && typeof w.tStart === 'number' && w.tStart >= anchor && w.tStart < probeEndEventTime
    );
    const measureSelfP99 = meanSelfP99(measure, design.upstream);
    inFlight = null;

    if (measureSelfP99 === null) {
      return { probe, result: inconclusive(probe, hypotheses, 'no windows covered the probe interval'), skipped: null };
    }

    probe.measureWindows = measure.map((w) => w.windowId);
    const measuredDeltaPct = round1(((measureSelfP99 - baselineSelfP99) / baselineSelfP99) * 100);
    const dropPct = -measuredDeltaPct / 100;

    // Step 8 — classify into the published magnitude classes, and only those.
    let matched = null;
    if (dropPct > h1MinDrop) matched = design.selfHypothesis.id;
    else if (dropPct < h2MaxDrop) matched = design.dependencyHypothesis.id;

    const result = {
      probeId: probe.id,
      measuredDeltaPct,
      matched,
      posteriors: matched
        ? posteriorsFor(hypotheses, matched)
        : unchangedPosteriors(hypotheses),
      inconclusive: matched === null,
    };
    if (matched === null) result.reason =
      `drop ${pct(dropPct)} falls between the published classes (${pct(h2MaxDrop)}..${pct(h1MinDrop)})`;

    validateProbeResult(result);
    console.log(`[experimenter] ${probe.id} measured ${measuredDeltaPct}% ` +
      `(baseline ${round1(baselineSelfP99)}ms -> ${round1(measureSelfP99)}ms over ${measure.length} windows) -> ` +
      `${matched || 'INCONCLUSIVE'}`);
    return { probe, result, skipped: null };
  }

  // A probe that cannot be measured must leave belief exactly where it was. Refusing to
  // update on a measurement you cannot trust is a real outcome, not a failure to hide.
  function inconclusive(probe, hypotheses, reason) {
    const result = {
      probeId: probe.id,
      measuredDeltaPct: 0,
      matched: null,
      posteriors: unchangedPosteriors(hypotheses),
      inconclusive: true,
      reason,
    };
    validateProbeResult(result);
    console.warn(`[experimenter] ${probe.id} INCONCLUSIVE: ${reason} — posteriors unchanged, escalating`);
    return result;
  }

  // Stops measurement and clears the in-flight slot. The intervention itself is already
  // bounded server-side, so the data plane recovers whether or not we are here to ask.
  function abort() {
    if (!inFlight) return null;
    const id = inFlight.id;
    aborted = true;
    inFlight = null;
    console.warn(`[experimenter] ${id} aborted — the intervention still expires on its own server-side timer`);
    return id;
  }

  function reset() {
    abort();
    probeSeq = 0;
    aborted = false;
  }

  return {
    shouldProbe, designProbe, buildProbe, run, abort, reset,
    isInFlight: () => inFlight !== null,
    current: () => inFlight,
  };
}

function posteriorsFor(hypotheses, matchedId) {
  const out = {};
  for (const h of hypotheses) {
    out[h.id] = h.id === matchedId ? derived.posteriorMatched : derived.posteriorOther;
  }
  return out;
}

function unchangedPosteriors(hypotheses) {
  const out = {};
  for (const h of hypotheses) out[h.id] = h.posterior;
  return out;
}

function meanSelfP99(windows, service) {
  const values = (windows || [])
    .map((w) => w?.services?.[service]?.selfP99)
    .filter((v) => typeof v === 'number' && Number.isFinite(v));
  if (values.length === 0) return null;
  return values.reduce((a, b) => a + b, 0) / values.length;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const pct = (f) => `${Math.round(f * 100)}%`;
const round1 = (v) => Math.round(v * 10) / 10;
const round3 = (v) => Math.round(v * 1000) / 1000;

module.exports = { createExperimenter, PROBE_FRACTION, PROBE_DURATION_MS };
