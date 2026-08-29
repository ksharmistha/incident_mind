'use strict';

// Frozen contracts for IncidentMind V2.
//
// P1 writes this file; both planes import it. The critical seam is WindowAggregate:
// P1's collector produces one per second, P2's control plane and console consume it.
// Changing a shape here is a stop-the-line event agreed by both people in one commit,
// never a quiet edit.
//
// Two tiers of API, on purpose:
//   xProblems(obj)  pure, always runs, returns an array of problem strings (empty = valid).
//                   Use it in tests and harnesses where you want to see what was wrong.
//   validateX(obj)  gated on IM_VALIDATE=1. Returns a boolean and logs loudly on failure.
//                   Safe in the request path: it short-circuits to true when the flag is
//                   off, and it never throws — a validation failure must not become an
//                   outage in the system whose outages we are trying to observe.

const validationEnabled = process.env.IM_VALIDATE === '1';

const PORTS = {
  gateway: 4000,
  auth: 4001,
  checkout: 4002,
  payments: 4003,
  datastore: 4004,
  collector: 4100,
  control: 4200,
  loadgen: 4300,
  console: 5173,
};

const SERVICES = ['gateway', 'auth', 'checkout', 'payments', 'datastore'];
const SERVICE_STATES = ['HEALTHY', 'DEGRADED', 'CRITICAL', 'UNKNOWN'];
const SHED_LEVELS = [0, 1, 2, 3];
const EVENT_KINDS = ['counters', 'state', 'probe', 'sample', 'log'];

// Priority is fixed per kind rather than chosen per event. This is what makes
// "counts and error rates are exact at every shed level" true by construction:
// counters/state/probe are discrete facts on pri-0 and are never dropped, samples
// are pri-1 and get sampled away under pressure, logs are pri-2 and go first.
const KIND_PRIORITY = { counters: 0, state: 0, probe: 0, sample: 1, log: 2 };

// Probe safety rails. Deliberately here and not in config/tuning.json: these are
// bounds on what the control plane is permitted to do to a live system, not knobs
// to be widened during calibration.
const MAX_PROBE_FRACTION = 0.2;
const MAX_PROBE_DURATION_MS = 5000;

const ACTION_TYPES = ['rollback', 'isolate', 'restart'];
const AUTONOMY_LEVELS = ['AUTONOMOUS', 'HUMAN', 'BLOCKED'];
const ACTION_OUTCOMES = ['APPLIED', 'EXECUTION_FAILED'];
const VERDICTS = ['RECOVERED', 'PARTIAL', 'FAILED', 'INCONCLUSIVE'];

// Timestamps are ms since epoch stamped at enqueue. Anything below this is a
// monotonic clock reading or a duration that leaked into a timestamp field.
const MIN_EPOCH_MS = 1e12;

// ---------------------------------------------------------------------------
// check helpers — each pushes a problem and reports whether the value was usable,
// so callers can skip dependent checks instead of cascading noise.
// ---------------------------------------------------------------------------

function show(v) {
  if (typeof v === 'string') return JSON.stringify(v);
  if (typeof v === 'number' || typeof v === 'boolean' || v === null) return String(v);
  if (v === undefined) return 'undefined';
  if (Array.isArray(v)) return `array(${v.length})`;
  return typeof v;
}

function isPlainObject(v) {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

function num(problems, path, v, opts = {}) {
  if (typeof v !== 'number' || !Number.isFinite(v)) {
    problems.push(`${path}: expected a finite number, got ${show(v)}`);
    return false;
  }
  if (opts.int && !Number.isInteger(v)) {
    problems.push(`${path}: expected an integer, got ${v}`);
    return false;
  }
  if (opts.min !== undefined && v < opts.min) {
    problems.push(`${path}: expected >= ${opts.min}, got ${v}`);
    return false;
  }
  if (opts.max !== undefined && v > opts.max) {
    problems.push(`${path}: expected <= ${opts.max}, got ${v}`);
    return false;
  }
  return true;
}

function str(problems, path, v) {
  if (typeof v !== 'string' || v.length === 0) {
    problems.push(`${path}: expected a non-empty string, got ${show(v)}`);
    return false;
  }
  return true;
}

function bool(problems, path, v) {
  if (typeof v !== 'boolean') {
    problems.push(`${path}: expected a boolean, got ${show(v)}`);
    return false;
  }
  return true;
}

function oneOf(problems, path, v, allowed) {
  if (!allowed.includes(v)) {
    problems.push(`${path}: expected one of ${allowed.map(show).join(', ')}, got ${show(v)}`);
    return false;
  }
  return true;
}

function object(problems, path, v) {
  if (!isPlainObject(v)) {
    problems.push(`${path}: expected an object, got ${show(v)}`);
    return false;
  }
  return true;
}

function array(problems, path, v) {
  if (!Array.isArray(v)) {
    problems.push(`${path}: expected an array, got ${show(v)}`);
    return false;
  }
  return true;
}

function stringArray(problems, path, v) {
  if (!array(problems, path, v)) return false;
  let ok = true;
  v.forEach((item, i) => {
    if (typeof item !== 'string') {
      problems.push(`${path}[${i}]: expected a string, got ${show(item)}`);
      ok = false;
    }
  });
  return ok;
}

// ---------------------------------------------------------------------------
// Wire events — emitter -> collector (POST /ingest)
// ---------------------------------------------------------------------------

function countersProblems(problems, path, evt) {
  if (object(problems, `${path}.req`, evt.req)) {
    num(problems, `${path}.req.in`, evt.req.in, { int: true, min: 0 });
    num(problems, `${path}.req.ok`, evt.req.ok, { int: true, min: 0 });
    num(problems, `${path}.req.err`, evt.req.err, { int: true, min: 0 });
  }
  if (array(problems, `${path}.edges`, evt.edges)) {
    evt.edges.forEach((edge, i) => {
      const at = `${path}.edges[${i}]`;
      if (!object(problems, at, edge)) return;
      str(problems, `${at}.to`, edge.to);
      str(problems, `${at}.op`, edge.op);
      const callsOk = num(problems, `${at}.calls`, edge.calls, { int: true, min: 0 });
      const attemptsOk = num(problems, `${at}.attempts`, edge.attempts, { int: true, min: 0 });
      num(problems, `${at}.timeouts`, edge.timeouts, { int: true, min: 0 });
      // attempts counts every network attempt including retries, so it can never be
      // below calls. If it is, the amplification headline is being fed by a counter bug.
      if (callsOk && attemptsOk && edge.attempts < edge.calls) {
        problems.push(`${at}: attempts (${edge.attempts}) < calls (${edge.calls})`);
      }
    });
  }
  if (evt.resource !== undefined && object(problems, `${path}.resource`, evt.resource)) {
    num(problems, `${path}.resource.poolInUse`, evt.resource.poolInUse, { int: true, min: 0 });
    num(problems, `${path}.resource.queueDepth`, evt.resource.queueDepth, { int: true, min: 0 });
    num(problems, `${path}.resource.queueWaitP99Ms`, evt.resource.queueWaitP99Ms, { min: 0 });
  }
}

function sampleProblems(problems, path, evt) {
  str(problems, `${path}.op`, evt.op);
  const totalOk = num(problems, `${path}.totalMs`, evt.totalMs, { min: 0 });
  const selfOk = num(problems, `${path}.selfMs`, evt.selfMs, { min: 0 });
  const downstreamOk = num(problems, `${path}.downstreamMs`, evt.downstreamMs, { min: 0 });
  num(problems, `${path}.status`, evt.status, { int: true, min: 100, max: 599 });
  // selfMs = totalMs - sum(downstreamMs). It is the probe's discriminator, so a service
  // that computes it wrong breaks the signature feature silently rather than loudly.
  // 1ms of tolerance for rounding; anything larger is an accounting error.
  if (totalOk && selfOk && downstreamOk) {
    const expected = evt.totalMs - evt.downstreamMs;
    if (Math.abs(evt.selfMs - expected) > 1) {
      problems.push(`${path}.selfMs: expected totalMs - downstreamMs = ${expected}, got ${evt.selfMs}`);
    }
  }
}

function eventProblems(evt, path = 'event') {
  const problems = [];
  if (!object(problems, path, evt)) return problems;

  num(problems, `${path}.seq`, evt.seq, { int: true, min: 0 });
  num(problems, `${path}.t`, evt.t, { min: MIN_EPOCH_MS });
  oneOf(problems, `${path}.pri`, evt.pri, [0, 1, 2]);
  const kindOk = oneOf(problems, `${path}.kind`, evt.kind, EVENT_KINDS);
  if (!kindOk) return problems;

  if (evt.pri !== KIND_PRIORITY[evt.kind]) {
    problems.push(
      `${path}.pri: kind "${evt.kind}" must ride pri-${KIND_PRIORITY[evt.kind]}, got ${show(evt.pri)}`
    );
  }

  switch (evt.kind) {
    case 'counters':
      countersProblems(problems, path, evt);
      break;
    case 'state':
      oneOf(problems, `${path}.from`, evt.from, SERVICE_STATES);
      oneOf(problems, `${path}.to`, evt.to, SERVICE_STATES);
      break;
    case 'probe':
      str(problems, `${path}.probeId`, evt.probeId);
      oneOf(problems, `${path}.phase`, evt.phase, ['start', 'end']);
      break;
    case 'sample':
      sampleProblems(problems, path, evt);
      break;
    case 'log':
      str(problems, `${path}.level`, evt.level);
      str(problems, `${path}.msg`, evt.msg);
      break;
  }
  return problems;
}

function ingestBatchProblems(batch) {
  const problems = [];
  if (!object(problems, 'batch', batch)) return problems;
  oneOf(problems, 'batch.svc', batch.svc, SERVICES);
  str(problems, 'batch.batchId', batch.batchId);
  if (array(problems, 'batch.events', batch.events)) {
    batch.events.forEach((evt, i) => {
      problems.push(...eventProblems(evt, `batch.events[${i}]`));
    });
  }
  return problems;
}

// The 202 body — the backpressure signal the emitters obey on their next flush.
function ingestAckProblems(ack) {
  const problems = [];
  if (!object(problems, 'ack', ack)) return problems;
  num(problems, 'ack.accepted', ack.accepted, { int: true, min: 0 });
  num(problems, 'ack.rejected', ack.rejected, { int: true, min: 0 });
  num(problems, 'ack.queueDepth', ack.queueDepth, { int: true, min: 0 });
  oneOf(problems, 'ack.shedLevel', ack.shedLevel, SHED_LEVELS);
  num(problems, 'ack.pri1SampleRate', ack.pri1SampleRate, { min: 0, max: 1 });
  num(problems, 'ack.watermarkLagMs', ack.watermarkLagMs, { min: 0 });
  return problems;
}

// ---------------------------------------------------------------------------
// WindowAggregate — collector -> everyone. THE critical seam.
// ---------------------------------------------------------------------------

function aggregateServiceProblems(problems, path, svc) {
  if (!object(problems, path, svc)) return;
  num(problems, `${path}.rps`, svc.rps, { min: 0 });
  num(problems, `${path}.errRate`, svc.errRate, { min: 0, max: 1 });
  num(problems, `${path}.inflight`, svc.inflight, { int: true, min: 0 });

  const quantiles = ['p50', 'p95', 'p99', 'selfP99', 'downstreamP99'];
  const ok = {};
  for (const q of quantiles) ok[q] = num(problems, `${path}.${q}`, svc[q], { min: 0 });

  // An unordered quantile set means the reservoir rollup is wrong, and every latency
  // number on the console downstream of it is wrong with it.
  if (ok.p50 && ok.p95 && svc.p50 > svc.p95) {
    problems.push(`${path}: p50 (${svc.p50}) > p95 (${svc.p95})`);
  }
  if (ok.p95 && ok.p99 && svc.p95 > svc.p99) {
    problems.push(`${path}: p95 (${svc.p95}) > p99 (${svc.p99})`);
  }

  num(problems, `${path}.sampleCount`, svc.sampleCount, { int: true, min: 0 });
  num(problems, `${path}.inclusionProbability`, svc.inclusionProbability, { min: 0, max: 1 });
  oneOf(problems, `${path}.state`, svc.state, SERVICE_STATES);
  if (svc.firstDegradedAt !== null) {
    num(problems, `${path}.firstDegradedAt`, svc.firstDegradedAt, { min: MIN_EPOCH_MS });
  }
}

function aggregateEdgeProblems(problems, path, key, edge) {
  // Edge keys are "from→to" with U+2192. P2 splits on it, so a stray "->" here
  // breaks her graph lookups silently rather than loudly.
  if (!key.includes('→')) {
    problems.push(`${path}: edge key must be "from→to", got ${show(key)}`);
  }
  if (!object(problems, path, edge)) return;
  const callsOk = num(problems, `${path}.calls`, edge.calls, { int: true, min: 0 });
  const attemptsOk = num(problems, `${path}.attempts`, edge.attempts, { int: true, min: 0 });
  const ampOk = num(problems, `${path}.amplification`, edge.amplification, { min: 0 });
  num(problems, `${path}.timeouts`, edge.timeouts, { int: true, min: 0 });

  if (callsOk && attemptsOk && edge.attempts < edge.calls) {
    problems.push(`${path}: attempts (${edge.attempts}) < calls (${edge.calls})`);
  }
  // The headline number. It is attempts/calls from pri-0 counter deltas and nothing else.
  if (callsOk && attemptsOk && ampOk && edge.calls > 0) {
    const expected = edge.attempts / edge.calls;
    if (Math.abs(edge.amplification - expected) > 0.01) {
      problems.push(
        `${path}.amplification: expected attempts/calls = ${expected.toFixed(3)}, got ${edge.amplification}`
      );
    }
  }
}

function aggregateResourceProblems(problems, path, res) {
  if (!object(problems, path, res)) return;
  const poolSizeOk = num(problems, `${path}.poolSize`, res.poolSize, { int: true, min: 1 });
  const inUseOk = num(problems, `${path}.poolInUse`, res.poolInUse, { int: true, min: 0 });
  num(problems, `${path}.queueDepth`, res.queueDepth, { int: true, min: 0 });
  num(problems, `${path}.queueWaitP99`, res.queueWaitP99, { min: 0 });
  num(problems, `${path}.arrivalRate`, res.arrivalRate, { min: 0 });
  num(problems, `${path}.serviceTimeMs`, res.serviceTimeMs, { min: 0 });
  // utilisation is rho = lambda*W/N. We range-check it but do not re-derive it here:
  // the collector is the single authority on how lambda and W are measured over a window.
  num(problems, `${path}.utilisation`, res.utilisation, { min: 0 });
  if (poolSizeOk && inUseOk && res.poolInUse > res.poolSize) {
    problems.push(`${path}: poolInUse (${res.poolInUse}) > poolSize (${res.poolSize})`);
  }
}

function aggregatePipelineProblems(problems, path, pipeline) {
  if (!object(problems, path, pipeline)) return;
  num(problems, `${path}.ingestRate`, pipeline.ingestRate, { min: 0 });
  num(problems, `${path}.queueDepth`, pipeline.queueDepth, { int: true, min: 0 });
  oneOf(problems, `${path}.shedLevel`, pipeline.shedLevel, SHED_LEVELS);

  if (object(problems, `${path}.dropped`, pipeline.dropped)) {
    const pri0Ok = num(problems, `${path}.dropped.pri0`, pipeline.dropped.pri0, { int: true, min: 0 });
    num(problems, `${path}.dropped.pri1`, pipeline.dropped.pri1, { int: true, min: 0 });
    num(problems, `${path}.dropped.pri2`, pipeline.dropped.pri2, { int: true, min: 0 });
    // The guarantee, enforced structurally: pri-0 rides counter snapshots that are
    // never dropped at any shed level. A non-zero here voids the exactness claim.
    if (pri0Ok && pipeline.dropped.pri0 !== 0) {
      problems.push(
        `${path}.dropped.pri0: must be 0 at every shed level, got ${pipeline.dropped.pri0}`
      );
    }
  }
  num(problems, `${path}.watermarkLagMs`, pipeline.watermarkLagMs, { min: 0 });
  num(problems, `${path}.dedupHits`, pipeline.dedupHits, { int: true, min: 0 });
  // Late events are counted and published, never silently absorbed. Optional until the
  // freeze review decides whether it joins the shape formally.
  if (pipeline.lateDropped !== undefined) {
    num(problems, `${path}.lateDropped`, pipeline.lateDropped, { int: true, min: 0 });
  }
}

function windowAggregateProblems(w) {
  const problems = [];
  if (!object(problems, 'aggregate', w)) return problems;

  num(problems, 'aggregate.windowId', w.windowId, { int: true, min: 0 });
  const startOk = num(problems, 'aggregate.tStart', w.tStart, { min: MIN_EPOCH_MS });
  const endOk = num(problems, 'aggregate.tEnd', w.tEnd, { min: MIN_EPOCH_MS });
  num(problems, 'aggregate.closedAt', w.closedAt, { min: MIN_EPOCH_MS });
  if (startOk && endOk && w.tEnd <= w.tStart) {
    problems.push(`aggregate.tEnd (${w.tEnd}) must be after tStart (${w.tStart})`);
  }
  bool(problems, 'aggregate.complete', w.complete);

  if (object(problems, 'aggregate.services', w.services)) {
    for (const [name, svc] of Object.entries(w.services)) {
      aggregateServiceProblems(problems, `aggregate.services.${name}`, svc);
    }
  }
  if (object(problems, 'aggregate.edges', w.edges)) {
    for (const [key, edge] of Object.entries(w.edges)) {
      aggregateEdgeProblems(problems, `aggregate.edges["${key}"]`, key, edge);
    }
  }
  if (object(problems, 'aggregate.resources', w.resources)) {
    for (const [name, res] of Object.entries(w.resources)) {
      aggregateResourceProblems(problems, `aggregate.resources.${name}`, res);
    }
  }
  aggregatePipelineProblems(problems, 'aggregate.pipeline', w.pipeline);

  num(problems, 'aggregate.observationConfidence', w.observationConfidence, { min: 0, max: 1 });
  if (object(problems, 'aggregate.observationTerms', w.observationTerms)) {
    num(problems, 'aggregate.observationTerms.shedRate', w.observationTerms.shedRate, { min: 0, max: 1 });
    num(problems, 'aggregate.observationTerms.watermarkLag', w.observationTerms.watermarkLag, { min: 0, max: 1 });
    num(problems, 'aggregate.observationTerms.silentServices', w.observationTerms.silentServices, { min: 0, max: 1 });
  }
  return problems;
}

// ---------------------------------------------------------------------------
// Control-plane shapes — P2 produces these. P1 validates them at integration.
// ---------------------------------------------------------------------------

function hypothesisProblems(h, path = 'hypothesis') {
  const problems = [];
  if (!object(problems, path, h)) return problems;
  str(problems, `${path}.id`, h.id);
  str(problems, `${path}.statement`, h.statement);
  str(problems, `${path}.rootCauseService`, h.rootCauseService);
  str(problems, `${path}.failureMode`, h.failureMode);
  num(problems, `${path}.score`, h.score);
  num(problems, `${path}.posterior`, h.posterior, { min: 0, max: 1 });
  if (object(problems, `${path}.terms`, h.terms)) {
    for (const term of ['temporalPrecedence', 'upstreamness', 'amplificationTarget', 'sharedResourcePenalty']) {
      num(problems, `${path}.terms.${term}`, h.terms[term]);
    }
  }
  return problems;
}

function probeProblems(p, path = 'probe') {
  const problems = [];
  if (!object(problems, path, p)) return problems;
  str(problems, `${path}.id`, p.id);
  const forOk = stringArray(problems, `${path}.forHypotheses`, p.forHypotheses);
  if (forOk && p.forHypotheses.length < 2) {
    problems.push(`${path}.forHypotheses: a probe discriminates between at least two hypotheses`);
  }

  if (object(problems, `${path}.intervention`, p.intervention)) {
    const iv = p.intervention;
    str(problems, `${path}.intervention.target`, iv.target);
    oneOf(problems, `${path}.intervention.action`, iv.action, ['shed']);
    str(problems, `${path}.intervention.upstream`, iv.upstream);
    // Safety rails, enforced by the contract rather than by remembering.
    num(problems, `${path}.intervention.fraction`, iv.fraction, { min: 0, max: MAX_PROBE_FRACTION });
    num(problems, `${path}.intervention.durationMs`, iv.durationMs, { min: 1, max: MAX_PROBE_DURATION_MS });
  }

  str(problems, `${path}.discriminator`, p.discriminator);
  if (object(problems, `${path}.predictions`, p.predictions) && forOk) {
    for (const id of p.forHypotheses) {
      const at = `${path}.predictions.${id}`;
      if (!object(problems, at, p.predictions[id])) continue;
      oneOf(problems, `${at}.direction`, p.predictions[id].direction, ['drop', 'rise', 'flat']);
      str(problems, `${at}.magnitude`, p.predictions[id].magnitude);
    }
  }
  array(problems, `${path}.baselineWindows`, p.baselineWindows);
  array(problems, `${path}.measureWindows`, p.measureWindows);
  // publishedAt must exist on the published object: it is the falsifiability claim.
  num(problems, `${path}.publishedAt`, p.publishedAt, { min: MIN_EPOCH_MS });
  return problems;
}

function probeResultProblems(r, path = 'probeResult') {
  const problems = [];
  if (!object(problems, path, r)) return problems;
  str(problems, `${path}.probeId`, r.probeId);
  num(problems, `${path}.measuredDeltaPct`, r.measuredDeltaPct);
  if (r.matched !== null) str(problems, `${path}.matched`, r.matched);
  const inconclusiveOk = bool(problems, `${path}.inconclusive`, r.inconclusive);

  if (object(problems, `${path}.posteriors`, r.posteriors)) {
    let total = 0;
    for (const [id, value] of Object.entries(r.posteriors)) {
      if (num(problems, `${path}.posteriors.${id}`, value, { min: 0, max: 1 })) total += value;
    }
    if (Math.abs(total - 1) > 0.01) {
      problems.push(`${path}.posteriors: must sum to 1, got ${total.toFixed(3)}`);
    }
  }
  // An inconclusive measurement names no winner, and naming a winner is not inconclusive.
  if (inconclusiveOk && r.inconclusive !== (r.matched === null)) {
    problems.push(
      `${path}: inconclusive (${r.inconclusive}) disagrees with matched (${show(r.matched)})`
    );
  }
  return problems;
}

function planProblems(plan, path = 'plan') {
  const problems = [];
  if (!object(problems, path, plan)) return problems;

  const ids = [];
  if (array(problems, `${path}.options`, plan.options)) {
    plan.options.forEach((opt, i) => {
      const at = `${path}.options[${i}]`;
      if (!object(problems, at, opt)) return;
      if (str(problems, `${at}.id`, opt.id)) ids.push(opt.id);
      oneOf(problems, `${at}.actionType`, opt.actionType, ACTION_TYPES);
      str(problems, `${at}.target`, opt.target);
      object(problems, `${at}.params`, opt.params);
      const reversibleOk = bool(problems, `${at}.reversible`, opt.reversible);
      if (object(problems, `${at}.predicted`, opt.predicted)) {
        stringArray(problems, `${at}.predicted.recovers`, opt.predicted.recovers);
        stringArray(problems, `${at}.predicted.degrades`, opt.predicted.degrades);
      }
      const autonomyOk = oneOf(problems, `${at}.autonomy`, opt.autonomy, AUTONOMY_LEVELS);
      str(problems, `${at}.gateReason`, opt.gateReason);
      // Irreversible actions are gated to a human unconditionally, at any confidence.
      if (reversibleOk && autonomyOk && !opt.reversible && opt.autonomy === 'AUTONOMOUS') {
        problems.push(`${at}: irreversible actions may never be AUTONOMOUS`);
      }
    });
  }
  num(problems, `${path}.effectiveConfidence`, plan.effectiveConfidence, { min: 0, max: 1 });
  if (plan.recommendedOptionId !== null) {
    if (str(problems, `${path}.recommendedOptionId`, plan.recommendedOptionId) &&
        ids.length > 0 && !ids.includes(plan.recommendedOptionId)) {
      problems.push(
        `${path}.recommendedOptionId: ${show(plan.recommendedOptionId)} is not one of the options`
      );
    }
  }
  return problems;
}

function actionRecordProblems(a, path = 'actionRecord') {
  const problems = [];
  if (!object(problems, path, a)) return problems;
  str(problems, `${path}.actionId`, a.actionId);
  str(problems, `${path}.optionId`, a.optionId);
  num(problems, `${path}.issuedAt`, a.issuedAt, { min: MIN_EPOCH_MS });
  str(problems, `${path}.target`, a.target);
  if (a.httpStatus !== null) {
    num(problems, `${path}.httpStatus`, a.httpStatus, { int: true, min: 100, max: 599 });
  }
  oneOf(problems, `${path}.outcome`, a.outcome, ACTION_OUTCOMES);
  return problems;
}

function verdictProblems(v, path = 'verdict') {
  const problems = [];
  if (!object(problems, path, v)) return problems;
  str(problems, `${path}.actionId`, v.actionId);
  oneOf(problems, `${path}.verdict`, v.verdict, VERDICTS);
  array(problems, `${path}.observedWindows`, v.observedWindows);
  object(problems, `${path}.before`, v.before);
  object(problems, `${path}.after`, v.after);
  return problems;
}

function controlStateProblems(s, path = 'controlState') {
  const problems = [];
  if (!object(problems, path, s)) return problems;
  if (s.incident !== null) object(problems, `${path}.incident`, s.incident);
  if (array(problems, `${path}.hypotheses`, s.hypotheses)) {
    s.hypotheses.forEach((h, i) => {
      problems.push(...hypothesisProblems(h, `${path}.hypotheses[${i}]`));
    });
  }
  if (s.probe !== null && s.probe !== undefined) {
    problems.push(...probeProblems(s.probe, `${path}.probe`));
  }
  if (s.plan !== null && s.plan !== undefined) {
    problems.push(...planProblems(s.plan, `${path}.plan`));
  }
  if (s.verdict !== null && s.verdict !== undefined) {
    problems.push(...verdictProblems(s.verdict, `${path}.verdict`));
  }
  return problems;
}

// ---------------------------------------------------------------------------
// The gated wrappers. These are what the request path calls.
// ---------------------------------------------------------------------------

function describe(value) {
  try {
    const json = JSON.stringify(value);
    if (json === undefined) return String(value);
    return json.length > 1500 ? `${json.slice(0, 1500)}... (truncated)` : json;
  } catch {
    return '<unserialisable>';
  }
}

function logInvalid(label, problems, offending) {
  console.error(`[contract] ${label} failed validation (${problems.length} problem(s)):`);
  for (const problem of problems) console.error(`[contract]   - ${problem}`);
  console.error(`[contract]   offending object: ${describe(offending)}`);
}

// One wrapper shape for all eight validators: check the flag, run the pure checker,
// log loudly, return a boolean. Never throws.
function gated(label, problemsFn) {
  return function validate(value) {
    if (!validationEnabled) return true;
    const problems = problemsFn(value);
    if (problems.length === 0) return true;
    logInvalid(label, problems, value);
    return false;
  };
}

module.exports = {
  // constants
  PORTS,
  SERVICES,
  SERVICE_STATES,
  SHED_LEVELS,
  EVENT_KINDS,
  KIND_PRIORITY,
  MAX_PROBE_FRACTION,
  MAX_PROBE_DURATION_MS,
  ACTION_TYPES,
  AUTONOMY_LEVELS,
  ACTION_OUTCOMES,
  VERDICTS,
  validationEnabled,

  // gated validators — boolean, log on failure, no-op when IM_VALIDATE is off
  validateEvent: gated('Event', eventProblems),
  validateIngestBatch: gated('IngestBatch', ingestBatchProblems),
  validateIngestAck: gated('IngestAck', ingestAckProblems),
  validateWindowAggregate: gated('WindowAggregate', windowAggregateProblems),
  validateHypothesis: gated('Hypothesis', hypothesisProblems),
  validateProbe: gated('Probe', probeProblems),
  validateProbeResult: gated('ProbeResult', probeResultProblems),
  validatePlan: gated('Plan', planProblems),
  validateActionRecord: gated('ActionRecord', actionRecordProblems),
  validateVerdict: gated('Verdict', verdictProblems),
  validateControlState: gated('ControlState', controlStateProblems),

  // pure checkers — always run, return problem strings
  eventProblems,
  ingestBatchProblems,
  ingestAckProblems,
  windowAggregateProblems,
  hypothesisProblems,
  probeProblems,
  probeResultProblems,
  planProblems,
  actionRecordProblems,
  verdictProblems,
  controlStateProblems,
};
