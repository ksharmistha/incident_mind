'use strict';

// TEMPORARY stand-in for packages/contracts/index.js, which P1 owns and freezes.
// Shapes transcribed from the P2 handoff sections 8.3 and 8.5. Delete when the repo arrives.
//
// Validation policy follows P1 handoff section 8.6: a failure logs loudly with the
// offending object, it does not throw in the request path. Validators therefore return
// an array of problem strings; report() does the logging.

const SERVICE_STATES = ['HEALTHY', 'DEGRADED', 'CRITICAL', 'UNKNOWN'];
const AUTONOMY = ['AUTONOMOUS', 'HUMAN', 'BLOCKED'];
const ACTION_TYPES = ['rollback', 'isolate', 'restart'];
const OUTCOMES = ['APPLIED', 'EXECUTION_FAILED'];
const VERDICTS = ['RECOVERED', 'PARTIAL', 'FAILED', 'INCONCLUSIVE'];

const num = (v) => typeof v === 'number' && Number.isFinite(v);
const nonNegInt = (v) => Number.isInteger(v) && v >= 0;
const str = (v) => typeof v === 'string' && v.length > 0;
const obj = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
const inUnit = (v) => num(v) && v >= 0 && v <= 1;

function validateWindowAggregate(w) {
  const p = [];
  if (!obj(w)) return ['not an object'];

  if (!nonNegInt(w.windowId)) p.push('windowId must be a non-negative integer');
  for (const k of ['tStart', 'tEnd', 'closedAt']) if (!num(w[k])) p.push(`${k} must be a number`);
  if (typeof w.complete !== 'boolean') p.push('complete must be a boolean');

  if (!obj(w.services)) p.push('services must be an object');
  else for (const [svc, s] of Object.entries(w.services)) {
    if (!obj(s)) { p.push(`services.${svc} must be an object`); continue; }
    for (const k of ['rps', 'errRate', 'p50', 'p95', 'p99', 'selfP99', 'downstreamP99']) {
      if (!num(s[k])) p.push(`services.${svc}.${k} must be a number`);
    }
    if (!nonNegInt(s.sampleCount)) p.push(`services.${svc}.sampleCount must be a non-negative integer`);
    if (!inUnit(s.inclusionProbability)) p.push(`services.${svc}.inclusionProbability must be in [0,1]`);
    if (!SERVICE_STATES.includes(s.state)) p.push(`services.${svc}.state must be one of ${SERVICE_STATES.join('|')}`);
  }

  if (!obj(w.edges)) p.push('edges must be an object');
  else for (const [key, e] of Object.entries(w.edges)) {
    if (!obj(e)) { p.push(`edges["${key}"] must be an object`); continue; }
    for (const k of ['calls', 'attempts', 'timeouts']) {
      if (!nonNegInt(e[k])) p.push(`edges["${key}"].${k} must be a non-negative integer`);
    }
    if (!num(e.amplification)) p.push(`edges["${key}"].amplification must be a number`);
    // The edge key carries the only from/to information in the contract, so a malformed
    // key silently zeroes two of the four scorer terms. Catch it here, in one window.
    if (!parseEdgeKey(key)) p.push(`edges["${key}"] key is not parseable as "<from>-><to>"`);
  }

  if (!obj(w.resources)) p.push('resources must be an object');
  else for (const [res, r] of Object.entries(w.resources)) {
    if (!obj(r)) { p.push(`resources.${res} must be an object`); continue; }
    for (const k of ['poolSize', 'poolInUse', 'queueDepth']) {
      if (!nonNegInt(r[k])) p.push(`resources.${res}.${k} must be a non-negative integer`);
    }
    for (const k of ['queueWaitP99', 'arrivalRate', 'serviceTimeMs', 'utilisation']) {
      if (!num(r[k])) p.push(`resources.${res}.${k} must be a number`);
    }
  }

  const pl = w.pipeline;
  if (!obj(pl)) p.push('pipeline must be an object');
  else {
    for (const k of ['ingestRate', 'queueDepth', 'watermarkLagMs', 'dedupHits']) {
      if (!num(pl[k])) p.push(`pipeline.${k} must be a number`);
    }
    if (![0, 1, 2, 3].includes(pl.shedLevel)) p.push('pipeline.shedLevel must be 0, 1, 2 or 3');
    if (!obj(pl.dropped)) p.push('pipeline.dropped must be an object');
    else {
      for (const k of ['pri0', 'pri1', 'pri2']) {
        if (!nonNegInt(pl.dropped[k])) p.push(`pipeline.dropped.${k} must be a non-negative integer`);
      }
      // The guarantee the whole submission rests on.
      if (pl.dropped.pri0 !== 0) p.push(`pipeline.dropped.pri0 must be 0, got ${pl.dropped.pri0}`);
    }
    // ASSUMPTION A1 (see control/adapters/contracts.js): lateDropped is documented behaviour
    // in all three PDFs but is absent from the formal WindowAggregate shape. Treated as
    // OPTIONAL: accepted when present, never required, never read by any agent.
    if (pl.lateDropped !== undefined && !nonNegInt(pl.lateDropped)) {
      p.push('pipeline.lateDropped, when present, must be a non-negative integer');
    }
  }

  if (!inUnit(w.observationConfidence)) p.push('observationConfidence must be in [0,1]');

  const t = w.observationTerms;
  if (!obj(t)) p.push('observationTerms must be an object');
  else for (const k of ['shedRate', 'watermarkLag', 'silentServices']) {
    // ASSUMPTION A2: observationTerms.* are normalised, unweighted fractions in [0,1].
    // watermarkLag = min(1, pipeline.watermarkLagMs / 5000). A value above 1 almost
    // certainly means raw milliseconds leaked into the term, so say so specifically.
    if (!num(t[k])) p.push(`observationTerms.${k} must be a number`);
    else if (t[k] < 0 || t[k] > 1) {
      p.push(`observationTerms.${k}=${t[k]} is outside [0,1] - expected a normalised term, not raw units`);
    }
  }

  return p;
}

// Accepts "gateway->auth", "gateway→auth", and either with surrounding spaces.
function parseEdgeKey(key) {
  if (typeof key !== 'string') return null;
  const m = key.split(/\s*(?:->|→|=>)\s*/);
  if (m.length !== 2 || !m[0] || !m[1]) return null;
  return { from: m[0].trim(), to: m[1].trim() };
}

function validateHypothesis(h) {
  const p = [];
  if (!obj(h)) return ['not an object'];
  if (!str(h.id)) p.push('id must be a non-empty string');
  if (!str(h.statement)) p.push('statement must be a non-empty string');
  if (!str(h.rootCauseService)) p.push('rootCauseService must be a non-empty string');
  if (!str(h.failureMode)) p.push('failureMode must be a non-empty string');
  if (!num(h.score)) p.push('score must be a number');
  if (!inUnit(h.posterior)) p.push('posterior must be in [0,1]');
  if (!obj(h.terms)) p.push('terms must be an object');
  else for (const k of ['temporalPrecedence', 'upstreamness', 'amplificationTarget', 'sharedResourcePenalty']) {
    if (!num(h.terms[k])) p.push(`terms.${k} must be a number`);
  }
  return p;
}

function validateProbe(pr) {
  const p = [];
  if (!obj(pr)) return ['not an object'];
  if (!str(pr.id)) p.push('id must be a non-empty string');
  if (!Array.isArray(pr.forHypotheses) || pr.forHypotheses.length !== 2) p.push('forHypotheses must be an array of 2 hypothesis ids');
  if (!str(pr.discriminator)) p.push('discriminator must be a non-empty string');
  if (!num(pr.publishedAt)) p.push('publishedAt must be a number, set BEFORE execution');
  const i = pr.intervention;
  if (!obj(i)) p.push('intervention must be an object');
  else {
    if (!str(i.target)) p.push('intervention.target must be a non-empty string');
    if (i.action !== 'shed') p.push('intervention.action must be "shed"');
    if (!str(i.upstream)) p.push('intervention.upstream must be a non-empty string');
    // Safety rails, not tuning knobs.
    if (!num(i.fraction) || i.fraction <= 0 || i.fraction > 0.2) p.push(`intervention.fraction must be in (0,0.2], got ${i.fraction}`);
    if (!num(i.durationMs) || i.durationMs <= 0 || i.durationMs > 5000) p.push(`intervention.durationMs must be in (0,5000], got ${i.durationMs}`);
  }
  if (!obj(pr.predictions)) p.push('predictions must be an object keyed by hypothesis id');
  return p;
}

function validateProbeResult(r) {
  const p = [];
  if (!obj(r)) return ['not an object'];
  if (!str(r.probeId)) p.push('probeId must be a non-empty string');
  if (!num(r.measuredDeltaPct)) p.push('measuredDeltaPct must be a number');
  if (r.matched !== null && !str(r.matched)) p.push('matched must be a hypothesis id or null');
  if (typeof r.inconclusive !== 'boolean') p.push('inconclusive must be a boolean');
  if (!obj(r.posteriors)) p.push('posteriors must be an object');
  return p;
}

function validatePlan(pl) {
  const p = [];
  if (!obj(pl)) return ['not an object'];
  if (!Array.isArray(pl.options)) p.push('options must be an array');
  else pl.options.forEach((o, n) => {
    if (!obj(o)) { p.push(`options[${n}] must be an object`); return; }
    if (!str(o.id)) p.push(`options[${n}].id must be a non-empty string`);
    if (!ACTION_TYPES.includes(o.actionType)) p.push(`options[${n}].actionType must be one of ${ACTION_TYPES.join('|')}`);
    if (!str(o.target)) p.push(`options[${n}].target must be a non-empty string`);
    if (typeof o.reversible !== 'boolean') p.push(`options[${n}].reversible must be a boolean`);
    if (!AUTONOMY.includes(o.autonomy)) p.push(`options[${n}].autonomy must be one of ${AUTONOMY.join('|')}`);
    // Irreversible actions are gated to a human unconditionally, regardless of confidence.
    if (o.reversible === false && o.autonomy === 'AUTONOMOUS') {
      p.push(`options[${n}] is irreversible and must never be AUTONOMOUS`);
    }
    if (o.autonomy === 'BLOCKED' && !str(o.gateReason)) p.push(`options[${n}] is BLOCKED and must carry a gateReason`);
    if (!obj(o.predicted)) p.push(`options[${n}].predicted must be an object`);
  });
  if (!inUnit(pl.effectiveConfidence)) p.push('effectiveConfidence must be in [0,1]');
  return p;
}

function validateActionRecord(a) {
  const p = [];
  if (!obj(a)) return ['not an object'];
  if (!str(a.actionId)) p.push('actionId must be a non-empty string');
  if (!str(a.optionId)) p.push('optionId must be a non-empty string');
  if (!num(a.issuedAt)) p.push('issuedAt must be a number');
  if (!str(a.target)) p.push('target must be a non-empty string');
  if (!OUTCOMES.includes(a.outcome)) p.push(`outcome must be one of ${OUTCOMES.join('|')}`);
  return p;
}

function validateVerdict(v) {
  const p = [];
  if (!obj(v)) return ['not an object'];
  if (!str(v.actionId)) p.push('actionId must be a non-empty string');
  if (!VERDICTS.includes(v.verdict)) p.push(`verdict must be one of ${VERDICTS.join('|')}`);
  if (!nonNegInt(v.observedWindows)) p.push('observedWindows must be a non-negative integer');
  if (!obj(v.before)) p.push('before must be an object');
  if (!obj(v.after)) p.push('after must be an object');
  return p;
}

function validateControlState(s) {
  const p = [];
  if (!obj(s)) return ['not an object'];
  if (!Array.isArray(s.hypotheses)) p.push('hypotheses must be an array');
  else s.hypotheses.forEach((h, n) => validateHypothesis(h).forEach((x) => p.push(`hypotheses[${n}]: ${x}`)));
  if (s.probe !== null) validateProbe(s.probe).forEach((x) => p.push(`probe: ${x}`));
  if (s.plan !== null) validatePlan(s.plan).forEach((x) => p.push(`plan: ${x}`));
  if (s.verdict !== null) validateVerdict(s.verdict).forEach((x) => p.push(`verdict: ${x}`));
  return p;
}

module.exports = {
  SERVICE_STATES, AUTONOMY, ACTION_TYPES, OUTCOMES, VERDICTS,
  parseEdgeKey,
  validateWindowAggregate,
  validateHypothesis, validateProbe, validateProbeResult,
  validatePlan, validateActionRecord, validateVerdict, validateControlState,
};
