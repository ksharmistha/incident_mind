'use strict';

// Scorer — ranks candidate root causes. Diagnosis, not detection, and not decision.
//
// P2 handoff §10.3:
//   score(s) = 0.30 · temporalPrecedence(s)     degraded first, decaying with lag
//            + 0.25 · upstreamness(s)            share of degraded services reachable from s
//            + 0.25 · amplificationTarget(s)     s is the TARGET of an edge with amp > 1.5
//            − 0.35 · sharedResourcePenalty(s)   in-degree ≥ 3 ⇒ likely symptom
//
// Two things this deliberately does not do, both load-bearing:
//
//   No change-correlation term. Including one would let a deploy marker resolve the
//   F1-vs-F2 ambiguity for free and the probe would become theatre.
//
//   The self-vs-downstream latency split labels a hypothesis but never scores it. Auth's
//   selfP99 is elevated under both faults — by its own CPU under F1, by queueing behind a
//   saturated pool under F2 — so its level is confounded. What separates the two is how it
//   RESPONDS to intervention, which is the Experimenter's measurement, not the Scorer's.
//   Scoring on the level here would pre-empt the experiment.

const { parseEdgeKey } = require('../adapters/contracts');
const { control, derived } = require('../adapters/tuning');

const WEIGHTS = {
  temporalPrecedence: 0.30,
  upstreamness: 0.25,
  amplificationTarget: 0.25,
  sharedResourcePenalty: -0.35,
};

const AMPLIFIED = 1.5;        // §10.3: "the TARGET of an edge with amp > 1.5"
const SHARED_IN_DEGREE = 3;   // §10.3: "in-degree ≥ 3 ⇒ likely symptom"

// ---------------------------------------------------------------------------
// Graph, built from the edge counters the collector already publishes.
// ---------------------------------------------------------------------------

function buildGraph(edges) {
  const out = new Map();       // caller -> Set(callee)
  const into = new Map();      // callee -> Set(caller)
  const inbound = new Map();   // callee -> [{ from, amplification }]

  for (const [key, edge] of Object.entries(edges || {})) {
    const e = parseEdgeKey(key);
    if (!e || !edge || typeof edge !== 'object') continue;
    if (!out.has(e.from)) out.set(e.from, new Set());
    if (!into.has(e.to)) into.set(e.to, new Set());
    out.get(e.from).add(e.to);
    into.get(e.to).add(e.from);
    if (!inbound.has(e.to)) inbound.set(e.to, []);
    inbound.get(e.to).push({ from: e.from, amplification: finite(edge.amplification) ?? 0 });
  }
  return { out, into, inbound };
}

// Everything that transitively CALLS s. If s is slow, these are the services whose
// slowness s explains — which is the direction causation actually travels. Following
// outgoing edges instead would credit the gateway, which reaches everything and explains
// nothing: the gateway is the caller, so it is a victim of its callees, never their cause.
function callersOf(graph, s) {
  const seen = new Set();
  const stack = [...(graph.into.get(s) || [])];
  while (stack.length) {
    const n = stack.pop();
    if (seen.has(n)) continue;
    seen.add(n);
    for (const up of graph.into.get(n) || []) stack.push(up);
  }
  return seen;
}

// ---------------------------------------------------------------------------
// The four terms. Each returns [0,1].
// ---------------------------------------------------------------------------

// Degraded first wins, decaying with lag. The decay constant is short on purpose: under a
// fast ramp, precedence within one or two windows is not reliable evidence, so services
// that degrade within a couple of seconds of each other must score near-equally rather
// than the earliest taking all of it.
function temporalPrecedence(onset, earliest) {
  if (onset === null || earliest === null) return 0;
  const lagSeconds = (onset - earliest) / 1000;
  if (!Number.isFinite(lagSeconds) || lagSeconds < 0) return 0;
  return Math.exp(-lagSeconds / derived.precedenceDecaySeconds);
}

function upstreamness(graph, name, degraded) {
  const others = degraded.filter((d) => d !== name);
  if (others.length === 0) return 0;
  const callers = callersOf(graph, name);
  return others.filter((d) => callers.has(d)).length / others.length;
}

// Credits the TARGET of an amplified edge, never its source. Retry amplification on
// gateway→auth means the callee is slow and the caller is retrying against it; the caller
// is behaving correctly. Crediting the source would rank the gateway as root cause, which
// is always wrong.
//
// Note this points at auth under BOTH faults, because a service waiting on a slow
// dependency gets retried exactly like one that is slow on its own. That is intended, and
// it is one of the reasons the probe is necessary.
function amplificationTarget(graph, name) {
  const inbound = graph.inbound.get(name) || [];
  let best = 0;
  for (const e of inbound) {
    if (e.amplification <= AMPLIFIED) continue;
    // Graded from the threshold up, so a marginal 1.6 is not treated as a 2.9.
    best = Math.max(best, Math.min(1, (e.amplification - AMPLIFIED) / AMPLIFIED));
  }
  return best;
}

// The negative term. A resource everyone depends on is where symptoms concentrate, not
// usually where causes originate — this is what stops the datastore, the loudest and most
// connected node on the screen, from winning when it is a victim.
function sharedResourcePenalty(graph, name) {
  return (graph.into.get(name)?.size || 0) >= SHARED_IN_DEGREE ? 1 : 0;
}

// ---------------------------------------------------------------------------
// Hypothesis labelling. Evidence-driven, never by service name.
// ---------------------------------------------------------------------------

function failureModeFor(graph, name, svc) {
  const self = finite(svc?.selfP99) ?? 0;
  const down = finite(svc?.downstreamP99) ?? 0;
  const total = self + down;
  const inDegree = graph.into.get(name)?.size || 0;
  const outDegree = graph.out.get(name)?.size || 0;

  // A leaf that everyone calls is a contended resource, whatever its name.
  if (inDegree >= SHARED_IN_DEGREE && outDegree === 0) {
    return { failureMode: 'resource_contention', selfShare: 1 };
  }
  if (total === 0) return { failureMode: 'unknown', selfShare: null };

  const selfShare = self / total;
  if (selfShare >= 0.6) return { failureMode: 'cpu_regression', selfShare };
  if (selfShare <= 0.4) return { failureMode: 'downstream_dependency', selfShare };
  return { failureMode: 'mixed', selfShare };
}

function statementFor(name, mode, svc, selfShare) {
  const self = round(finite(svc?.selfP99) ?? 0);
  const down = round(finite(svc?.downstreamP99) ?? 0);
  const pct = selfShare === null ? '?' : Math.round(selfShare * 100);
  switch (mode) {
    case 'cpu_regression':
      return `${name} is slow in its own code path (selfP99 ${self}ms, ${pct}% of its ${round(self + down)}ms)`;
    case 'downstream_dependency':
      return `${name} is waiting on a slow dependency (downstreamP99 ${down}ms, selfP99 only ${self}ms)`;
    case 'resource_contention':
      return `${name} is a shared resource under contention (serviceTime + queue wait ${self}ms, in-degree ${'≥'}${SHARED_IN_DEGREE})`;
    case 'mixed':
      return `${name} is degraded with time split across itself and its dependencies (self ${self}ms, downstream ${down}ms)`;
    default:
      return `${name} is degraded with insufficient latency decomposition to attribute`;
  }
}

// ---------------------------------------------------------------------------

function createScorer(opts = {}) {
  const marginThreshold = opts.probeMarginThreshold ?? control.probeMarginThreshold;
  let lastResult = null;

  // Pure: (incident, window) -> ranked hypotheses. No clock, no randomness, no memory of
  // previous calls feeding the result — the same inputs always score the same.
  function score(incident, window) {
    if (!usableIncident(incident)) return empty();

    const services = (window && isObject(window.services)) ? window.services : {};
    const edges = isObject(incident.edges) ? incident.edges
      : (window && isObject(window.edges) ? window.edges : {});
    const graph = buildGraph(edges);

    const candidates = incident.candidates.filter((c) => c && typeof c.service === 'string');
    if (candidates.length === 0) return empty();

    const degraded = candidates.map((c) => c.service);
    const onsets = new Map(candidates.map((c) => [c.service, finite(c.firstDegradedAt)]));
    const known = [...onsets.values()].filter((v) => v !== null);
    const earliest = known.length ? Math.min(...known) : null;

    const scored = candidates.map((c) => {
      const name = c.service;
      const svc = services[name] || c;
      const terms = {
        temporalPrecedence: clamp01(temporalPrecedence(onsets.get(name), earliest)),
        upstreamness: clamp01(upstreamness(graph, name, degraded)),
        amplificationTarget: clamp01(amplificationTarget(graph, name)),
        sharedResourcePenalty: clamp01(sharedResourcePenalty(graph, name)),
      };
      const total =
        WEIGHTS.temporalPrecedence * terms.temporalPrecedence +
        WEIGHTS.upstreamness * terms.upstreamness +
        WEIGHTS.amplificationTarget * terms.amplificationTarget +
        WEIGHTS.sharedResourcePenalty * terms.sharedResourcePenalty;

      const { failureMode, selfShare } = failureModeFor(graph, name, svc);
      return {
        rootCauseService: name,
        failureMode,
        statement: statementFor(name, failureMode, svc, selfShare),
        score: round3(total),
        terms: {
          temporalPrecedence: round3(terms.temporalPrecedence),
          upstreamness: round3(terms.upstreamness),
          amplificationTarget: round3(terms.amplificationTarget),
          sharedResourcePenalty: round3(terms.sharedResourcePenalty),
        },
      };
    });

    // Rank by score, then by name so ties are deterministic rather than insertion-ordered.
    scored.sort((a, b) => b.score - a.score || a.rootCauseService.localeCompare(b.rootCauseService));

    const posteriors = normalise(scored.map((h) => h.score), marginThreshold);
    const hypotheses = scored.map((h, i) => ({
      id: `H${i + 1}`,
      statement: h.statement,
      rootCauseService: h.rootCauseService,
      failureMode: h.failureMode,
      score: h.score,
      posterior: posteriors[i],
      terms: h.terms,
    }));

    const margin = hypotheses.length >= 2
      ? round3(hypotheses[0].score - hypotheses[1].score)
      : null;

    lastResult = {
      hypotheses,
      margin,
      // Below the threshold, observation alone cannot separate the top two. Saying so is
      // the honest output; the Experimenter decides what to do about it.
      ambiguous: margin !== null && margin < marginThreshold,
      windowId: window?.windowId ?? null,
    };
    return lastResult;
  }

  function empty() {
    lastResult = { hypotheses: [], margin: null, ambiguous: false, windowId: null };
    return lastResult;
  }

  return { score, reset: () => { lastResult = null; }, last: () => lastResult, WEIGHTS };
}

// Posteriors before any experiment.
//
// Softmax, with the temperature set to probeMarginThreshold: a score difference of one
// margin-threshold is one unit of evidence. That ties the belief scale to the same number
// that decides whether the evidence is ambiguous, instead of a second invented constant.
//
// Normalising by shifting the lowest score to zero — the obvious alternative — is wrong
// and dangerous: with two candidates it always yields 1.0 and 0.0, turning a 0.03 margin
// into absolute certainty. Preserving uncertainty is the whole point of publishing a
// posterior before the experiment runs.
function normalise(scores, temperature) {
  if (scores.length === 0) return [];
  if (scores.length === 1) return [1];
  const t = temperature > 0 ? temperature : 0.1;
  const max = Math.max(...scores);                    // subtract max for numerical stability
  const weights = scores.map((s) => Math.exp((s - max) / t));
  const sum = weights.reduce((a, b) => a + b, 0);
  return weights.map((w) => round3(w / sum));
}

function usableIncident(i) {
  return isObject(i) && Array.isArray(i.candidates);
}
function isObject(v) { return v !== null && typeof v === 'object' && !Array.isArray(v); }
function finite(v) { return typeof v === 'number' && Number.isFinite(v) ? v : null; }
function clamp01(v) { return Number.isFinite(v) ? Math.min(1, Math.max(0, v)) : 0; }
function round(v) { return Math.round(v * 10) / 10; }
function round3(v) { return Math.round(v * 1000) / 1000; }

module.exports = { createScorer, WEIGHTS, buildGraph, callersOf };
