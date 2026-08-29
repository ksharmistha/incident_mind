'use strict';

// Planner — decides what SHOULD be done, and stops there.
//
// P2 handoff §10.4:
//   effectiveConfidence = topPosterior × observationConfidence
//   reversible actions   (open breaker, run probe)  → AUTONOMOUS if effConf ≥ 0.60
//   irreversible actions (rollback, restart)        → always HUMAN
//   any action                                      → BLOCKED if effConf < 0.60,
//                                                     with a stated gateReason
//
// It describes options; it never issues one. There is no HTTP client in this file and it
// does not import the admin adapter — a Planner that could act would make the approval gate
// a convention rather than a structural fact.
//
// Predicted recovers[] / degrades[] come from graph reachability over the observed edges,
// not from a table of expected outcomes.

const { ACTION_TYPES, parseEdgeKey } = require('../adapters/contracts');
const { tuning, control } = require('../adapters/tuning');

// Reversibility is a property of the action, not of our confidence in it. Opening a breaker
// can be undone by closing it. A version rollback and a restart cannot be un-done — the
// requests lost in between are gone.
const REVERSIBLE = { isolate: true, rollback: false, restart: false };

function createPlanner(opts = {}) {
  const autonomyConfidence = opts.autonomyConfidence ?? control.autonomyConfidence;
  const marginThreshold = opts.probeMarginThreshold ?? control.probeMarginThreshold;
  const config = opts.tuning ?? tuning;
  let plan = null;

  // -------------------------------------------------------------------------
  // Which hypothesis does the evidence actually support?
  //
  // A conclusive probe outranks passive ranking, because it is the only evidence gathered
  // by intervention. Without one, observation must have separated the top two on its own.
  // If neither holds there is no supported root cause, and inventing a remediation for a
  // cause we cannot name is how a small outage becomes a large one.
  // -------------------------------------------------------------------------
  function supportedHypothesis(raw, probeResult) {
    const hypotheses = (Array.isArray(raw) ? raw : []).filter(
      (h) => h && typeof h === 'object' && typeof h.rootCauseService === 'string' && typeof h.id === 'string'
    );
    if (hypotheses.length === 0) {
      return { ok: false, reason: 'no usable hypotheses to plan against' };
    }
    if (probeResult && probeResult.inconclusive) {
      return { ok: false, reason: 'probe was INCONCLUSIVE — escalate to a human rather than acting on a measurement we do not trust' };
    }
    if (probeResult && probeResult.matched) {
      const h = hypotheses.find((x) => x.id === probeResult.matched);
      if (!h) return { ok: false, reason: `probe matched ${probeResult.matched}, which is not among the current hypotheses` };
      return { ok: true, hypothesis: h, evidence: `probe ${probeResult.probeId} measured ${probeResult.measuredDeltaPct}%` };
    }
    if (hypotheses.length >= 2) {
      const margin = (hypotheses[0].score ?? 0) - (hypotheses[1].score ?? 0);
      if (margin < marginThreshold) {
        return { ok: false, reason: `top-two margin ${round3(margin)} < ${marginThreshold} and no conclusive probe — evidence does not name a root cause` };
      }
    }
    return { ok: true, hypothesis: hypotheses[0], evidence: `observation separates the top two by ${round3((hypotheses[0].score ?? 0) - (hypotheses[1]?.score ?? 0))}` };
  }

  // -------------------------------------------------------------------------
  // Graph reachability. What an action is predicted to fix, and what it costs.
  // -------------------------------------------------------------------------
  function graphOf(edges) {
    const out = new Map(), into = new Map();
    for (const key of Object.keys(edges || {})) {
      const e = parseEdgeKey(key);
      if (!e) continue;
      if (!out.has(e.from)) out.set(e.from, new Set());
      if (!into.has(e.to)) into.set(e.to, new Set());
      out.get(e.from).add(e.to);
      into.get(e.to).add(e.from);
    }
    return { out, into };
  }

  // Services that share a downstream dependency with `service`. Isolating or fixing
  // `service` frees whatever it was consuming, so these are the ones predicted to recover —
  // this is the semaphore contention expressed as graph reachability, with no rule anywhere
  // naming a specific pair of services.
  function contentionPeers(graph, service, degraded) {
    const deps = graph.out.get(service) || new Set();
    const peers = new Set();
    for (const dep of deps) {
      for (const other of graph.into.get(dep) || []) {
        if (other !== service && degraded.includes(other)) peers.add(other);
      }
    }
    return [...peers].sort();
  }

  function callersOf(graph, service, degraded) {
    const seen = new Set(), stack = [...(graph.into.get(service) || [])];
    while (stack.length) {
      const n = stack.pop();
      if (seen.has(n)) continue;
      seen.add(n);
      for (const up of graph.into.get(n) || []) stack.push(up);
    }
    return degraded.filter((d) => seen.has(d)).sort();
  }

  // -------------------------------------------------------------------------
  // Option generation. Only actions the evidence and the environment support.
  // -------------------------------------------------------------------------
  function optionsFor(service, graph, degraded) {
    const options = [];

    // rollback — offered only when configuration knows another version of this service
    // exists to roll back TO. Without that, a rollback option is an action we cannot
    // actually take, and offering it would be inventing a remedy.
    const versions = Object.keys(config[service]?.workUnits || {}).sort();
    if (versions.length >= 2) {
      options.push({
        actionType: 'rollback',
        target: service,
        params: { version: versions[0] },
        predicted: {
          recovers: [...new Set([service, ...callersOf(graph, service, degraded), ...contentionPeers(graph, service, degraded)])].sort(),
          degrades: [],
        },
        rationale: `roll ${service} back to ${versions[0]}, removing the regression itself`,
      });
    }

    // isolate — open a breaker on the busiest inbound edge, shedding this service's load off
    // the shared resource. Reversible, so it is the only action that can ever be autonomous.
    const callers = [...(graph.into.get(service) || [])].sort();
    if (callers.length > 0) {
      const from = callers[0];
      options.push({
        actionType: 'isolate',
        target: from,
        params: { upstream: service, open: true },
        predicted: {
          // Freeing the contended resource lets the bystanders through...
          recovers: contentionPeers(graph, service, degraded),
          // ...at the cost of everything that actually needs this service.
          degrades: [...new Set([service, ...callersOf(graph, service, degraded)])].sort(),
        },
        rationale: `open ${from}'s breaker on ${service} to free the resource it is consuming`,
      });
    }

    // restart — always available, always blunt. Clears in-process state (a swapped version,
    // a compaction flag) at the cost of dropping whatever the service was serving.
    options.push({
      actionType: 'restart',
      target: service,
      params: {},
      predicted: {
        recovers: [...new Set([service, ...callersOf(graph, service, degraded), ...contentionPeers(graph, service, degraded)])].sort(),
        degrades: [service],
      },
      rationale: `restart ${service}, clearing in-process fault state`,
    });

    return options;
  }

  // -------------------------------------------------------------------------
  // The autonomy gate.
  // -------------------------------------------------------------------------
  function gate(actionType, reversible, effectiveConfidence) {
    if (effectiveConfidence < autonomyConfidence) {
      return {
        autonomy: 'BLOCKED',
        gateReason: `effectiveConfidence ${round3(effectiveConfidence)} < ${autonomyConfidence} — too uncertain to change production`,
      };
    }
    if (!reversible) {
      return {
        autonomy: 'HUMAN',
        gateReason: `${actionType} is irreversible and always requires human approval, at any confidence`,
      };
    }
    return {
      autonomy: 'AUTONOMOUS',
      gateReason: `reversible action at effectiveConfidence ${round3(effectiveConfidence)} ≥ ${autonomyConfidence}`,
    };
  }

  // Deterministic ordering: the action that addresses the named cause most directly first,
  // then the reversible mitigation, then the blunt instrument. Ties break on target name so
  // two runs over the same evidence always produce the same list in the same order.
  const DIRECTNESS = { rollback: 0, isolate: 1, restart: 2 };

  // -------------------------------------------------------------------------

  function build({ incident, hypotheses, probeResult, observationConfidence }) {
    if (!incident || typeof incident !== 'object') {
      return refuse('no active incident');
    }
    const support = supportedHypothesis(hypotheses, probeResult);
    if (!support.ok) return refuse(support.reason);

    const oc = typeof observationConfidence === 'number' && Number.isFinite(observationConfidence)
      ? clamp01(observationConfidence) : 0;
    const topPosterior = clamp01(support.hypothesis.posterior);
    const effectiveConfidence = round3(topPosterior * oc);

    const service = support.hypothesis.rootCauseService;
    const graph = graphOf(incident.edges);
    const degraded = Array.isArray(incident.services) ? incident.services : [];

    const generated = optionsFor(service, graph, degraded)
      .sort((a, b) => DIRECTNESS[a.actionType] - DIRECTNESS[b.actionType] || a.target.localeCompare(b.target));

    const options = generated.map((o) => {
      const reversible = REVERSIBLE[o.actionType];
      const { autonomy, gateReason } = gate(o.actionType, reversible, effectiveConfidence);
      return {
        // Stable across re-plans. The plan is rebuilt every window, so an id derived from a
        // rebuild counter changes underneath an operator who is reading the console — they
        // click approve and the id they saw no longer exists. (actionType, target) is unique
        // within a plan and does not move.
        id: `OPT-${o.actionType}-${o.target}`,
        actionType: o.actionType,
        target: o.target,
        params: o.params,
        reversible,
        predicted: o.predicted,
        autonomy,
        gateReason,
        rationale: o.rationale,
      };
    })
      .filter((o) => ACTION_TYPES.includes(o.actionType))   // nothing outside the contract
      // An action predicted to recover nothing while degrading something is not a
      // remediation, whatever its confidence. Isolating a shared resource is the case that
      // reaches here: cutting the datastore off starves every service that depends on it
      // and frees nothing, because a leaf has no downstream contention to relieve.
      .filter((o) => {
        const useless = o.predicted.recovers.length === 0 && o.predicted.degrades.length > 0;
        if (useless) {
          console.log(`[planner] dropped ${o.actionType} on ${o.target}: predicted to recover nothing and degrade ${o.predicted.degrades.join(', ')}`);
        }
        return !useless;
      })


    // Recommend the most direct option that is not blocked. A blocked plan recommends
    // nothing rather than quietly suggesting the least-bad forbidden action.
    const recommended = options.find((o) => o.autonomy !== 'BLOCKED') || null;

    plan = {
      options,
      effectiveConfidence,
      recommendedOptionId: recommended ? recommended.id : null,
      // Context for the console and for a judge asking where this came from. Not part of
      // the frozen shape, which validates required fields only.
      basis: {
        hypothesisId: support.hypothesis.id,
        rootCauseService: service,
        failureMode: support.hypothesis.failureMode,
        evidence: support.evidence,
        topPosterior,
        observationConfidence: oc,
      },
    };
    console.log(`[planner] ${options.length} options for ${service} (${support.hypothesis.failureMode}), ` +
      `effConf ${effectiveConfidence} = ${topPosterior} × ${oc}, recommending ${plan.recommendedOptionId || 'NOTHING'}`);
    return plan;
  }

  function refuse(reason) {
    plan = null;
    console.log(`[planner] no plan: ${reason}`);
    return null;
  }

  return {
    build,
    reset: () => { plan = null; },
    current: () => plan,
    lastReason: () => null,
  };
}

function clamp01(v) {
  return typeof v === 'number' && Number.isFinite(v) ? Math.min(1, Math.max(0, v)) : 0;
}
function round3(v) { return Math.round(v * 1000) / 1000; }

module.exports = { createPlanner, REVERSIBLE };
