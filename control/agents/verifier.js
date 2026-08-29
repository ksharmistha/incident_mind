'use strict';

// Verifier — decides whether the system actually got better.
//
// P2 handoff §10.5:
//   Verifier  observes 10 windows after the action, FROM THE PIPELINE ONLY.
//     RECOVERED    — all previously-degraded services back to HEALTHY
//     PARTIAL      — some recovered, some still degraded
//     FAILED       — no material improvement -> escalate, do NOT auto-retry
//     INCONCLUSIVE — insufficient signal -> escalate
//
// The invariant this file exists to enforce: a 200 OK is not evidence that anything got
// better. classifyVerdict cannot see an ActionRecord, an HTTP status, or the admin adapter,
// because it is not given them — the guarantee is structural rather than remembered. There
// is no fetch here and no import that could reach the data plane.
//
// Everything is measured in the telemetry stream's own clock. Windows are selected by
// windowId, never by comparing event time to the control plane's Date.now() — mixing the
// two silently selects the wrong windows, which is exactly how the M5 probe measurement
// went wrong before it was caught.

const { validateVerdict, actionRecordProblems, SERVICE_STATES } = require('../adapters/contracts');
const { control, derived } = require('../adapters/tuning');

// Severity ordering, so "improved" and "worsened" are comparisons rather than opinions.
// UNKNOWN is not a severity — it means the pipeline could not see the service, which is a
// gap in evidence and is treated as such.
const RANK = { HEALTHY: 0, DEGRADED: 1, CRITICAL: 2 };
const rankOf = (state) => (state in RANK ? RANK[state] : null);

// ---------------------------------------------------------------------------
// Summaries. A WindowAggregate set collapsed to the per-service facts verification
// reasons about, and nothing else.
// ---------------------------------------------------------------------------

function summarise(windows) {
  const services = {};
  const ids = [];
  for (const w of windows || []) {
    if (!w || typeof w !== 'object' || !w.services || typeof w.services !== 'object') continue;
    ids.push(w.windowId);
    for (const [name, svc] of Object.entries(w.services)) {
      if (!svc || typeof svc !== 'object') continue;
      if (!services[name]) services[name] = { states: [], p99: [], errRate: [] };
      if (SERVICE_STATES.includes(svc.state)) services[name].states.push(svc.state);
      if (finite(svc.p99) !== null) services[name].p99.push(svc.p99);
      if (finite(svc.errRate) !== null) services[name].errRate.push(svc.errRate);
    }
  }
  const out = { windowIds: ids, services: {} };
  for (const [name, s] of Object.entries(services)) {
    // Worst state observed, so a single good window inside a bad stretch cannot pass for
    // recovery — and the same rule applies symmetrically when judging regression.
    const ranks = s.states.map(rankOf).filter((r) => r !== null);
    out.services[name] = {
      state: ranks.length ? stateOfRank(Math.max(...ranks)) : 'UNKNOWN',
      rank: ranks.length ? Math.max(...ranks) : null,
      p99: mean(s.p99),
      errRate: mean(s.errRate),
      windows: s.states.length,
    };
  }
  return out;
}

function stateOfRank(r) {
  return Object.keys(RANK).find((k) => RANK[k] === r) ?? 'UNKNOWN';
}

// ---------------------------------------------------------------------------
// The classifier. Pure, and deliberately blind to how the action was performed.
//
// It takes two telemetry summaries and the Plan's prediction. It does NOT take the
// ActionRecord: no actionId, no httpStatus, no outcome. Verification that could see the
// executor's response would not be independent of it.
// ---------------------------------------------------------------------------

function classifyVerdict(before, after, predicted) {
  if (!before || !after || !before.services || !after.services) {
    return { verdict: 'INCONCLUSIVE', reason: 'missing before/after telemetry summaries' };
  }
  const recovers = Array.isArray(predicted?.recovers) ? predicted.recovers.filter(isName) : [];
  const tolerated = new Set(Array.isArray(predicted?.degrades) ? predicted.degrades.filter(isName) : []);

  if (recovers.length === 0) {
    return { verdict: 'INCONCLUSIVE', reason: 'the plan predicted no service would recover, so there is nothing to verify' };
  }

  const detail = [];
  let measurable = 0, fullyHealthy = 0, improved = 0, unmeasurable = [];

  for (const name of recovers) {
    const b = before.services[name];
    const a = after.services[name];
    if (!b || !a || b.rank === null || a.rank === null) {
      unmeasurable.push(name);
      detail.push({ service: name, before: b?.state ?? 'ABSENT', after: a?.state ?? 'ABSENT', measurable: false });
      continue;
    }
    measurable += 1;
    if (a.rank === 0) fullyHealthy += 1;
    if (a.rank < b.rank) improved += 1;
    detail.push({ service: name, before: b.state, after: a.state, measurable: true,
                  improved: a.rank < b.rank, healthy: a.rank === 0 });
  }

  // Not enough of the predicted set is visible to judge. Missing evidence is not evidence
  // that the action failed.
  if (measurable === 0) {
    return { verdict: 'INCONCLUSIVE', reason: `none of the predicted services were measurable (${unmeasurable.join(', ')})`, detail };
  }

  // Anything that got worse and was NOT predicted to. A predicted trade-off is the plan
  // working as described; an unpredicted one is the action doing harm.
  const regressions = [];
  for (const [name, a] of Object.entries(after.services)) {
    const b = before.services[name];
    if (!b || b.rank === null || a.rank === null) continue;
    if (a.rank > b.rank && !tolerated.has(name)) {
      regressions.push({ service: name, before: b.state, after: a.state });
    }
  }

  const allHealthy = fullyHealthy === measurable && unmeasurable.length === 0;

  if (allHealthy && regressions.length === 0) {
    return { verdict: 'RECOVERED', reason: `all ${measurable} predicted services returned to HEALTHY`, detail, regressions };
  }
  if (allHealthy && regressions.length > 0) {
    // The predicted set recovered but something else got worse. That is not a clean win,
    // and the frozen enum has no verdict for "recovered with collateral damage".
    return { verdict: 'PARTIAL', reason: `predicted services recovered, but ${regressions.map((r) => `${r.service} ${r.before}->${r.after}`).join(', ')} regressed unpredicted`, detail, regressions };
  }
  if (improved > 0 || fullyHealthy > 0) {
    const parts = [`${fullyHealthy}/${measurable} predicted services HEALTHY`];
    if (unmeasurable.length) parts.push(`${unmeasurable.length} unmeasurable`);
    if (regressions.length) parts.push(`${regressions.length} unpredicted regression(s)`);
    return { verdict: 'PARTIAL', reason: parts.join(', '), detail, regressions };
  }
  if (regressions.length > 0) {
    // No frozen verdict expresses "made things worse". FAILED plus an explicit reason is
    // the honest mapping: it escalates and stops, which is the required behaviour anyway.
    return { verdict: 'FAILED', reason: `no predicted service improved and ${regressions.map((r) => `${r.service} ${r.before}->${r.after}`).join(', ')} regressed`, detail, regressions };
  }
  return { verdict: 'FAILED', reason: `no material improvement in any of the ${measurable} predicted services`, detail, regressions };
}

// ---------------------------------------------------------------------------
// The observation loop.
// ---------------------------------------------------------------------------

function createVerifier(opts = {}) {
  const observationWindows = opts.verifyWindows ?? derived.verifyWindows;
  const baselineWindows = opts.verifyBaselineWindows ?? derived.verifyBaselineWindows;
  const sustainWindows = opts.verifySustainWindows ?? derived.verifySustainWindows ?? control.debounceWindows;
  const maxSpan = opts.verifyMaxWindowSpan ?? derived.verifyMaxWindowSpan;

  let active = null;                 // the verification in progress
  const completed = new Map();       // `${incidentId}:${actionId}` -> Verdict

  const key = (incidentId, actionId) => `${incidentId}:${actionId}`;

  // Verification begins only for an action the data plane actually accepted. An
  // EXECUTION_FAILED action changed nothing, so there is nothing to observe.
  function start({ incident, actionRecord, predicted, telemetry }) {
    if (!incident || typeof incident.id !== 'string') return refuse('no active incident');
    // Validated with the frozen checker rather than a hand-rolled one: a record missing
    // its target or issuedAt is malformed, and binding evidence to a malformed identity is
    // how verification ends up describing the wrong action.
    const recordProblems = actionRecordProblems(actionRecord);
    if (recordProblems.length > 0) {
      return refuse(`ActionRecord is malformed: ${recordProblems[0]}`);
    }
    if (actionRecord.outcome !== 'APPLIED') {
      return refuse(`action ${actionRecord.actionId} is ${actionRecord.outcome}; nothing was applied to verify`);
    }
    const id = key(incident.id, actionRecord.actionId);
    if (completed.has(id)) return refuse(`${actionRecord.actionId} already verified as ${completed.get(id).verdict}`);
    if (active) {
      return active.id === id
        ? refuse(`${actionRecord.actionId} is already under verification`)
        : refuse(`verification of ${active.actionId} is already in progress`);
    }

    // Baseline: the newest windows already in the ring, i.e. the state of the system
    // immediately before the action. Selected by windowId, never by wall clock.
    const recent = (telemetry.recent(baselineWindows) || []).filter(usableWindow);
    if (recent.length < baselineWindows) {
      return refuse(`only ${recent.length} usable window(s) available; need ${baselineWindows} for a baseline`);
    }
    const baseline = recent.slice().reverse();          // oldest -> newest
    const anchorWindowId = Math.max(...baseline.map((w) => w.windowId));
    const predictedNorm = normalisePrediction(predicted);

    // Nothing was predicted to recover, so there is no proposition to test. Spending ten
    // windows discovering that would be theatre; say so now and escalate.
    if (predictedNorm.recovers.length === 0) {
      const verdict = immediateVerdict(actionRecord.actionId, baseline,
        'the plan predicted no service would recover, so there is nothing to verify');
      completed.set(id, verdict);
      console.warn(`[verifier] ${actionRecord.actionId} -> INCONCLUSIVE immediately: ${verdict.reason}`);
      return { ok: false, reason: verdict.reason, verdict };
    }

    active = {
      id,
      incidentId: incident.id,
      actionId: actionRecord.actionId,
      optionId: actionRecord.optionId ?? null,
      predicted: predictedNorm,
      anchorWindowId,
      baseline,
      before: summarise(baseline),
      observed: [],
      sustained: 0,
      startedAtWindowId: anchorWindowId,
      gaps: 0,
    };
    console.log(`[verifier] watching ${actionRecord.actionId} from window ${anchorWindowId}: ` +
      `expect ${active.predicted.recovers.join(', ') || 'nothing'} to recover` +
      `${active.predicted.degrades.length ? `, tolerating ${active.predicted.degrades.join(', ')}` : ''}`);
    return { ok: true, verification: publicView() };
  }

  // Feed every window that arrived since the last call. Returns a Verdict once there is
  // enough evidence, or null while still observing.
  function observe(windows) {
    if (!active) return null;

    for (const w of windows || []) {
      if (!usableWindow(w)) continue;
      if (w.windowId <= active.anchorWindowId) continue;         // pre-action, not evidence

      const last = active.observed[active.observed.length - 1];
      // A hole in the stream is a hole in the evidence. It is counted, never interpolated.
      if (last && w.windowId !== last.windowId + 1) active.gaps += 1;

      active.observed.push(w);

      // Positive evidence may end observation early, but only once it has held. Negative
      // evidence never ends it early: an action still settling has not failed yet.
      const after = summarise(tail(active.observed, sustainWindows));
      const trial = classifyVerdict(active.before, after, active.predicted);
      active.sustained = trial.verdict === 'RECOVERED' ? active.sustained + 1 : 0;

      if (trial.verdict === 'RECOVERED' && active.sustained >= sustainWindows) {
        return finish(trial, after, 'sustained recovery');
      }
      if (active.observed.length >= observationWindows) {
        return finish(...conclude());
      }
      if (w.windowId - active.anchorWindowId > maxSpan) {
        return finish({ verdict: 'INCONCLUSIVE', reason: `stream advanced ${w.windowId - active.anchorWindowId} windows past the action with only ${active.observed.length} usable window(s)` },
          summarise(active.observed), 'window budget exhausted');
      }
    }
    return null;
  }

  function conclude() {
    const after = summarise(tail(active.observed, Math.max(sustainWindows, 1)));
    // Gaps make the measurement period untrustworthy rather than negative.
    if (active.gaps > 0) {
      return [{ verdict: 'INCONCLUSIVE', reason: `${active.gaps} gap(s) in the ${active.observed.length}-window measurement period` }, after, 'telemetry gap'];
    }
    if (active.observed.length < sustainWindows) {
      return [{ verdict: 'INCONCLUSIVE', reason: `only ${active.observed.length} usable window(s) after the action` }, after, 'insufficient windows'];
    }
    return [classifyVerdict(active.before, after, active.predicted), after, 'observation complete'];
  }

  // Called when the observation period can no longer complete — a stalled stream, or the
  // supervisor's budget. Never converts silence into failure.
  function timeout(reason) {
    if (!active) return null;
    const after = summarise(active.observed);
    return finish({ verdict: 'INCONCLUSIVE', reason: reason || 'verification period elapsed without sufficient evidence' }, after, 'timeout');
  }

  function finish(trial, after, how) {
    const verdict = {
      actionId: active.actionId,
      verdict: trial.verdict,
      observedWindows: active.observed.map((w) => w.windowId),
      before: active.before.services,
      after: after.services,
      // P2 additions for the console. The frozen validator checks required fields only.
      reason: trial.reason,
      detail: trial.detail ?? [],
      regressions: trial.regressions ?? [],
      predicted: active.predicted,
      baselineWindows: active.baseline.map((w) => w.windowId),
      concludedBy: how,
    };
    validateVerdict(verdict);
    completed.set(active.id, verdict);
    console.log(`[verifier] ${active.actionId} -> ${verdict.verdict} (${how}): ${trial.reason}`);
    active = null;
    return verdict;
  }

  function refuse(reason) {
    return { ok: false, reason };
  }

  function publicView() {
    if (!active) return null;
    return {
      actionId: active.actionId, optionId: active.optionId, incidentId: active.incidentId,
      anchorWindowId: active.anchorWindowId, observed: active.observed.length,
      needed: observationWindows, gaps: active.gaps, predicted: active.predicted,
    };
  }

  function reset() {
    active = null;
    completed.clear();
  }

  return {
    start, observe, timeout, reset, classifyVerdict,
    isVerifying: () => active !== null,
    current: publicView,
    verdictFor: (incidentId, actionId) => completed.get(key(incidentId, actionId)) ?? null,
    status: () => ({ active: publicView(), completed: completed.size,
                     verdicts: [...completed.values()].map((v) => ({ actionId: v.actionId, verdict: v.verdict })) }),
  };
}

// A completed INCONCLUSIVE verdict for a verification that could never run.
function immediateVerdict(actionId, baseline, reason) {
  const before = summarise(baseline);
  const verdict = {
    actionId, verdict: 'INCONCLUSIVE', observedWindows: [],
    before: before.services, after: before.services,
    reason, detail: [], regressions: [],
    predicted: { recovers: [], degrades: [] },
    baselineWindows: baseline.map((w) => w.windowId),
    concludedBy: 'nothing to verify',
  };
  validateVerdict(verdict);
  return verdict;
}

function normalisePrediction(p) {
  return {
    recovers: Array.isArray(p?.recovers) ? p.recovers.filter(isName) : [],
    degrades: Array.isArray(p?.degrades) ? p.degrades.filter(isName) : [],
  };
}

function usableWindow(w) {
  return !!w && typeof w === 'object' && Number.isInteger(w.windowId)
    && !!w.services && typeof w.services === 'object' && !Array.isArray(w.services);
}

const isName = (v) => typeof v === 'string' && v.length > 0;
const tail = (arr, n) => arr.slice(Math.max(0, arr.length - n));
const finite = (v) => (typeof v === 'number' && Number.isFinite(v) ? v : null);
const mean = (a) => (a.length ? Math.round((a.reduce((x, y) => x + y, 0) / a.length) * 10) / 10 : null);

module.exports = { createVerifier, classifyVerdict, summarise, RANK };
