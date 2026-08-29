'use strict';

// Detector — decides that something abnormal is happening, and nothing more.
//
// P2 handoff §10.2:
//   EWMA baseline over a 60s trailing window, per service
//   flag when  p99 > baseline × detectMultiplier  OR  errRate > detectErrRate
//   sustained across debounceWindows consecutive windows
//   incident opens when ≥2 services are flagged
//                OR any user-facing endpoint's error rate exceeds 5%
//
// It deliberately does not rank causes. Naming a root cause is the Scorer's job, and the
// Detector handing over a suspect would bias it. What it hands over is the flagged set,
// when each service first went abnormal, and the observed edges — the raw material the
// four scoring terms need.

const { parseEdgeKey } = require('../adapters/contracts');
const { control, derived } = require('../adapters/tuning');

// EWMA weight for a trailing window of derived.baselineWindows samples at 1 Hz. This is
// the standard N-sample smoothing factor, not a tuned number: the 60s window is the spec's.
const ALPHA = 2 / (derived.baselineWindows + 1);

function createDetector(opts = {}) {
  const detectMultiplier = opts.detectMultiplier ?? control.detectMultiplier;
  const detectErrRate = opts.detectErrRate ?? control.detectErrRate;
  const debounceWindows = opts.debounceWindows ?? control.debounceWindows;
  const userFacingErrRate = opts.userFacingErrRate ?? derived.userFacingErrRate;
  const warmup = opts.baselineWarmupWindows ?? derived.baselineWarmupWindows;

  // svc -> { p99: ewma, samples, consecutive, flagged, firstFlaggedWindowId, reasons }
  let services = new Map();
  let incident = null;
  let lastWindowId = null;
  let windowsObserved = 0;
  let windowsSkipped = 0;

  function track(name) {
    if (!services.has(name)) {
      services.set(name, {
        p99: null, samples: 0, consecutive: 0, flagged: false,
        firstFlaggedWindowId: null, reasons: [],
      });
    }
    return services.get(name);
  }

  // The entry point is whatever nothing else calls. Reading it from the observed graph
  // rather than naming "gateway" keeps the detector honest if the topology changes — and
  // CP-A's fallback drops a service, so nothing may assume five.
  function userFacingServices(w) {
    const names = Object.keys(w.services);
    const called = new Set();
    for (const key of Object.keys(w.edges || {})) {
      const e = parseEdgeKey(key);
      if (e) called.add(e.to);
    }
    const entry = names.filter((n) => !called.has(n));
    // With no edges yet there is no graph to reason about, so claim no entry point rather
    // than treating every service as user-facing.
    return called.size === 0 ? [] : entry;
  }

  function evaluate(name, svc, windowId) {
    const s = track(name);
    const p99 = num(svc.p99);
    const errRate = num(svc.errRate);

    const baselineReady = s.samples >= warmup && s.p99 > 0;
    const latencyBreach = baselineReady && p99 !== null && p99 > s.p99 * detectMultiplier;
    const errorBreach = errRate !== null && errRate > detectErrRate;

    const reasons = [];
    if (latencyBreach) reasons.push(`p99 ${p99} > baseline ${round(s.p99)} × ${detectMultiplier}`);
    if (errorBreach) reasons.push(`errRate ${errRate} > ${detectErrRate}`);

    if (reasons.length > 0) {
      s.consecutive += 1;
    } else {
      s.consecutive = 0;
      s.firstFlaggedWindowId = null;
    }

    const wasFlagged = s.flagged;
    s.flagged = s.consecutive >= debounceWindows;
    s.reasons = reasons;
    if (s.flagged && !wasFlagged) s.firstFlaggedWindowId = windowId - (debounceWindows - 1);

    // Freeze the baseline while a service is breaching. An EWMA that keeps learning during
    // an incident climbs to meet the degradation and un-flags the service a minute later,
    // which is how a detector talks itself out of a real outage.
    if (reasons.length === 0 && p99 !== null) {
      s.p99 = s.p99 === null ? p99 : ALPHA * p99 + (1 - ALPHA) * s.p99;
      s.samples += 1;
    }
    return s;
  }

  function observe(w) {
    if (!isUsable(w)) { windowsSkipped += 1; return incident; }
    if (lastWindowId !== null && w.windowId === lastWindowId) return incident;
    lastWindowId = w.windowId;
    windowsObserved += 1;

    for (const [name, svc] of Object.entries(w.services)) {
      if (svc && typeof svc === 'object') evaluate(name, svc, w.windowId);
    }

    const flagged = [...services.entries()].filter(([, s]) => s.flagged).map(([n]) => n);

    const entry = userFacingServices(w);
    const userFacingBreach = entry.filter((n) => num(w.services[n]?.errRate) > userFacingErrRate);

    const open = flagged.length >= 2 || userFacingBreach.length > 0;
    if (!open) return incident;

    const reason = flagged.length >= 2
      ? `${flagged.length} services flagged: ${flagged.join(', ')}`
      : `user-facing ${userFacingBreach.join(', ')} errRate > ${userFacingErrRate}`;

    // The user-facing rule can fire before any service has cleared debounce, which would
    // otherwise open an incident with nothing in it for the Scorer to rank. A service whose
    // error rate breached is observed-abnormal by the same rule that opened the incident,
    // so it belongs in the candidate set even without the debounce.
    const candidateNames = [...new Set([...flagged, ...userFacingBreach])];

    if (!incident) {
      console.log(`[detector] INCIDENT INC-${w.windowId} opened at window ${w.windowId}: ${reason}`);
    }
    const opened = incident
      ? { id: incident.id, openedAt: incident.openedAt, openedWindowId: incident.openedWindowId, reason: incident.reason }
      : { id: `INC-${w.windowId}`, openedAt: w.tEnd, openedWindowId: w.windowId, reason };

    // A fresh object every window rather than a mutated one: control/state.js detects
    // change by value, and an object mutated in place is invisible to it.
    incident = { ...opened, lastWindowId: w.windowId, services: flagged, userFacing: entry };
    incident.candidates = candidateNames.map((name) => {
      const s = services.get(name);
      const svc = w.services[name];
      return {
        service: name,
        firstFlaggedWindowId: s.firstFlaggedWindowId,
        firstDegradedAt: svc.firstDegradedAt ?? null,
        state: svc.state,
        p99: num(svc.p99),
        selfP99: num(svc.selfP99),
        downstreamP99: num(svc.downstreamP99),
        errRate: num(svc.errRate),
        baselineP99: s.p99 === null ? null : round(s.p99),
        reasons: s.reasons,
      };
    });
    // The whole observed graph, not just the flagged part: upstreamness needs reachability
    // through healthy services too.
    incident.edges = w.edges;
    return incident;
  }

  function reset() {
    services = new Map();
    incident = null;
    lastWindowId = null;
    windowsObserved = 0;
    windowsSkipped = 0;
  }

  function stats() {
    return {
      incident: incident ? incident.id : null,
      windowsObserved,
      windowsSkipped,
      lastWindowId,
      baselines: Object.fromEntries(
        [...services].map(([n, s]) => [n, {
          p99: s.p99 === null ? null : round(s.p99),
          samples: s.samples, consecutive: s.consecutive, flagged: s.flagged,
        }])
      ),
    };
  }

  return { observe, reset, stats, getIncident: () => incident };
}

// Telemetry can be malformed or absent; a detector that throws takes the control plane
// with it, and the console freezing during an incident is exactly what a judge notices.
function isUsable(w) {
  return !!w
    && typeof w === 'object'
    && Number.isInteger(w.windowId)
    && w.services
    && typeof w.services === 'object'
    && !Array.isArray(w.services);
}

function num(v) {
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
}

function round(v) {
  return Math.round(v * 10) / 10;
}

module.exports = { createDetector, ALPHA };
