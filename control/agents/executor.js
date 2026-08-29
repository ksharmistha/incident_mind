'use strict';

// Executor — the only component that turns an approved option into a real change.
//
// P2 handoff §10.5:
//   Executor  POST /admin/* on the target. Records ActionRecord.
//             On failure → EXECUTION_FAILED. NEVER claims success.
//
// Three things this file is careful about:
//
//   It has no HTTP client. Every side effect goes through control/adapters/admin.js, whose
//   four-entry allowlist makes the operator-only fault-injection routes unrepresentable.
//   There is no fetch here and no URL is composed here.
//
//   An ActionRecord is created only once an outcome is known. The frozen contract has no
//   pending state — outcome must be APPLIED or EXECUTION_FAILED — so a record that exists
//   is a record of something that actually happened.
//
//   APPLIED means the intervention was accepted by the data plane. It is NOT a claim that
//   anything recovered. A 200 OK is not evidence; only the Verifier, reading
//   WindowAggregates, decides whether the system got better.

const { validateActionRecord, ACTION_TYPES } = require('../adapters/contracts');
const adminAdapter = require('../adapters/admin');

// How each planner action reaches the data plane. This table is the complete set of things
// the Executor can do — an actionType absent from it cannot be executed, whatever a plan
// says. Every entry maps to an allowlisted /admin/* route.
const ACTIONS = {
  rollback: {
    adminAction: 'version',
    required: ['version'],
    body: (opt) => ({ version: opt.params.version }),
    describe: (opt) => `set ${opt.target} version to ${opt.params.version}`,
  },
  isolate: {
    adminAction: 'breaker',
    required: ['upstream'],
    body: (opt) => ({ upstream: opt.params.upstream, open: opt.params.open !== false }),
    // Routed through setBreaker rather than the generic call, because the adapter records
    // which breakers are open. That record is the ONLY thing the Experimenter can consult
    // to honour "never probe an edge whose breaker is already open" — the WindowAggregate
    // carries no breaker state. Calling admin.call directly here would open a real breaker
    // and leave the probe gate blind to it.
    perform: (admin, opt, timeoutMs) =>
      admin.setBreaker(opt.target, opt.params.upstream, opt.params.open !== false, timeoutMs),
    describe: (opt) => `${opt.params.open === false ? 'close' : 'open'} ${opt.target}'s breaker on ${opt.params.upstream}`,
  },
  restart: {
    adminAction: 'restart',
    required: [],
    body: () => ({}),
    describe: (opt) => `restart ${opt.target}`,
  },
};

// Below the supervisor's 3000ms budget, so a hung data plane becomes a recorded
// EXECUTION_FAILED here rather than an agent the supervisor has to kill.
const DEFAULT_TIMEOUT_MS = 2500;

function createExecutor(opts = {}) {
  const admin = opts.admin ?? adminAdapter;
  const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;

  // Execution identity, derived from identifiers that already exist rather than invented:
  // an option id is unique within a plan, and a plan belongs to one incident.
  const executed = new Map();     // `${incidentId}:${optionId}` -> ActionRecord
  const approvals = new Set();    // `${incidentId}:${optionId}`
  let inFlight = null;
  let lastRecord = null;

  const key = (incidentId, optionId) => `${incidentId}:${optionId}`;

  // -------------------------------------------------------------------------
  // Approval. Recorded against the incident it was granted for, so an approval
  // cannot survive into a different incident.
  // -------------------------------------------------------------------------
  function approve(incident, plan, optionId) {
    const check = locate(incident, plan, optionId);
    if (!check.ok) return check;
    if (check.option.autonomy === 'BLOCKED') {
      return { ok: false, code: 'BLOCKED', reason: `option ${optionId} is BLOCKED: ${check.option.gateReason}` };
    }
    approvals.add(key(incident.id, optionId));
    console.log(`[executor] approval recorded for ${optionId} on ${incident.id}`);
    return { ok: true, option: check.option };
  }

  function isApproved(incidentId, optionId) {
    return approvals.has(key(incidentId, optionId));
  }

  // -------------------------------------------------------------------------
  // Gating.
  // -------------------------------------------------------------------------
  function locate(incident, plan, optionId) {
    if (!incident || typeof incident !== 'object' || typeof incident.id !== 'string') {
      return { ok: false, code: 'NO_INCIDENT', reason: 'no active incident' };
    }
    if (!plan || !Array.isArray(plan.options)) {
      return { ok: false, code: 'NO_PLAN', reason: 'no plan to execute from' };
    }
    if (typeof optionId !== 'string' || optionId.length === 0) {
      return { ok: false, code: 'NO_OPTION', reason: 'no option selected' };
    }
    const option = plan.options.find((o) => o && o.id === optionId);
    if (!option) {
      return { ok: false, code: 'UNKNOWN_OPTION', reason: `option "${optionId}" is not in the current plan` };
    }
    return { ok: true, option };
  }

  function eligible(incident, plan, optionId) {
    const found = locate(incident, plan, optionId);
    if (!found.ok) return found;
    const option = found.option;

    if (!ACTION_TYPES.includes(option.actionType) || !ACTIONS[option.actionType]) {
      return { ok: false, code: 'UNSUPPORTED_ACTION', reason: `"${option.actionType}" is not an executable action type` };
    }
    if (typeof option.target !== 'string' || option.target.length === 0) {
      return { ok: false, code: 'MALFORMED_OPTION', reason: `option ${optionId} has no target` };
    }
    const spec = ACTIONS[option.actionType];
    const params = option.params && typeof option.params === 'object' ? option.params : {};
    for (const p of spec.required) {
      if (params[p] === undefined || params[p] === null || params[p] === '') {
        return { ok: false, code: 'MALFORMED_OPTION', reason: `option ${optionId} (${option.actionType}) is missing params.${p}` };
      }
    }

    // A previous run of this exact action already happened. Return that record rather than
    // doing it again: the supervisor loop and a retried /approve must not both fire.
    const prior = executed.get(key(incident.id, optionId));
    if (prior) return { ok: false, code: 'ALREADY_EXECUTED', reason: `${optionId} already executed as ${prior.actionId}`, record: prior };

    if (inFlight) {
      return { ok: false, code: 'IN_FLIGHT', reason: `execution of ${inFlight} is already in progress` };
    }

    // The autonomy gate, enforced here and not merely described by the Planner. HUMAN is
    // never silently downgraded: the only way past it is a recorded approval.
    if (option.autonomy === 'BLOCKED') {
      return { ok: false, code: 'BLOCKED', reason: `option ${optionId} is BLOCKED: ${option.gateReason}` };
    }
    if (option.autonomy === 'HUMAN' && !isApproved(incident.id, optionId)) {
      return { ok: false, code: 'APPROVAL_REQUIRED', reason: `${option.actionType} on ${option.target} requires human approval: ${option.gateReason}` };
    }
    if (option.autonomy !== 'AUTONOMOUS' && option.autonomy !== 'HUMAN') {
      return { ok: false, code: 'MALFORMED_OPTION', reason: `option ${optionId} has an unrecognised autonomy "${option.autonomy}"` };
    }
    return { ok: true, option };
  }

  // -------------------------------------------------------------------------
  // Execution.
  // -------------------------------------------------------------------------
  async function execute({ incident, plan, optionId, publish }) {
    const gate = eligible(incident, plan, optionId);
    if (!gate.ok) {
      console.log(`[executor] refused ${optionId}: ${gate.reason}`);
      return { ok: false, refusal: gate, record: gate.record ?? null };
    }

    const option = gate.option;
    const spec = ACTIONS[option.actionType];
    const actionId = `ACT-${incident.id}-${option.id}`;
    inFlight = option.id;

    // Publish intent before the side effect, so the console shows an action in progress
    // even if the data plane never answers.
    if (typeof publish === 'function') {
      try {
        await publish({ actionId, optionId: option.id, target: option.target,
                        actionType: option.actionType, status: 'executing', issuedAt: Date.now() });
      } catch (err) {
        console.error(`[executor] publish hook threw: ${err.message}`);
      }
    }

    const issuedAt = Date.now();
    console.log(`[executor] ${actionId}: ${spec.describe(option)}`);

    let res;
    try {
      res = spec.perform
        ? await spec.perform(admin, option, timeoutMs)
        : await admin.call(option.target, spec.adminAction, spec.body(option), timeoutMs);
    } catch (err) {
      // The adapter throws only for a programming error — an action outside its allowlist,
      // or an unknown service. That is a refusal to act, never a silent success.
      res = { ok: false, httpStatus: 0, error: err.message };
    } finally {
      inFlight = null;
    }

    const record = buildRecord({ actionId, option, issuedAt, res });
    executed.set(key(incident.id, option.id), record);
    lastRecord = record;
    validateActionRecord(record);

    console.log(`[executor] ${actionId} -> ${record.outcome}` +
      `${record.httpStatus === null ? ' (no HTTP status)' : ` (HTTP ${record.httpStatus})`}` +
      `${record.error ? ` — ${record.error}` : ''}`);

    return { ok: record.outcome === 'APPLIED', record, refusal: null };
  }

  function reset() {
    executed.clear();
    approvals.clear();
    inFlight = null;
    lastRecord = null;
  }

  function stats() {
    return {
      inFlight,
      executedCount: executed.size,
      approvals: [...approvals],
      lastRecord,
      records: [...executed.values()],
    };
  }

  return {
    execute, approve, eligible, isApproved, reset, stats,
    recordFor: (incidentId, optionId) => executed.get(key(incidentId, optionId)) ?? null,
    isInFlight: () => inFlight !== null,
    ACTIONS,
  };
}

// The contract allows httpStatus to be null but not an out-of-range number, and the adapter
// reports 0 when the request never reached a server. A transport failure has no HTTP status,
// so it is recorded as null rather than as a fabricated code.
function httpStatusOf(res) {
  const n = res && res.httpStatus;
  return Number.isInteger(n) && n >= 100 && n <= 599 ? n : null;
}

function buildRecord({ actionId, option, issuedAt, res }) {
  const httpStatus = httpStatusOf(res);
  // APPLIED means the data plane accepted the intervention — a 2xx, and no explicit
  // rejection in the body. It does NOT mean the incident is over.
  const accepted = !!(res && res.ok) && httpStatus !== null
    && !(res.body && typeof res.body === 'object' && res.body.ok === false);

  const record = {
    actionId,
    optionId: option.id,
    issuedAt,
    target: option.target,
    httpStatus,
    outcome: accepted ? 'APPLIED' : 'EXECUTION_FAILED',
    // Diagnostics. Enough to explain a failure without ever implying it succeeded.
    actionType: option.actionType,
    reversible: option.reversible,
  };
  if (!accepted) {
    record.error = (res && (res.error || (res.body && res.body.error))) ||
      (httpStatus === null ? 'no response from the data plane' : `HTTP ${httpStatus}`);
  }
  return record;
}

module.exports = { createExecutor, ACTIONS, DEFAULT_TIMEOUT_MS };
