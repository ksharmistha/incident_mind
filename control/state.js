'use strict';

// The control plane's entire state, held in memory. A restart loses the current incident;
// that is an accepted limitation of the 10-hour scope, not an oversight.
//
// ControlState is exactly the five contract fields - { incident, hypotheses[], probe, plan,
// verdict } - and nothing else, because P1 validates this object at integration. Anything
// operational (agent timings, window counts) stays out of it.
//
// Every mutation goes through update(). There is no state library: the WebSocket is the
// state distribution mechanism.

const { check } = require('./adapters/contracts');

function emptyState() {
  return { incident: null, hypotheses: [], probe: null, plan: null, verdict: null };
}

let state = emptyState();
const listeners = new Set();

// Registered by the Experimenter so POST /reset can stop an experiment that is mid-flight.
// Held here rather than in the experimenter because reset must work from any state.
let abortProbe = null;

function get() {
  return state;
}

function subscribe(fn) {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

function broadcast(reason) {
  check('ControlState', state);
  for (const fn of listeners) {
    try {
      fn(state, reason);
    } catch (err) {
      console.error('[state] listener threw:', err.message);
    }
  }
}

// patch is a partial ControlState. Broadcasts only when something actually changed, so a
// console redraw means a real state transition rather than a heartbeat.
function update(patch, reason) {
  const next = { ...state, ...patch };
  if (JSON.stringify(next) === JSON.stringify(state)) return false;
  state = next;
  console.log(`[state] ${reason}`);
  broadcast(reason);
  return true;
}

function onProbeAbort(fn) {
  abortProbe = fn;
}

// Clears everything and safely stops an in-flight probe. Returns a summary of what was
// cleared. Frozen inbound contract: P1's tools/reset.js calls POST /reset and needs a
// fast 200, so this does no network work and never awaits telemetry.
function reset() {
  const cleared = {
    incident: state.incident !== null,
    hypotheses: state.hypotheses.length,
    probe: state.probe !== null,
    plan: state.plan !== null,
    verdict: state.verdict !== null,
    probeAborted: false,
  };

  if (abortProbe) {
    try {
      abortProbe();
      cleared.probeAborted = true;
    } catch (err) {
      console.error('[state] probe abort failed:', err.message);
    }
  }

  state = emptyState();
  console.log('[state] reset:', JSON.stringify(cleared));
  broadcast('reset');
  return cleared;
}

module.exports = { get, update, reset, subscribe, onProbeAbort, emptyState };
