'use strict';

// The single import point for contract shapes and validators.
//
// Every agent requires this file rather than packages/contracts directly, so the frozen
// module has exactly one entry point into the control plane. That is also what made the
// switch away from the development stand-in a one-file change.
//
// The frozen module offers two tiers (its own section header says so):
//   xProblems(obj)  pure, always runs, returns problem strings
//   validateX(obj)  gated on IM_VALIDATE=1, returns a boolean, logs loudly, never throws
// The request path uses the gated tier; harnesses and tests use the pure tier.

const contracts = require('../../packages/contracts');

// Returns true when the object is valid or validation is off. Never throws, so a bad
// object can never become an outage in the system whose outages we are observing.
function check(kind, value) {
  const validate = contracts['validate' + kind];
  if (typeof validate !== 'function') {
    console.warn(`[contracts] no validate${kind} in packages/contracts`);
    return true;
  }
  return validate(value);
}

// Returns the problem strings regardless of IM_VALIDATE. For tests and harnesses.
function problems(kind, value) {
  const fn = contracts[kind.charAt(0).toLowerCase() + kind.slice(1) + 'Problems'];
  return typeof fn === 'function' ? fn(value) : [];
}

// Edge keys are the only place the contract records an edge's direction — the edge object
// itself carries no from/to — so the scorer's amplificationTarget and upstreamness terms
// depend entirely on splitting this key correctly. Getting it wrong zeroes two of four
// terms silently rather than loudly.
//
// The contract requires U+2192 and P1's validator checks for it, but it permits
// surrounding whitespace, and the runbook renders the key spaced. Split tolerantly.
function parseEdgeKey(key) {
  if (typeof key !== 'string') return null;
  const parts = key.split(/\s*(?:→|->|=>)\s*/);
  if (parts.length !== 2) return null;
  const from = parts[0].trim();
  const to = parts[1].trim();
  if (!from || !to) return null;
  return { from, to };
}

console.log(
  `[contracts] packages/contracts (FROZEN), IM_VALIDATE=${contracts.validationEnabled ? '1' : '0'}`
);

module.exports = { ...contracts, check, problems, parseEdgeKey };
