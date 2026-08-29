'use strict';

// The single import point for contract shapes and validators.
//
// Every agent requires this file, never packages/contracts or control/dev directly, so
// swapping the temporary stand-in for P1's frozen module is a one-line change here.
//
// Two assumptions live in the stand-in's validators and are flagged for integration:
//   A1  pipeline.lateDropped is OPTIONAL. All three PDFs mandate the counter and P1's
//       judge Q&A says it is published, but it is absent from the formal WindowAggregate
//       shape in all three. Accepted when present, never required, never read by an agent.
//   A2  observationTerms.* are normalised unweighted fractions in [0,1], with
//       watermarkLag = min(1, pipeline.watermarkLagMs / 5000). Evidence: the paired
//       IngestAck example carries watermarkLagMs 900, and 900/5000 is exactly the printed
//       0.18; raw ms is impossible because the watermark is defined as max(t) - 2000ms.

const path = require('path');

const REAL = path.join(__dirname, '..', '..', 'packages', 'contracts', 'index.js');
const DEV = path.join(__dirname, '..', 'dev', 'contracts-local.js');

let source = REAL;
let contracts;
try {
  contracts = require(REAL);
} catch {
  source = DEV;
  contracts = require(DEV);
}

const usingRealContracts = source === REAL;
const VALIDATE = process.env.IM_VALIDATE === '1';

console.log(
  `[contracts] using ${usingRealContracts ? 'packages/contracts (FROZEN)' : 'control/dev/contracts-local.js (TEMPORARY STAND-IN)'}` +
  `, IM_VALIDATE=${VALIDATE ? '1' : '0'}`
);

// A validation failure logs loudly with the offending object. It never throws in the
// request path - P1 handoff section 8.6.
function check(kind, value) {
  if (!VALIDATE) return true;
  const validator = contracts['validate' + kind];
  if (typeof validator !== 'function') {
    console.warn(`[contracts] no validate${kind} in the active contracts module`);
    return true;
  }
  const problems = validator(value);
  if (problems.length === 0) return true;
  console.error(`[contracts] ${kind} FAILED VALIDATION (${problems.length}):`);
  for (const p of problems) console.error(`  - ${p}`);
  console.error('  offending object:', JSON.stringify(value));
  return false;
}

module.exports = { ...contracts, check, usingRealContracts, VALIDATE };
