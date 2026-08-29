'use strict';

// Reads config/tuning.json, which P1 owns and calibrates on the Windows machine.
// Falls back to control/dev/tuning-local.json until the repo arrives. Never written to.

const path = require('path');

const REAL = path.join(__dirname, '..', '..', 'config', 'tuning.json');
const DEV = path.join(__dirname, '..', 'dev', 'tuning-local.json');

let source = REAL;
let tuning;
try {
  tuning = require(REAL);
} catch {
  source = DEV;
  tuning = require(DEV);
}

const usingRealTuning = source === REAL;
console.log(
  `[tuning] using ${usingRealTuning ? 'config/tuning.json (P1 authoritative)' : 'control/dev/tuning-local.json (PROVISIONAL)'}`
);

const control = tuning.control || {};

// Thresholds the specification requires but config/tuning.json has no home for. They are
// gathered here rather than scattered through the agents, so probe calibration at H7 has
// exactly one file to edit. If the frozen tuning.json turns out to carry a probe block,
// these read from it instead and this object goes away.
const derived = {
  // Refuse to probe below this observation confidence - you cannot measure a delta you
  // cannot see. P2 handoff section 11.2.
  probeMinObservationConfidence: 0.40,
  // Probe discriminator class boundaries, as fractional drops in auth.selfP99.
  // PROVISIONAL until re-measured on P1's machine at H7 (P2 handoff section 16).
  probeH1MinDropPct: 0.40,
  probeH2MaxDropPct: 0.15,
  // Posteriors after a conclusive probe. INCONCLUSIVE leaves posteriors unchanged.
  posteriorMatched: 0.93,
  posteriorOther: 0.07,
  // Incident also opens when a user-facing endpoint exceeds this error rate.
  // Distinct from control.detectErrRate, which is the per-service flag threshold.
  userFacingErrRate: 0.05,
  // Probe measurement windows, per section 11.2 steps 4 and 6.
  probeBaselineWindows: 2,
  probeSettlingWindows: 2,
  // Verifier observes this many windows after an action, section 10.5.
  verifyWindows: 10,
};

module.exports = { tuning, control, derived, usingRealTuning };
