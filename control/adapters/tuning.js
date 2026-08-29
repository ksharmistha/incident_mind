'use strict';

// Reads config/tuning.json, which P1 owns and calibrates against real CPU on the Windows
// machine. Read-only here: no agent may write a value back, and no value is duplicated.

const tuning = require('../../config/tuning.json');
const { MAX_PROBE_FRACTION, MAX_PROBE_DURATION_MS } = require('../../packages/contracts');

const control = tuning.control;

// Thresholds the specification requires that config/tuning.json has no home for — checked
// against the real file, which carries only the five control.* keys. Gathered here rather
// than scattered through the agents, so probe calibration at H7 has one file to edit.
//
// The probe safety rails are deliberately NOT here: fraction and duration are bounds on
// what the control plane may do to a live system, and the frozen contract enforces them.
const derived = {
  // Refuse to probe below this observation confidence — you cannot measure a delta you
  // cannot see. P2 handoff §11.2.
  probeMinObservationConfidence: 0.40,

  // Probe discriminator class boundaries, as fractional drops in auth.selfP99.
  // PROVISIONAL until re-measured on P1's machine at H7 (P2 handoff §16). Adjust these
  // to fit the measured separation — never to make a demo pass.
  probeH1MinDropPct: 0.40,
  probeH2MaxDropPct: 0.15,

  // Posteriors after a conclusive probe. INCONCLUSIVE leaves posteriors unchanged.
  posteriorMatched: 0.93,
  posteriorOther: 0.07,

  // An incident also opens when a user-facing endpoint exceeds this error rate. Distinct
  // from control.detectErrRate, which is the per-service flag threshold.
  userFacingErrRate: 0.05,

  // Probe measurement windows, §11.2 steps 4 and 6.
  probeBaselineWindows: 2,
  probeSettlingWindows: 2,

  // The Verifier observes this many windows after an action, §10.5.
  verifyWindows: 10,
};

console.log(
  `[tuning] config/tuning.json (P1 authoritative) — margin<${control.probeMarginThreshold}` +
  ` autonomy>=${control.autonomyConfidence} probeFloor>=${derived.probeMinObservationConfidence}` +
  ` rails: fraction<=${MAX_PROBE_FRACTION} duration<=${MAX_PROBE_DURATION_MS}ms`
);

module.exports = { tuning, control, derived };
