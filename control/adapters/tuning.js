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

  // The Detector's EWMA baseline is specified as "over a 60s trailing window" (§10.2).
  // At the collector's 1 Hz cadence that is 60 samples; the EWMA weight is derived from
  // it rather than chosen.
  baselineWindows: 60,
  // An EWMA needs samples before its value means anything. Flagging against a baseline
  // built from two windows produces noise, not detection. This is an implementation
  // necessity, not a tuned sensitivity — raising it delays first detection, it does not
  // change what counts as abnormal.
  baselineWarmupWindows: 10,

  // temporalPrecedence decays with lag. Short on purpose: §10.3 says precedence within
  // one or two windows is unreliable under a fast ramp, so services that degrade within a
  // few seconds of each other must score near-equally instead of the earliest taking all
  // the weight. This shapes the term's response, it is not a pass/fail threshold.
  precedenceDecaySeconds: 5,

  // Probe measurement windows, §11.2 steps 4 and 6.
  probeBaselineWindows: 2,
  probeSettlingWindows: 2,

  // The Verifier observes this many windows after an action, §10.5.
  verifyWindows: 10,
  // Baseline for verification: the windows immediately before the action, mirroring the
  // probe's two-window convention (§11.2 step 4). One window is a sample, not a baseline.
  verifyBaselineWindows: 2,
  // How long a condition must hold before it counts as evidence. Deliberately reuses the
  // Detector's debounce rather than introducing a second notion of "sustained": one good
  // window is noise in the same way one bad window is.
  verifySustainWindows: control.debounceWindows,
  // Give up and report INCONCLUSIVE if the stream advances this far past the action
  // without producing enough clean windows. 3x the observation budget tolerates a couple
  // of gaps without waiting forever on a stream that will never deliver.
  verifyMaxWindowSpan: 30,
};

console.log(
  `[tuning] config/tuning.json (P1 authoritative) — margin<${control.probeMarginThreshold}` +
  ` autonomy>=${control.autonomyConfidence} probeFloor>=${derived.probeMinObservationConfidence}` +
  ` rails: fraction<=${MAX_PROBE_FRACTION} duration<=${MAX_PROBE_DURATION_MS}ms`
);

module.exports = { tuning, control, derived };
