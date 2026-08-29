'use strict';

// Calibration harness for the Detector and Scorer. Measurement only.
//
// It replays a corpus of WindowAggregates through the real agents and reports every
// component term, so weight decisions are made against numbers rather than intuition.
//
// Two rules it follows so that calibrating cannot quietly change the thing being measured:
//
//   1. It imports the production agents unmodified. No test hooks were added to them.
//   2. Weight sweeps are computed HERE, not in the scorer. score() is a linear function of
//      the four terms, so re-weighting a recorded term vector is arithmetically identical
//      to re-running the scorer with different weights — without touching production code.
//
// Fixture corpus (real, recorded from P1's mesh) is preferred. The synthetic generator is
// accepted only with --synthetic and every report it produces is labelled PROVISIONAL:
// numbers from a shaped generator are not calibration evidence.

const fs = require('fs');
const path = require('path');
const { createDetector } = require('../control/agents/detector');
const { createScorer, WEIGHTS } = require('../control/agents/scorer');
const { windowAggregateProblems } = require('../packages/contracts');
const { control, derived } = require('../control/adapters/tuning');

const FIXTURE_DIR = path.join(__dirname, '..', 'fixtures');
const TERMS = ['temporalPrecedence', 'upstreamness', 'amplificationTarget', 'sharedResourcePenalty'];

// ---------------------------------------------------------------------------
// Corpus loading
// ---------------------------------------------------------------------------

function findFixtures() {
  if (!fs.existsSync(FIXTURE_DIR)) return [];
  return fs.readdirSync(FIXTURE_DIR)
    .filter((f) => f.endsWith('.jsonl'))
    .map((f) => ({ name: f.replace(/\.jsonl$/, ''), file: path.join(FIXTURE_DIR, f), real: true }))
    .sort((a, b) => a.name.localeCompare(b.name));
}

function loadFixture(file) {
  const windows = [];
  const bad = [];
  const lines = fs.readFileSync(file, 'utf8').split('\n');
  lines.forEach((line, i) => {
    const trimmed = line.trim();
    if (!trimmed) return;
    let w;
    try { w = JSON.parse(trimmed); } catch { bad.push(`line ${i + 1}: not JSON`); return; }
    const problems = windowAggregateProblems(w);
    if (problems.length) bad.push(`line ${i + 1}: ${problems[0]}`);
    windows.push(w);
  });
  return { windows, bad };
}

function loadSynthetic(scenario, count) {
  const { buildWindow } = require('./dev-windows.js');
  const opts = { scenario, seed: 7, epoch: 1756450321000, lateDropped: true };
  return { windows: Array.from({ length: count }, (_, n) => buildWindow(n, opts)), bad: [] };
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

function replay(windows) {
  const detector = createDetector();
  const scorer = createScorer();
  const frames = [];
  let openedAt = null;

  for (const w of windows) {
    const incident = detector.observe(w);
    if (!incident) continue;
    if (openedAt === null) openedAt = w.windowId;
    const result = scorer.score(incident, w);
    if (result.hypotheses.length === 0) continue;
    frames.push({
      windowId: w.windowId,
      observationConfidence: w.observationConfidence,
      incidentId: incident.id,
      flagged: incident.services,
      hypotheses: result.hypotheses.map((h) => ({
        id: h.id, service: h.rootCauseService, failureMode: h.failureMode,
        score: h.score, posterior: h.posterior, terms: h.terms,
      })),
      margin: result.margin,
      ambiguous: result.ambiguous,
    });
  }
  return { openedAt, frames, incidentDetected: openedAt !== null };
}

// Would the Experimenter's gate let a probe through on this frame? Mirrors the documented
// conditions without importing the Experimenter, so calibration cannot accidentally fire one.
function probeEligible(frame) {
  if (!frame) return { eligible: false, reason: 'no scored frame' };
  if (frame.hypotheses.length < 2) return { eligible: false, reason: 'fewer than two hypotheses' };
  if (!(frame.margin < control.probeMarginThreshold)) {
    return { eligible: false, reason: `margin ${frame.margin} >= ${control.probeMarginThreshold}` };
  }
  if (!(frame.observationConfidence >= derived.probeMinObservationConfidence)) {
    return { eligible: false, reason: `observationConfidence ${frame.observationConfidence} < ${derived.probeMinObservationConfidence}` };
  }
  return { eligible: true, reason: `margin ${frame.margin} < ${control.probeMarginThreshold}` };
}

// ---------------------------------------------------------------------------
// Analysis
// ---------------------------------------------------------------------------

// Re-weight a recorded term vector. Arithmetically identical to re-running the scorer.
function rescore(terms, weights) {
  return TERMS.reduce((sum, t) => sum + weights[t] * terms[t], 0);
}

function rankUnder(frame, weights) {
  const ranked = frame.hypotheses
    .map((h) => ({ ...h, score: round3(rescore(h.terms, weights)) }))
    .sort((a, b) => b.score - a.score || a.service.localeCompare(b.service));
  return {
    ranked,
    margin: ranked.length >= 2 ? round3(ranked[0].score - ranked[1].score) : null,
    top: ranked[0], second: ranked[1],
  };
}

// The ceiling each candidate can reach given its position in the observed graph. This is
// what decides whether a hypothesis is competitive at all, or is structurally excluded
// before any evidence is considered.
function ceilings(frame, weights) {
  return frame.hypotheses.map((h) => {
    // Terms fixed by topology rather than by the incident: a node's in-degree does not
    // change during an incident, and retry amplification lands where the retries are.
    const fixed = {
      amplificationTarget: h.terms.amplificationTarget,
      sharedResourcePenalty: h.terms.sharedResourcePenalty,
    };
    const best = weights.temporalPrecedence * 1 + weights.upstreamness * 1
      + weights.amplificationTarget * fixed.amplificationTarget
      + weights.sharedResourcePenalty * fixed.sharedResourcePenalty;
    const worst = weights.amplificationTarget * fixed.amplificationTarget
      + weights.sharedResourcePenalty * fixed.sharedResourcePenalty;
    return {
      service: h.service, actual: h.score, ceiling: round3(best), floor: round3(worst),
      fixedAmplification: fixed.amplificationTarget, fixedPenalty: fixed.sharedResourcePenalty,
    };
  }).sort((a, b) => b.ceiling - a.ceiling);
}

// Where does the gap between two candidates actually come from?
function gapAttribution(a, b, weights) {
  const rows = TERMS.map((t) => ({
    term: t,
    a: a.terms[t], b: b.terms[t],
    contribution: round3(weights[t] * (a.terms[t] - b.terms[t])),
  }));
  return { rows, total: round3(rows.reduce((s, r) => s + r.contribution, 0)) };
}

// Single-parameter sensitivity. For a chosen pair, solve for the weight value that would
// put their margin exactly at the ambiguity threshold, holding the other three fixed.
//
// score is linear in the terms, so gap(w) = w·(a_t − b_t) + (contributions of the others).
// Setting gap = threshold gives one equation in one unknown. A solution outside [0,1] — or
// one that flips a penalty into a reward — is not a calibration, it is an inversion of the
// term's meaning, and the harness says so rather than reporting a number.
function sensitivity(a, b, weights, threshold) {
  const rows = [];
  for (const term of TERMS) {
    const delta = a.terms[term] - b.terms[term];
    const others = TERMS.filter((t) => t !== term)
      .reduce((s, t) => s + weights[t] * (a.terms[t] - b.terms[t]), 0);
    const isPenalty = weights[term] < 0;
    let required = null, verdict;
    if (Math.abs(delta) < 1e-9) {
      verdict = 'no leverage — the two candidates score identically on this term';
    } else {
      required = round3((threshold - others) / delta);
      const magnitude = Math.abs(required);
      if (isPenalty && required > 0) verdict = 'REJECT — the penalty would have to become a reward';
      else if (!isPenalty && required < 0) verdict = 'REJECT — the weight would have to become negative, inverting the term';
      else if (magnitude > 1) verdict = 'REJECT — outside [0,1]';
      else verdict = `feasible: ${weights[term]} -> ${required}`;
    }
    rows.push({ term, current: weights[term], aTerm: a.terms[term], bTerm: b.terms[term], delta: round3(delta),
                othersContribute: round3(others), requiredWeight: required, verdict });
  }
  const gapNow = round3(TERMS.reduce((s, t) => s + weights[t] * (a.terms[t] - b.terms[t]), 0));
  return { pair: `${a.service} vs ${b.service}`, gapNow, threshold, rows };
}

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

function summarise(name, real, run, weights) {
  const last = run.frames[run.frames.length - 1] || null;
  const gate = probeEligible(last);
  return {
    corpus: name,
    real,
    incidentDetected: run.incidentDetected,
    openedAtWindow: run.openedAt,
    framesScored: run.frames.length,
    final: last && {
      windowId: last.windowId,
      observationConfidence: last.observationConfidence,
      top: last.hypotheses[0], second: last.hypotheses[1],
      margin: last.margin, ambiguous: last.ambiguous,
      ranking: last.hypotheses.map((h) => ({ service: h.service, score: h.score, posterior: h.posterior, failureMode: h.failureMode, terms: h.terms })),
      ceilings: ceilings(last, weights),
    },
    probeEligible: gate.eligible,
    probeReason: gate.reason,
  };
}

function human(s) {
  const L = [];
  L.push(`\n── ${s.corpus}${s.real ? '' : '   [PROVISIONAL — synthetic, not calibration evidence]'} ──`);
  if (!s.incidentDetected) { L.push('   no incident detected · no hypotheses · no probe'); return L.join('\n'); }
  L.push(`   incident opened at window ${s.openedAtWindow}, ${s.framesScored} scored frames`);
  const f = s.final;
  L.push(`   final window ${f.windowId}  observationConfidence ${f.observationConfidence}`);
  L.push('   service      failMode                 score   post    prec   upst   ampT    srp   ceiling');
  for (const r of f.ranking) {
    const c = f.ceilings.find((x) => x.service === r.service);
    L.push(`   ${r.service.padEnd(12)} ${r.failureMode.padEnd(23)} ${fmt(r.score)} ${fmt(r.posterior)}` +
      `  ${fmt(r.terms.temporalPrecedence)} ${fmt(r.terms.upstreamness)} ${fmt(r.terms.amplificationTarget)} ${fmt(r.terms.sharedResourcePenalty)}   ${fmt(c.ceiling)}`);
  }
  L.push(`   margin ${f.margin}  ambiguous ${f.ambiguous}  ->  probe ${s.probeEligible ? 'ELIGIBLE' : 'REFUSED'} (${s.probeReason})`);
  return L.join('\n');
}

const fmt = (v) => (v === null || v === undefined ? '  -  ' : Number(v).toFixed(3).padStart(6));
const round3 = (v) => Math.round(v * 1000) / 1000;

// ---------------------------------------------------------------------------

function main(argv) {
  const useSynthetic = argv.includes('--synthetic');
  const jsonOut = argv.includes('--json');
  const count = Number(argv[argv.indexOf('--windows') + 1]) || 45;

  const fixtures = findFixtures();
  const report = {
    generatedFrom: null,
    weights: { ...WEIGHTS },
    thresholds: { probeMarginThreshold: control.probeMarginThreshold, probeMinObservationConfidence: derived.probeMinObservationConfidence },
    fixturesFound: fixtures.map((f) => f.name),
    corpora: [],
  };

  let sources;
  if (fixtures.length > 0) {
    report.generatedFrom = 'fixtures';
    sources = fixtures.map((f) => ({ ...f, load: () => loadFixture(f.file) }));
  } else if (useSynthetic) {
    report.generatedFrom = 'synthetic (PROVISIONAL)';
    sources = ['healthy', 'spike', 'f1', 'f2'].map((s) => ({ name: s, real: false, load: () => loadSynthetic(s, count) }));
  } else {
    console.error('No fixtures found in fixtures/ and --synthetic not given.');
    console.error('Real calibration requires recorded WindowAggregate streams from P1\'s mesh.');
    console.error('Run with --synthetic for a PROVISIONAL measurement that is not calibration evidence.');
    process.exit(3);
  }

  if (!jsonOut) {
    console.log('='.repeat(84));
    console.log(`  CALIBRATION HARNESS — measurement only, no production behaviour is modified`);
    console.log(`  corpus: ${report.generatedFrom}`);
    console.log(`  weights: ${TERMS.map((t) => `${t}=${WEIGHTS[t]}`).join('  ')}`);
    console.log(`  probeMarginThreshold=${control.probeMarginThreshold}  probeFloor=${derived.probeMinObservationConfidence}`);
    console.log('='.repeat(84));
  }

  for (const src of sources) {
    const { windows, bad } = src.load();
    if (bad.length && !jsonOut) {
      console.warn(`   ${src.name}: ${bad.length} contract problems, first: ${bad[0]}`);
    }
    const run = replay(windows);
    const s = summarise(src.name, src.real !== false, run, WEIGHTS);
    s.contractProblems = bad.length;
    s.windows = windows.length;
    report.corpora.push(s);
    if (!jsonOut) console.log(human(s));
  }

  // Gap attribution between the top two of every corpus that opened an incident.
  if (!jsonOut) {
    for (const c of report.corpora) {
      if (!c.final || !c.final.second) continue;
      const a = c.final.ranking[0], b = c.final.ranking[1];
      const g = gapAttribution(a, b, WEIGHTS);
      console.log(`\n   gap attribution ${c.corpus}: ${a.service} over ${b.service} = ${g.total}`);
      for (const r of g.rows) {
        console.log(`     ${r.term.padEnd(22)} ${fmt(r.a)} vs ${fmt(r.b)}  ->  ${r.contribution >= 0 ? '+' : ''}${r.contribution}`);
      }
    }
  }

  // Sensitivity on the pair the runbook names as the competing hypotheses: the top
  // candidate against the shared resource it depends on.
  if (!jsonOut) {
    for (const c of report.corpora) {
      if (!c.final) continue;
      const top = c.final.ranking[0];
      const shared = c.final.ranking.find((r) => r.terms.sharedResourcePenalty === 1);
      if (!shared || shared.service === top.service) continue;
      const s = sensitivity(top, shared, WEIGHTS, control.probeMarginThreshold);
      report.sensitivity = report.sensitivity || [];
      report.sensitivity.push({ corpus: c.corpus, ...s });
      console.log(`\n   single-parameter sensitivity ${c.corpus}: ${s.pair}, gap ${s.gapNow} -> target ${s.threshold}`);
      for (const r of s.rows) {
        console.log(`     ${r.term.padEnd(22)} w=${String(r.current).padStart(5)} delta=${fmt(r.delta)}  ${r.verdict}`);
      }
    }
  }

  if (jsonOut) console.log(JSON.stringify(report, null, 2));
  return report;
}

module.exports = { replay, rankUnder, ceilings, gapAttribution, sensitivity, probeEligible, rescore, findFixtures, loadFixture, TERMS };

if (require.main === module) main(process.argv.slice(2));
