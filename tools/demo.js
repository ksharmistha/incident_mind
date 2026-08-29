'use strict';

// Developer-facing end-to-end demo of the control loop.
//
//   node tools/demo.js --dev --scenario f2 --approve
//
// It starts the synthetic telemetry source and the real control server as child processes,
// watches the control WebSocket, and narrates the lifecycle. It is a TOOL: it orchestrates
// processes and issues an approval when asked. No production control code does either.
//
// Everything it prints for a synthetic scenario is shaped input, not measurement. The
// banner says so on every run, and the summary says so again.

const path = require('path');
const { spawn } = require('child_process');
const WebSocket = require('ws');
const { PORTS, controlStateProblems } = require('../packages/contracts');

const SCENARIOS = ['healthy', 'spike', 'f1', 'f2'];

function parseArgs(argv) {
  const a = { scenario: 'f1', intervalMs: 500, approve: false, dev: false,
              seed: 7, timeoutMs: 90000, verify: null };
  for (let i = 0; i < argv.length; i++) {
    const k = argv[i];
    if (k === '--dev') a.dev = true;
    else if (k === '--approve') a.approve = true;
    else if (k === '--scenario') a.scenario = argv[++i];
    else if (k === '--interval') a.intervalMs = Number(argv[++i]);
    else if (k === '--seed') a.seed = Number(argv[++i]);
    else if (k === '--timeout') a.timeoutMs = Number(argv[++i]);
    else if (k === '--verify-success') a.verify = 'success';
    else if (k === '--verify-failure') a.verify = 'failure';
    else { console.error(`unknown argument ${k}`); process.exit(2); }
  }
  return a;
}

const children = [];
function child(script, args, tag) {
  const p = spawn(process.execPath, [path.join(__dirname, '..', script), ...args],
    { stdio: ['ignore', 'pipe', 'pipe'] });
  const keep = (buf) => { for (const line of buf.toString().split('\n')) if (line.trim()) log(tag, line); };
  p.stdout.on('data', keep);
  p.stderr.on('data', keep);
  children.push(p);
  return p;
}

let verbose = false;
function log(tag, line) { if (verbose) console.log(`   ${tag} ${line}`); }
function say(line) { console.log(line); }

function cleanup() {
  for (const p of children) { try { p.kill('SIGTERM'); } catch { /* already gone */ } }
}
process.on('SIGINT', () => { cleanup(); process.exit(130); });
process.on('exit', cleanup);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const control = `http://127.0.0.1:${PORTS.control}`;

async function waitForHealth(deadline) {
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${control}/health`, { signal: AbortSignal.timeout(500) });
      if (r.ok) return true;
    } catch { /* not up yet */ }
    await sleep(200);
  }
  return false;
}

// Narrates ControlState transitions. The state machine below is descriptive — it exists
// here and in the documentation, not as a production enum.
function phaseOf(s) {
  if (!s.incident) return 'IDLE';
  if (s.verdict) return s.verdict.verdict;
  if (s.plan?.verification) return 'VERIFYING';
  if (s.plan?.executing) return 'EXECUTING';
  if (s.plan?.action) return 'EXECUTED';
  if (s.plan) return s.plan.recommendedOptionId ? 'AWAITING_APPROVAL' : 'PLANNED_BLOCKED';
  if (s.probe) return `PROBING(${s.probe.phase ?? 'published'})`;
  if (s.hypotheses.length) return 'HYPOTHESES';
  return 'INCIDENT';
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (!args.dev) { console.error('refusing to run without --dev: this demo drives a synthetic source'); process.exit(2); }
  if (!SCENARIOS.includes(args.scenario)) { console.error(`--scenario must be one of ${SCENARIOS.join(', ')}`); process.exit(2); }

  const rule = '='.repeat(78);
  say(rule);
  say('  IncidentMind control-loop demo — SYNTHETIC INPUT');
  say('  NOT MEASURED. NOT EVIDENCE. Nothing here is a measurement of the real mesh.');
  say(`  scenario=${args.scenario} interval=${args.intervalMs}ms approve=${args.approve}`);
  say(rule);

  child('tools/dev-windows.js', ['--dev', '--scenario', args.scenario, '--seed', String(args.seed),
    '--interval', String(args.intervalMs)], '[telemetry]');
  await sleep(600);
  child('control/server.js', [], '[control]');

  if (!await waitForHealth(Date.now() + 15000)) {
    say('  control plane never became healthy'); cleanup(); process.exit(1);
  }
  say('  control plane ready\n');

  const seen = [];
  let lastPhase = null, approved = false, finished = false;
  const ws = new WebSocket(`ws://127.0.0.1:${PORTS.control}/stream`);

  ws.on('message', async (raw) => {
    let s;
    try { s = JSON.parse(raw); } catch { return; }
    const problems = controlStateProblems(s);
    if (problems.length) say(`  !! ControlState invalid: ${problems[0]}`);

    const phase = phaseOf(s);
    if (phase !== lastPhase) {
      seen.push(phase);
      say(`  ${String(phase).padEnd(20)} ${describe(s, phase)}`);
      lastPhase = phase;
    }

    // The one place this tool acts: an approval, and only when explicitly asked for.
    if (args.approve && !approved && phase === 'AWAITING_APPROVAL') {
      approved = true;
      const id = s.plan.recommendedOptionId;
      say(`  -> POST /approve ${id}`);
      const r = await fetch(`${control}/approve`, { method: 'POST',
        headers: { 'content-type': 'application/json' }, body: JSON.stringify({ optionId: id }) });
      const body = await r.json();
      say(`     HTTP ${r.status} ${body.actionRecord ? `${body.actionRecord.actionId} ${body.actionRecord.outcome}` : body.error}`);
    }
    if (s.verdict && !finished) { finished = true; setTimeout(() => finish(s, seen, args), 400); }
  });

  ws.on('error', (e) => say(`  ws error: ${e.message}`));
  setTimeout(async () => {
    if (!finished) {
      const s = await fetch(`${control}/debug/status`).then((r) => r.json()).then((j) => j.state).catch(() => null);
      finish(s, seen, args);
    }
  }, args.timeoutMs);
}

function describe(s, phase) {
  switch (phase) {
    case 'IDLE': return 'no incident';
    case 'INCIDENT': return `${s.incident.id} — ${s.incident.reason}`;
    case 'HYPOTHESES': {
      const [a, b] = s.hypotheses;
      return `${a.rootCauseService} ${a.score}${b ? ` vs ${b.rootCauseService} ${b.score}` : ''}`;
    }
    case 'AWAITING_APPROVAL':
      return `effConf ${s.plan.effectiveConfidence}, recommends ${s.plan.recommendedOptionId} (human gate)`;
    case 'PLANNED_BLOCKED':
      return `effConf ${s.plan.effectiveConfidence} — every option BLOCKED, nothing recommended`;
    case 'EXECUTING': return `${s.plan.executing.actionType} on ${s.plan.executing.target}`;
    case 'EXECUTED': return `${s.plan.action.actionId} ${s.plan.action.outcome} (HTTP ${s.plan.action.httpStatus})`;
    case 'VERIFYING': return `watching from window ${s.plan.verification.anchorWindowId}`;
    default: return s.verdict ? s.verdict.reason : '';
  }
}

function finish(s, seen, args) {
  const rule = '='.repeat(78);
  say(`\n${rule}`);
  say(`  phases: ${seen.join(' -> ') || 'IDLE'}`);
  if (s?.plan?.action) {
    say(`  action : ${s.plan.action.actionId} ${s.plan.action.outcome} (HTTP ${s.plan.action.httpStatus})`);
  }
  if (s?.verdict) {
    say(`  verdict: ${s.verdict.verdict} — ${s.verdict.reason}`);
    say(`           observed windows ${JSON.stringify(s.verdict.observedWindows)}`);
    say('  a 2xx from the data plane did not decide this. The windows above did.');
  } else {
    say('  verdict: none — no action was verified in this run');
  }
  say(`  scenario ${args.scenario} was SYNTHETIC. NOT MEASURED. NOT EVIDENCE.`);
  say(rule);
  cleanup();
  process.exit(0);
}

main().catch((e) => { say(`demo failed: ${e.message}`); cleanup(); process.exit(1); });
