'use strict';

const { spawn } = require('node:child_process');
const path = require('node:path');
const readline = require('node:readline');
const { PORTS } = require('../packages/contracts');

// One command, the whole mesh. Processes are spawned as direct node children with
// spawn(process.execPath, ...) and never through a shell: Windows has no SIGTERM
// process-tree semantics, so an intermediate shell would leave orphans behind, and
// shell:true breaks argument quoting on paths that contain spaces.

// Started datastore-first so that by the time the gateway takes traffic, everything
// it depends on is already listening.
const PROCESSES = [
  { name: 'datastore', script: 'services/datastore/index.js', colour: 35 },
  { name: 'payments', script: 'services/payments/index.js', colour: 34 },
  { name: 'checkout', script: 'services/checkout/index.js', colour: 36 },
  { name: 'auth', script: 'services/auth/index.js', colour: 33 },
  { name: 'gateway', script: 'services/gateway/index.js', colour: 32 },
  { name: 'collector', script: 'collector/server.js', colour: 95 },
  // The control plane starts last: it retries its collector connection with backoff, so
  // ordering is a convenience rather than a requirement, but starting it after the thing
  // it subscribes to keeps the boot log readable.
  { name: 'control', script: 'control/server.js', colour: 96 },
  { name: 'loadgen', script: 'loadgen/index.js', colour: 90 },
  // The console is Vite's dev server. Started the same way as everything else — a direct
  // node child running vite's own entry script, never through a shell — so Ctrl-C takes it
  // down with the rest and argument quoting cannot break on a path containing spaces.
  // Its /health is proxied through to the control plane, so the READY check works unchanged.
  { name: 'console', script: 'console/node_modules/vite/bin/vite.js', cwd: 'console', colour: 94 },
];

const root = path.join(__dirname, '..');
const children = new Map();

// Every line is timestamped. With several processes emitting 1Hz counters, correlating
// "the queue started climbing" with "amplification crossed 2" is the whole job during
// rho tuning, and it cannot be done without a clock on each line.
function stamp() {
  const d = new Date();
  const p = (n, w = 2) => String(n).padStart(w, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}

function prefix(name, colour, stream, line) {
  stream.write(`${stamp()} \u001b[${colour}m${name.padEnd(9)}\u001b[0m ${line}\n`);
}

function start(proc) {
  const child = spawn(process.execPath, [path.join(root, proc.script)], {
    cwd: proc.cwd ? path.join(root, proc.cwd) : root,
    stdio: ['ignore', 'pipe', 'pipe'],
    env: process.env,
  });

  // Split on newlines by hand rather than with readline. A readline Interface pauses and
  // resumes its source stream, and under sustained output that cycling was killing child
  // processes on Windows with a bare fast-fail exit (0xC0000409, no stack). A plain data
  // handler never pauses the stream.
  for (const [source, stream] of [[child.stdout, process.stdout], [child.stderr, process.stderr]]) {
    let pending = '';
    source.setEncoding('utf8');
    source.on('data', (chunk) => {
      pending += chunk;
      const lines = pending.split('\n');
      pending = lines.pop();
      for (const line of lines) prefix(proc.name, proc.colour, stream, line);
    });
  }

  child.on('exit', (code, signal) => {
    if (!shuttingDown) prefix(proc.name, proc.colour, process.stderr, `exited (code=${code} signal=${signal})`);
    children.delete(proc.name);
  });

  children.set(proc.name, child);
  return child;
}

async function waitForHealth(name, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://127.0.0.1:${PORTS[name]}/health`, { signal: AbortSignal.timeout(500) });
      if (res.ok) return true;
    } catch {
      // not listening yet
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  return false;
}

let shuttingDown = false;

function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  for (const child of children.values()) child.kill();
  setTimeout(() => process.exit(0), 300);
}

async function main() {
  for (const proc of PROCESSES) start(proc);

  const results = await Promise.all(PROCESSES.map((p) => waitForHealth(p.name)));
  const down = PROCESSES.filter((p, i) => !results[i]).map((p) => p.name);

  if (down.length > 0) {
    console.log(`\n\u001b[31mNOT READY — no health from: ${down.join(', ')}\u001b[0m\n`);
  } else {
    console.log(`\n\u001b[32m${'='.repeat(58)}\n  READY — ${PROCESSES.length} processes up on 127.0.0.1\n${'='.repeat(58)}\u001b[0m\n`);
  }
  console.log('commands:  kill <name>   restart <name>   quit\n');

  readline.createInterface({ input: process.stdin }).on('line', async (line) => {
    const [command, name] = line.trim().split(/\s+/);
    const proc = PROCESSES.find((p) => p.name === name);

    if (command === 'quit') return shutdown();
    if (!proc) return console.log(`unknown process "${name}" — one of: ${PROCESSES.map((p) => p.name).join(', ')}`);

    if (command === 'kill') {
      children.get(name)?.kill();
      console.log(`killed ${name}`);
    } else if (command === 'restart') {
      children.get(name)?.kill();
      setTimeout(async () => {
        start(proc);
        console.log(await waitForHealth(name) ? `${name} back up` : `${name} did not come back`);
      }, 300);
    } else {
      console.log('commands: kill <name> | restart <name> | quit');
    }
  });
}

process.on('SIGINT', shutdown);
main();
