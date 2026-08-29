'use strict';

// Per-agent timeouts, retries and failure policy - P2 handoff section 10.1.
//
// The point of the table is that no agent can hang the control plane. An agent that
// exceeds its budget loses; the supervisor returns that agent's documented exhaustion
// value and the pipeline carries on. Nothing here retries a production change silently:
// only the Executor gets a retry, and its failure value still says EXECUTION_FAILED.

const AGENTS = {
  detector:     { timeoutMs: 200,   retries: 0, onExhaustion: 'skip window, count the gap' },
  scorer:       { timeoutMs: 300,   retries: 0, onExhaustion: 'pure - cannot fail' },
  experimenter: { timeoutMs: 12000, retries: 0, onExhaustion: 'INCONCLUSIVE, posteriors unchanged' },
  planner:      { timeoutMs: 1000,  retries: 0, onExhaustion: 'no safe action, escalate' },
  executor:     { timeoutMs: 3000,  retries: 1, onExhaustion: 'EXECUTION_FAILED - never claims success' },
  verifier:     { timeoutMs: 20000, retries: 0, onExhaustion: 'INCONCLUSIVE, escalate to human' },
};

const gaps = {};   // agent -> count of runs that failed or timed out
for (const name of Object.keys(AGENTS)) gaps[name] = 0;

function withTimeout(promise, timeoutMs, name) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${name} exceeded ${timeoutMs}ms`)), timeoutMs);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

// Runs one agent under its budget. Always resolves - never rejects - so a caller can
// never take down the control plane by forgetting a catch.
//
// -> { ok: true, value, attempts, durationMs }
// -> { ok: false, error, timedOut, attempts, durationMs, onExhaustion }
async function run(name, fn, ...args) {
  const cfg = AGENTS[name];
  if (!cfg) throw new Error(`unknown agent "${name}" - add it to the supervisor table`);

  const started = Date.now();
  let lastError;

  for (let attempt = 1; attempt <= cfg.retries + 1; attempt++) {
    try {
      const value = await withTimeout(Promise.resolve(fn(...args)), cfg.timeoutMs, name);
      return { ok: true, value, attempts: attempt, durationMs: Date.now() - started };
    } catch (err) {
      lastError = err;
      if (attempt <= cfg.retries) {
        console.warn(`[supervisor] ${name} attempt ${attempt} failed (${err.message}), retrying`);
      }
    }
  }

  gaps[name] += 1;
  const timedOut = lastError.message.includes('exceeded');
  console.error(
    `[supervisor] ${name} exhausted after ${cfg.retries + 1} attempt(s): ${lastError.message}` +
    ` -> ${cfg.onExhaustion} (gaps: ${gaps[name]})`
  );
  return {
    ok: false,
    error: lastError,
    timedOut,
    attempts: cfg.retries + 1,
    durationMs: Date.now() - started,
    onExhaustion: cfg.onExhaustion,
  };
}

// The Detector runs on its own interval, independent of everything downstream. Telemetry
// and detection must never stall while the system waits for a human - if the console
// freezes during the approval gate, a judge will notice. Ticks never overlap.
function startInterval(name, everyMs, fn) {
  let running = false;
  const timer = setInterval(async () => {
    if (running) return;
    running = true;
    try {
      await run(name, fn);
    } finally {
      running = false;
    }
  }, everyMs);
  timer.unref();
  console.log(`[supervisor] ${name} on its own ${everyMs}ms interval`);
  return () => clearInterval(timer);
}

function stats() {
  return { gaps: { ...gaps } };
}

function resetStats() {
  for (const name of Object.keys(gaps)) gaps[name] = 0;
}

module.exports = { AGENTS, run, startInterval, stats, resetStats };
