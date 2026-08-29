'use strict';

const express = require('express');
const { callJson } = require('../services/_shared/service');
const { PORTS } = require('../packages/contracts');
const tuning = require('../config/tuning.json');

// Closed-loop load: N virtual users, each one sending, awaiting, and sending again.
// Coordinated omission does not arise, because when the system slows the offered load
// falls — exactly as it does with real users staring at a spinner.

const GATEWAY = `http://127.0.0.1:${PORTS.gateway}`;
const { mix } = tuning.loadgen;

let targetVus = 0;
let activeVus = 0;
let nextUserId = 0;

let completed = 0;
let throughput = 0;
let failedCheckouts = 0;
const latencies = [];

// Per-endpoint tallies for the current second, and the last completed second. CP-A gates
// on checkout's error rate specifically, and deriving it from the traffic mix would be a
// guess — this counts the requests the virtual users actually sent and how they ended.
const ENDPOINTS = ['checkout', 'login', 'browse'];
const blank = () => Object.fromEntries(ENDPOINTS.map((e) => [e, { sent: 0, failed: 0 }]));
let current = blank();
let lastSecond = blank();

function record(endpoint, failed) {
  current[endpoint].sent++;
  if (failed) current[endpoint].failed++;
}

function pickEndpoint() {
  const r = Math.random();
  if (r < mix.checkout) return 'checkout';
  if (r < mix.checkout + mix.login) return 'login';
  return 'browse';
}

async function runVirtualUser(id) {
  activeVus++;
  while (id < targetVus) {
    const endpoint = pickEndpoint();
    const started = performance.now();
    try {
      // Same deterministic-deadline client the services use; a virtual user waits at
      // most 10s before giving up, exactly as before.
      const res = await callJson(
        `${GATEWAY}/api/${endpoint}`,
        { token: `tok-${id}`, userId: `u-${id}`, orderId: `o-${nextUserId++}` },
        10000
      );
      const failed = res.status >= 400;
      if (failed && endpoint === 'checkout') failedCheckouts++;
      record(endpoint, failed);
    } catch {
      if (endpoint === 'checkout') failedCheckouts++;
      record(endpoint, true);
    }
    completed++;
    latencies.push(performance.now() - started);
    if (latencies.length > 2000) latencies.shift();
  }
  activeVus--;
}

function setVus(n) {
  const previous = targetVus;
  targetVus = Math.max(0, Math.floor(n));
  for (let id = previous; id < targetVus; id++) runVirtualUser(id);
  console.log(`vus ${previous} -> ${targetVus}`);
  return { ok: true, vus: targetVus };
}

function p99() {
  if (latencies.length === 0) return 0;
  const sorted = [...latencies].sort((a, b) => a - b);
  return Number(sorted[Math.min(sorted.length - 1, Math.floor(0.99 * sorted.length))].toFixed(1));
}

const app = express();
app.use(express.json());

app.post('/load', (req, res) => res.json(setVus(req.body?.vus ?? 0)));

app.get('/stats', (req, res) => {
  const errRate = (e) => (lastSecond[e].sent > 0 ? lastSecond[e].failed / lastSecond[e].sent : 0);
  res.json({
    vus: targetVus,
    activeVus,
    throughput,
    p99: p99(),
    failedCheckouts,
    endpoints: Object.fromEntries(ENDPOINTS.map((e) => [e, { ...lastSecond[e], errRate: errRate(e) }])),
  });
});

app.get('/health', (req, res) => res.json({ svc: 'loadgen', up: true, version: null, faults: [] }));

setInterval(() => {
  throughput = completed;
  completed = 0;
  lastSecond = current;
  current = blank();
  if (targetVus > 0) {
    const c = lastSecond.checkout;
    const errPct = c.sent > 0 ? ((c.failed / c.sent) * 100).toFixed(1) : '0.0';
    console.log(
      `vus=${targetVus} throughput=${throughput}/s p99=${p99()}ms ` +
        `checkout sent=${c.sent} failed=${c.failed} err=${errPct}% failedCheckoutsTotal=${failedCheckouts}`
    );
  }
}, 1000).unref();

app.listen(PORTS.loadgen, '127.0.0.1', () => {
  console.log(`loadgen up on 127.0.0.1:${PORTS.loadgen}`);
  setVus(tuning.loadgen.vusDefault);
});
