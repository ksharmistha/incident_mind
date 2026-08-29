'use strict';

// The control plane's only inbound telemetry path: P1's collector, one WindowAggregate per
// second over ws://127.0.0.1:4100/stream.
//
// The same client serves development and production. During development the socket is
// answered by tools/dev-windows.js, which binds the same port; at integration that process
// is stopped and P1's collector takes the port. Neither this file nor any agent changes.
//
// It must tolerate the collector not existing yet: connection failures are expected for the
// first several hours of the build and must never crash the control plane.

const WebSocket = require('ws');
const { check, PORTS } = require('./contracts');

const DEFAULT_URL = process.env.IM_COLLECTOR_URL || `ws://127.0.0.1:${PORTS.collector}/stream`;
const RING_SIZE = 120;               // ~2 minutes of 1 Hz windows
const RECONNECT_MIN_MS = 500;
const RECONNECT_MAX_MS = 5000;

const windows = [];                  // oldest -> newest
const listeners = new Set();

let socket = null;
let reconnectMs = RECONNECT_MIN_MS;
let connected = false;
let lastWindowId = null;
let gapCount = 0;
let invalidCount = 0;
let stopped = false;

function onWindow(fn) {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

// Newest first, which is how every consumer wants it: baselines, measure windows and the
// verifier all reason backwards from now.
function recent(n = 1) {
  return windows.slice(-n).reverse();
}

function all() {
  return windows.slice();
}

function status() {
  return {
    connected,
    url: DEFAULT_URL,
    windows: windows.length,
    lastWindowId,
    gapCount,
    invalidCount,
  };
}

function accept(agg) {
  if (!check('WindowAggregate', agg)) invalidCount += 1;

  // windowId is contracted to be strictly increasing with no gaps. A gap means the
  // collector restarted or we dropped a message, and every windowed calculation
  // downstream needs to know rather than quietly averaging across the hole.
  if (lastWindowId !== null && agg.windowId !== lastWindowId + 1) {
    gapCount += 1;
    console.warn(`[telemetry] windowId gap: ${lastWindowId} -> ${agg.windowId}`);
  }
  lastWindowId = agg.windowId;

  windows.push(agg);
  if (windows.length > RING_SIZE) windows.shift();

  for (const fn of listeners) {
    try {
      fn(agg);
    } catch (err) {
      console.error('[telemetry] listener threw:', err.message);
    }
  }
}

function connect(url = DEFAULT_URL) {
  if (stopped) return;
  socket = new WebSocket(url);

  socket.on('open', () => {
    connected = true;
    reconnectMs = RECONNECT_MIN_MS;
    console.log(`[telemetry] connected to ${url}`);
  });

  socket.on('message', (raw) => {
    let agg;
    try {
      agg = JSON.parse(raw);
    } catch {
      console.error('[telemetry] non-JSON message discarded');
      return;
    }
    accept(agg);
  });

  socket.on('close', () => {
    if (connected) console.warn('[telemetry] disconnected');
    connected = false;
    scheduleReconnect(url);
  });

  // Expected until P1's collector exists. Logged once per backoff step, not per attempt.
  socket.on('error', (err) => {
    if (connected) console.error('[telemetry] socket error:', err.message);
  });
}

function scheduleReconnect(url) {
  if (stopped) return;
  const delay = reconnectMs;
  reconnectMs = Math.min(reconnectMs * 2, RECONNECT_MAX_MS);
  setTimeout(() => connect(url), delay).unref();
}

function start(url = DEFAULT_URL) {
  stopped = false;
  console.log(`[telemetry] watching ${url} (waiting for the collector is normal)`);
  connect(url);
}

function stop() {
  stopped = true;
  if (socket) socket.close();
}

// Called by POST /reset. The window ring is observation history, and after a reset the
// old windows describe a system that no longer exists.
function clear() {
  windows.length = 0;
  lastWindowId = null;
  gapCount = 0;
  invalidCount = 0;
}

module.exports = { start, stop, clear, onWindow, recent, all, status };
