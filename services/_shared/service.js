'use strict';

const express = require('express');
const http = require('node:http');
const { PORTS } = require('../../packages/contracts');
const { Emitter } = require('../../packages/telemetry/emitter');
const { Counters } = require('../../packages/telemetry/counters');
const { middleware, recordDownstream, startCounterFlush } = require('../../packages/telemetry/instrument');

// Boots one service of the mesh. Every service has the same three surfaces:
//   /health        liveness plus whatever the service wants to expose about itself
//   /chaos/*       the OPERATOR breaks things here. The control plane may never call it.
//   /admin/*       the control plane fixes things here. It is the only write path in.
// The service supplies handler tables; this file does the express plumbing and nothing else.

function startService({ svc, routes, chaos, admin, health, op, resource }) {
  const port = PORTS[svc];
  if (!port) throw new Error(`no port assigned to service "${svc}"`);

  const emitter = new Emitter(svc);
  const counters = new Counters();
  if (resource) counters.resource = resource;

  const app = express();
  app.use(express.json());
  app.use(middleware({ emitter, counters, op: op ?? svc }));

  app.get('/health', (req, res) => {
    // /health is operator traffic, not mesh traffic; it still passes through the
    // middleware but is cheap and constant, so it does not distort the samples.
    const mem = process.memoryUsage();
    res.json({
      svc,
      up: true,
      version: null,
      faults: [],
      mem: { heapUsedMb: Math.round(mem.heapUsed / 1048576), rssMb: Math.round(mem.rss / 1048576) },
      ...health(),
    });
  });

  for (const [name, handler] of Object.entries(chaos)) {
    app.post(`/chaos/${name}`, async (req, res) => res.json(await handler(req.body ?? {})));
  }
  for (const [name, handler] of Object.entries(admin)) {
    app.post(`/admin/${name}`, async (req, res) => res.json(await handler(req.body ?? {})));
  }

  routes(app, { emitter, counters });

  // Express 5 forwards rejected promises here on its own. Expected failures carry a
  // code and a status and are not logged — during an incident there are thousands of
  // them a second and they are the signal, not noise. Anything uncoded is a real bug
  // and gets logged loudly.
  app.use((err, req, res, next) => {
    const status = err.status ?? 500;
    if (!err.code) console.error(`${req.method} ${req.path} -> ${status}: ${err.stack ?? err.message}`);
    res.status(status).json({ error: err.code ?? err.name ?? 'ERROR', message: err.message });
  });

  startCounterFlush(emitter, counters);
  app.listen(port, '127.0.0.1', () => console.log(`${svc} up on 127.0.0.1:${port}`));
  return { app, emitter, counters };
}

// One keep-alive pool per process, shared by every outbound call.
//
// maxSockets is deliberately well above anything the mesh can actually run concurrently
// (the gateway's bulkhead caps auth at 64, and no service exceeds one in-flight call per
// virtual user). The agent must never become a second, hidden bulkhead: concurrency is
// limited on purpose in the gateway and by the datastore's semaphore, and a socket limit
// here would add invisible queueing that corrupts the latency we are trying to measure.
// maxFreeSockets is small so idle connections do not accumulate between load changes.
const agent = new http.Agent({
  keepAlive: true,
  keepAliveMsecs: 1000,
  maxSockets: 512,
  maxFreeSockets: 32,
  scheduling: 'fifo',
});

// Deliberately node:http rather than fetch. Node 24's fetch (undici) aborts the whole
// process with a native fast-fail under sustained request-cancellation churn on Windows,
// which is exactly the traffic this mesh generates once the gateway starts timing out.
// See docs/REAL-VS-SIMULATED.md for the measurement. Same deadline semantics, same
// return shape, same error codes.
function callJson(url, body, timeoutMs) {
  const target = new URL(url);
  const payload = JSON.stringify(body);

  return new Promise((resolve, reject) => {
    let settled = false;
    let timedOut = false;
    let timer = null;
    const startedAt = performance.now();

    const fail = (cause) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      // A failed attempt still consumed real downstream time; excluding it would inflate
      // selfMs during exactly the incident we are trying to measure.
      recordDownstream(performance.now() - startedAt);
      // A missed deadline is an upstream failure, not a bug in this service. It has to
      // surface as 503 or the error rate blames the caller for its dependency's outage —
      // and blaming the wrong service is precisely the mistake the scorer must not make.
      const err = new Error(`${url} unreachable: ${cause.message}`);
      err.status = 503;
      err.code = timedOut ? 'UPSTREAM_TIMEOUT' : 'UPSTREAM_UNREACHABLE';
      err.cause = cause;
      reject(err);
    };

    const req = http.request(
      {
        agent,
        host: target.hostname,
        port: target.port,
        path: target.pathname + target.search,
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'content-length': Buffer.byteLength(payload),
        },
      },
      (res) => {
        const chunks = [];
        res.on('data', (chunk) => chunks.push(chunk));
        res.on('error', fail);
        res.on('end', () => {
          if (settled) return;
          settled = true;
          clearTimeout(timer);
          recordDownstream(performance.now() - startedAt);
          let parsed = {};
          try {
            parsed = JSON.parse(Buffer.concat(chunks).toString('utf8'));
          } catch {
            parsed = {};
          }
          resolve({ status: res.statusCode, body: parsed });
        });
      }
    );

    // One deadline covering connect, send and body read, matching the AbortSignal this
    // replaces. Destroying the request also destroys its socket, so a half-read
    // connection is never handed back to the keep-alive pool.
    timer = setTimeout(() => {
      timedOut = true;
      req.destroy(new Error(`deadline ${timeoutMs}ms exceeded`));
    }, timeoutMs);

    req.on('error', fail);
    req.end(payload);
  });
}

// Burn real CPU for ms milliseconds. Not a sleep: this occupies the event loop the way
// a slow synchronous handler does, which is the whole point of both faults.
function burnCpu(ms) {
  const deadline = performance.now() + ms;
  let acc = 0;
  while (performance.now() < deadline) {
    for (let i = 0; i < 500; i++) acc = (acc * 31 + i) % 2147483647;
  }
  return acc;
}

module.exports = { startService, callJson, burnCpu };
