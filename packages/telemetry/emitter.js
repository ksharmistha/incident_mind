'use strict';

const { PORTS, validateIngestAck } = require('../contracts');

// The telemetry client every service imports. One absolute requirement: it must never
// block or slow the request path. An observability client that adds latency during an
// incident amplifies the outage it is reporting.

const INGEST = `http://127.0.0.1:${PORTS.collector}/ingest`;
const RING_CAP = 5000;          // per priority
const FLUSH_MS = 200;
const BATCH_MAX = 500;
const RETAIN_MAX = 20;          // batches held while the collector is unreachable

// TASK #2 — applyShed.
//
// The collector answers every ingest with the shed level it wants, and the emitter obeys
// it on its next flush. This is the backpressure loop closing: the pipeline tells the
// emitters what it can absorb, and they discard locally rather than pushing work at a
// queue that is already full.
//
// Level 1 drops debug logs. Level 2 also samples latency down. Level 3 keeps only pri-0.
// pri-0 is never dropped at any level, which is what makes "counts and error rates are
// exact at every shed level" true by construction rather than by hope: counters ride
// pri-0, so shedding costs quantile resolution and never the incident record.
//
// Pure function: takes a batch, returns what survives plus what was discarded by
// priority, so the caller can report exactly what it threw away.
function applyShed(batch, level, pri1SampleRate) {
  const kept = [];
  const dropped = { pri0: 0, pri1: 0, pri2: 0 };

  for (const evt of batch) {
    if (evt.pri === 0) {
      kept.push(evt);
      continue;
    }
    if (level >= 3) {
      dropped[`pri${evt.pri}`]++;
      continue;
    }
    if (evt.pri === 2) {
      if (level >= 1) dropped.pri2++;
      else kept.push(evt);
      continue;
    }
    // pri-1 latency samples: kept whole below level 2, sampled at the collector's rate at
    // level 2. The rate is published so the aggregate can record inclusion probability.
    if (level >= 2 && Math.random() >= pri1SampleRate) dropped.pri1++;
    else kept.push(evt);
  }
  return { kept, dropped };
}

class Emitter {
  constructor(svc) {
    this.svc = svc;
    this.seq = 0;
    this.batchId = 0;
    this.rings = [[], [], []];
    this.localDrops = { pri0: 0, pri1: 0, pri2: 0 };
    this.shedDrops = { pri0: 0, pri1: 0, pri2: 0 };
    this.retained = [];

    // Last signal received from the collector. Applied on the next flush.
    this.shedLevel = 0;
    this.pri1SampleRate = 1;
    this.watermarkLagMs = 0;

    this.timer = setInterval(() => this.flush(), FLUSH_MS);
    this.timer.unref();
  }

  // O(1), never awaits, never throws. Called from the request path.
  enqueue(evt) {
    evt.svc = this.svc;
    evt.seq = this.seq++;
    if (evt.t === undefined) evt.t = Date.now();

    const ring = this.rings[evt.pri];
    if (ring.length < RING_CAP) {
      ring.push(evt);
      return;
    }
    // Ring full. Drop the lowest priority currently held rather than this event, so a
    // flood of debug logs can never displace a counter snapshot.
    for (let pri = 2; pri >= 0; pri--) {
      if (pri === 0) break;
      if (this.rings[pri].length > 0) {
        this.rings[pri].shift();
        this.localDrops[`pri${pri}`]++;
        ring.push(evt);
        return;
      }
    }
    this.localDrops[`pri${evt.pri}`]++;
  }

  takeBatch() {
    const batch = [];
    for (let pri = 0; pri <= 2 && batch.length < BATCH_MAX; pri++) {
      const take = this.rings[pri].splice(0, BATCH_MAX - batch.length);
      batch.push(...take);
    }
    return batch;
  }

  async flush() {
    if (this.inFlight) return;
    const fresh = this.takeBatch();
    if (fresh.length === 0 && this.retained.length === 0) return;

    const { kept, dropped } = applyShed(fresh, this.shedLevel, this.pri1SampleRate);
    for (const pri of ['pri0', 'pri1', 'pri2']) this.shedDrops[pri] += dropped[pri];

    const events = this.retained.flat().concat(kept);
    this.retained = [];
    if (events.length === 0) return;

    this.inFlight = true;
    try {
      const ack = await this.post({ svc: this.svc, batchId: `${this.svc}-${this.batchId++}`, events });
      validateIngestAck(ack);
      this.shedLevel = ack.shedLevel;
      this.pri1SampleRate = ack.pri1SampleRate;
      this.watermarkLagMs = ack.watermarkLagMs;
    } catch {
      // Collector unreachable: retain and retry, but never without bound, and never let
      // pri-0 be the thing we discard.
      this.retained.push(events);
      while (this.retained.length > RETAIN_MAX) {
        const oldest = this.retained.shift();
        for (const evt of oldest) {
          if (evt.pri === 0) this.retained[0]?.unshift(evt);
          else this.localDrops[`pri${evt.pri}`]++;
        }
      }
    } finally {
      this.inFlight = false;
    }
  }

  post(body) {
    const http = require('node:http');
    const payload = JSON.stringify(body);
    return new Promise((resolve, reject) => {
      const req = http.request(
        {
          host: '127.0.0.1',
          port: PORTS.collector,
          path: '/ingest',
          method: 'POST',
          headers: { 'content-type': 'application/json', 'content-length': Buffer.byteLength(payload) },
          agent: agent(),
        },
        (res) => {
          const chunks = [];
          res.on('data', (c) => chunks.push(c));
          res.on('end', () => {
            try {
              resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
            } catch (err) {
              reject(err);
            }
          });
        }
      );
      req.on('error', reject);
      req.setTimeout(1000, () => req.destroy(new Error('ingest timeout')));
      req.end(payload);
    });
  }

  stats() {
    return {
      queued: this.rings.reduce((a, r) => a + r.length, 0),
      localDrops: { ...this.localDrops },
      shedDrops: { ...this.shedDrops },
      shedLevel: this.shedLevel,
      retained: this.retained.length,
    };
  }
}

let sharedAgent = null;
function agent() {
  if (!sharedAgent) {
    const http = require('node:http');
    sharedAgent = new http.Agent({ keepAlive: true, maxSockets: 8, maxFreeSockets: 2 });
  }
  return sharedAgent;
}

module.exports = { Emitter, applyShed, INGEST };
