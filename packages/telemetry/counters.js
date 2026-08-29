'use strict';

// Cumulative counters, snapshotted once a second as a single pri-0 event.
//
// This is the decision that makes the exactness guarantee true. Requests, errors, retry
// attempts and pool stats do not ride as individual events — they ride as one periodic
// snapshot per service, a handful of events per second, small enough to survive any shed
// level. Only latency samples get sampled away.
//
// Snapshots are cumulative rather than pre-differenced so that a lost snapshot costs
// resolution but not correctness: the collector differences consecutive values, and the
// next difference silently spans the gap.

class Counters {
  constructor() {
    this.req = { in: 0, ok: 0, err: 0 };
    this.edges = new Map();      // to -> { op, calls, attempts, timeouts }
    this.inflight = 0;
    this.resource = null;        // set by the datastore
  }

  request(status) {
    this.req.in++;
    if (status >= 500) this.req.err++;
    else this.req.ok++;
  }

  edge(to, op) {
    let e = this.edges.get(to);
    if (!e) {
      e = { op, calls: 0, attempts: 0, timeouts: 0 };
      this.edges.set(to, e);
    }
    return e;
  }

  // TASK #7 lives in the gateway: calls counts one per logical call, attempts counts one
  // per network attempt including retries. Both ride pri-0, which is why amplification
  // stays exact while the incident is at its worst.
  call(to, op) {
    this.edge(to, op).calls++;
  }

  attempt(to, op) {
    this.edge(to, op).attempts++;
  }

  timeout(to, op) {
    this.edge(to, op).timeouts++;
  }

  snapshot() {
    return {
      pri: 0,
      kind: 'counters',
      req: { ...this.req },
      edges: [...this.edges.entries()].map(([to, e]) => ({
        to,
        op: e.op,
        calls: e.calls,
        attempts: e.attempts,
        timeouts: e.timeouts,
      })),
      inflight: this.inflight,
      ...(this.resource ? { resource: this.resource() } : {}),
    };
  }
}

module.exports = { Counters };
