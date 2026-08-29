'use strict';

const { Reservoir } = require('../packages/util/reservoir');

// Turns one window's events into the WindowAggregate the whole control plane consumes.
//
// The division of labour is the point: counts and edges come from pri-0 counter
// snapshots, which are never shed, so they are exact at every shed level. Quantiles come
// from pri-1 samples through a bounded reservoir, and always carry the probability that
// any given sample was included. We degrade resolution, never the incident record.

// TASK #3 — counterDelta.
//
// Services emit CUMULATIVE counters once a second, and the collector differences
// consecutive snapshots. Cumulative-plus-difference rather than pre-differenced deltas is
// deliberate: if a snapshot is lost or shed, the next difference silently spans the gap
// and the totals stay right. Pre-computed deltas would lose that window forever.
//
// A counter that moves backwards means the service restarted and its counters reset, so
// the current value is the delta.
function counterDelta(previous, current) {
  if (previous === undefined || current < previous) return current;
  return current - previous;
}

// amplification = attempts / calls, from pri-0 counter deltas and nothing else. Computing
// it from pri-1 samples would make the headline number degrade exactly when the incident
// is worst, because pri-1 is what gets sampled away under pressure.
function edgeAmplification(callsDelta, attemptsDelta) {
  if (callsDelta <= 0) return 0;
  return Number((attemptsDelta / callsDelta).toFixed(3));
}

const STATES = { HEALTHY: 'HEALTHY', DEGRADED: 'DEGRADED', CRITICAL: 'CRITICAL', UNKNOWN: 'UNKNOWN' };

class Aggregator {
  constructor({ reservoirSize }) {
    this.reservoirSize = reservoirSize;
    // Last cumulative counter snapshot seen per service, and per edge within it.
    this.lastCounters = new Map();     // svc -> { req:{in,ok,err}, edges: Map(key -> {calls,attempts,timeouts}) }
    this.firstDegradedAt = new Map();  // svc -> timestamp
    this.states = new Map();           // svc -> state
  }

  // Build the services/edges/resources sections for one closed window.
  build(bucket, knownServices) {
    const services = {};
    const edges = {};
    const resources = {};
    const reservoirs = new Map();
    const selfRes = new Map();
    const downRes = new Map();
    const counts = new Map();          // svc -> {in, ok, err}
    const inflight = new Map();

    for (const evt of bucket.events) {
      const svc = evt.svc;

      if (evt.kind === 'counters') {
        const prev = this.lastCounters.get(svc);
        const req = {
          in: counterDelta(prev?.req.in, evt.req.in),
          ok: counterDelta(prev?.req.ok, evt.req.ok),
          err: counterDelta(prev?.req.err, evt.req.err),
        };
        const existing = counts.get(svc) ?? { in: 0, ok: 0, err: 0 };
        counts.set(svc, { in: existing.in + req.in, ok: existing.ok + req.ok, err: existing.err + req.err });

        const prevEdges = prev?.edges ?? new Map();
        const nextEdges = new Map();
        for (const edge of evt.edges ?? []) {
          const key = `${svc}→${edge.to}`;
          const before = prevEdges.get(edge.to);
          const callsDelta = counterDelta(before?.calls, edge.calls);
          const attemptsDelta = counterDelta(before?.attempts, edge.attempts);
          const timeoutsDelta = counterDelta(before?.timeouts, edge.timeouts);

          const acc = edges[key] ?? { calls: 0, attempts: 0, timeouts: 0, amplification: 0 };
          acc.calls += callsDelta;
          acc.attempts += attemptsDelta;
          acc.timeouts += timeoutsDelta;
          edges[key] = acc;

          nextEdges.set(edge.to, { calls: edge.calls, attempts: edge.attempts, timeouts: edge.timeouts });
        }

        if (evt.resource) {
          resources[svc] = {
            poolSize: evt.resource.poolSize,
            poolInUse: evt.resource.poolInUse,
            queueDepth: evt.resource.queueDepth,
            queueWaitP99: evt.resource.queueWaitP99Ms,
            arrivalRate: evt.resource.arrivalRate,
            serviceTimeMs: evt.resource.serviceTimeMs,
            utilisation: evt.resource.utilisation,
          };
        }
        if (evt.inflight !== undefined) inflight.set(svc, evt.inflight);

        this.lastCounters.set(svc, { req: evt.req, edges: nextEdges });
        continue;
      }

      if (evt.kind === 'sample') {
        if (!reservoirs.has(svc)) {
          reservoirs.set(svc, new Reservoir(this.reservoirSize));
          selfRes.set(svc, new Reservoir(this.reservoirSize));
          downRes.set(svc, new Reservoir(this.reservoirSize));
        }
        reservoirs.get(svc).add(evt.totalMs);
        selfRes.get(svc).add(evt.selfMs);
        downRes.get(svc).add(evt.downstreamMs);
      }
    }

    for (const [key, edge] of Object.entries(edges)) {
      edge.amplification = edgeAmplification(edge.calls, edge.attempts);
    }

    const windowSeconds = (bucket.tEnd - bucket.tStart) / 1000;
    for (const svc of knownServices) {
      const c = counts.get(svc);
      const res = reservoirs.get(svc);
      const rps = c ? Number((c.in / windowSeconds).toFixed(2)) : 0;
      const errRate = c && c.in > 0 ? Number((c.err / c.in).toFixed(4)) : 0;
      const state = this.classify(svc, c, errRate, bucket.tEnd);

      services[svc] = {
        rps,
        errRate,
        inflight: inflight.get(svc) ?? 0,
        p50: res ? res.quantile(0.5) : 0,
        p95: res ? res.quantile(0.95) : 0,
        p99: res ? res.quantile(0.99) : 0,
        selfP99: selfRes.get(svc) ? selfRes.get(svc).quantile(0.99) : 0,
        downstreamP99: downRes.get(svc) ? downRes.get(svc).quantile(0.99) : 0,
        sampleCount: res ? res.samples.length : 0,
        inclusionProbability: res ? res.inclusionProbability : 1,
        state,
        firstDegradedAt: this.firstDegradedAt.get(svc) ?? null,
      };
    }

    return { services, edges, resources };
  }

  // A service that reported nothing this window is UNKNOWN, not healthy — silence is a
  // measurement, and it feeds observation confidence.
  classify(svc, counts, errRate, at) {
    if (!counts || counts.in === 0) {
      this.states.set(svc, STATES.UNKNOWN);
      return STATES.UNKNOWN;
    }
    let state = STATES.HEALTHY;
    if (errRate >= 0.25) state = STATES.CRITICAL;
    else if (errRate >= 0.05) state = STATES.DEGRADED;

    if (state === STATES.HEALTHY) this.firstDegradedAt.delete(svc);
    else if (!this.firstDegradedAt.has(svc)) this.firstDegradedAt.set(svc, at);

    this.states.set(svc, state);
    return state;
  }

  reset() {
    this.lastCounters.clear();
    this.firstDegradedAt.clear();
    this.states.clear();
  }
}

module.exports = { Aggregator, counterDelta, edgeAmplification, STATES };
