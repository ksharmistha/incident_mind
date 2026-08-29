'use strict';

// Event-time windowing. Events are bucketed by the time they happened (stamped at enqueue
// in the emitting service), not the time they arrived here. Under load those differ by
// hundreds of milliseconds, and bucketing by arrival would smear an incident across the
// wrong seconds — which is exactly when the timeline matters most.

class WindowStore {
  constructor({ windowMs, watermarkLagMs, latenessMs }) {
    this.windowMs = windowMs;
    this.watermarkLagMs = watermarkLagMs;
    this.latenessMs = latenessMs;

    this.open = new Map();        // windowId -> { windowId, tStart, tEnd, events[] }
    this.maxEventTime = 0;
    this.watermark = 0;
    this.lastClosedId = null;
    this.lateDropped = 0;
  }

  windowIdFor(t) {
    return Math.floor(t / this.windowMs);
  }

  // Place an event in the window its own timestamp belongs to. If that window has already
  // been closed and published, the event is counted as late rather than quietly folded
  // into a window it does not belong to.
  add(evt) {
    const id = this.windowIdFor(evt.t);
    if (evt.t > this.maxEventTime) this.maxEventTime = evt.t;

    if (this.lastClosedId !== null && id <= this.lastClosedId) {
      this.lateDropped++;
      return false;
    }
    let bucket = this.open.get(id);
    if (!bucket) {
      bucket = { windowId: id, tStart: id * this.windowMs, tEnd: (id + 1) * this.windowMs, events: [] };
      this.open.set(id, bucket);
    }
    bucket.events.push(evt);
    return true;
  }

  // TASK #4 — advanceWatermark.
  //
  // The watermark is our claim about how far event time has certainly progressed: the
  // newest event time we have seen, minus a lag that allows for events still in flight.
  // Subtracting watermarkLagMs is what buys out-of-order tolerance — without it, one
  // event from a fast service would immediately close windows that a slow service has
  // not reported into yet.
  //
  // A window may close once the watermark has passed its end plus the allowed lateness.
  // Windows are returned in ascending order so windowId is strictly increasing with no
  // gaps, which the aggregate contract guarantees to P2's detector.
  advanceWatermark(now = Date.now()) {
    // Event time can only be trusted to have reached maxEventTime; wall clock is used
    // only so an idle pipeline still closes its windows instead of stalling forever.
    const basis = Math.max(this.maxEventTime, now - this.watermarkLagMs);
    this.watermark = basis - this.watermarkLagMs;

    const closable = [];
    for (const bucket of this.open.values()) {
      if (bucket.tEnd + this.latenessMs <= this.watermark) closable.push(bucket);
    }
    closable.sort((a, b) => a.windowId - b.windowId);

    for (const bucket of closable) {
      this.open.delete(bucket.windowId);
      this.lastClosedId = bucket.windowId;
    }
    return closable;
  }

  // How far behind real time our newest event is — the age of the freshest thing we have
  // ingested. Rides into observation confidence, so staleness lowers our own certainty
  // instead of being hidden.
  //
  // Deliberately measured against maxEventTime rather than against the watermark: the
  // watermark is offset by a fixed watermarkLagMs by construction, so reporting `now -
  // watermark` would show a constant 2s of "staleness" that is really just our own
  // out-of-order tolerance. What the term is meant to capture is ingestion falling
  // behind — emitters retaining batches, flushes backing up — and that is exactly what
  // the age of the newest event measures.
  watermarkLag(now = Date.now()) {
    if (this.maxEventTime === 0) return 0;
    return Math.max(0, now - this.maxEventTime);
  }

  reset() {
    this.open.clear();
    this.maxEventTime = 0;
    this.watermark = 0;
    this.lastClosedId = null;
    this.lateDropped = 0;
  }
}

module.exports = { WindowStore };
