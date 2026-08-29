'use strict';

// (svc,seq) LRU. Emitters retain and retry batches when the collector is unreachable, so
// the same event can legitimately arrive twice. Counters are cumulative snapshots, but
// samples and state changes are not, so a replayed batch must change nothing.

class Dedup {
  constructor(capacity) {
    this.capacity = capacity;
    // A Map preserves insertion order, which is all an LRU needs here: re-inserting on
    // hit moves the key to the end, and eviction takes from the front.
    this.seen = new Map();
    this.hits = 0;
  }

  // True if this event has been seen before.
  isDuplicate(svc, seq) {
    const key = `${svc}:${seq}`;
    if (this.seen.has(key)) {
      this.seen.delete(key);
      this.seen.set(key, true);
      this.hits++;
      return true;
    }
    this.seen.set(key, true);
    if (this.seen.size > this.capacity) {
      this.seen.delete(this.seen.keys().next().value);
    }
    return false;
  }

  filter(events) {
    return events.filter((evt) => !this.isDuplicate(evt.svc, evt.seq));
  }

  reset() {
    this.seen.clear();
    this.hits = 0;
  }
}

module.exports = { Dedup };
