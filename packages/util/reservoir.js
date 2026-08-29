'use strict';

// Bounded reservoir sampler (Algorithm R). A window can contain tens of thousands of
// latency samples; we keep at most `size` of them and publish the probability that any
// given sample was included, so a quantile drawn from it is honestly labelled as an
// estimate rather than presented as exact.

class Reservoir {
  constructor(size) {
    this.size = size;
    this.samples = [];
    this.seen = 0;
  }

  add(value) {
    this.seen++;
    if (this.samples.length < this.size) {
      this.samples.push(value);
      return;
    }
    // Each of the `seen` values must end up equally likely to be held.
    const i = Math.floor(Math.random() * this.seen);
    if (i < this.size) this.samples[i] = value;
  }

  // What fraction of the values offered are actually represented. Rides on every
  // quantile we publish; the console shows it beside the number.
  get inclusionProbability() {
    return this.seen === 0 ? 1 : Math.min(1, this.samples.length / this.seen);
  }

  quantile(q) {
    if (this.samples.length === 0) return 0;
    const sorted = [...this.samples].sort((a, b) => a - b);
    const i = Math.min(sorted.length - 1, Math.floor(q * sorted.length));
    return Number(sorted[i].toFixed(2));
  }
}

module.exports = { Reservoir };
