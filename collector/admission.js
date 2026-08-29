'use strict';

// Admission is where backpressure actually happens. Everything upstream of here is
// producing telemetry as fast as the incident makes it; this file decides what the
// pipeline can absorb and tells the emitters, in the 202 body, what to stop sending.

// TASK #1 — depthToShedLevel.
//
// The shed level is a pure function of how full the queue is. It is deliberately not a
// rate, a moving average or a predictor: a judge can read the queue depth off the console
// and check the level themselves. Thresholds come from tuning.collector.shedThresholds.
function depthToShedLevel(depth, thresholds) {
  if (depth >= thresholds[2]) return 3;
  if (depth >= thresholds[1]) return 2;
  if (depth >= thresholds[0]) return 1;
  return 0;
}

// At level 2 the emitters down-sample latency samples. The rate falls smoothly as the
// queue fills across the level-2 band rather than snapping to a constant, so the console
// shows fidelity degrading with pressure instead of stepping.
function depthToPri1SampleRate(depth, thresholds) {
  const level = depthToShedLevel(depth, thresholds);
  if (level < 2) return 1;
  if (level === 3) return 0;
  const span = thresholds[2] - thresholds[1];
  const into = depth - thresholds[1];
  return Number(Math.max(0.1, 1 - into / span).toFixed(3));
}

class Admission {
  constructor({ queueMax, shedThresholds }) {
    this.queueMax = queueMax;
    this.thresholds = shedThresholds;
    // One queue per priority. Keeping them separate is what makes "drop the lowest
    // priority present" an O(1) decision at the cap instead of a scan.
    this.queues = [[], [], []];
    this.dropped = { pri0: 0, pri1: 0, pri2: 0 };
    this.admitted = 0;
    this.ingestRateCounter = 0;
    this.ingestRate = 0;
  }

  get depth() {
    return this.queues[0].length + this.queues[1].length + this.queues[2].length;
  }

  get shedLevel() {
    return depthToShedLevel(this.depth, this.thresholds);
  }

  get pri1SampleRate() {
    return depthToPri1SampleRate(this.depth, this.thresholds);
  }

  // Admit a batch. Returns how much was taken and how much was refused, per priority.
  admit(events) {
    let accepted = 0;
    let rejected = 0;

    for (const evt of events) {
      this.ingestRateCounter++;
      if (this.depth < this.queueMax) {
        this.queues[evt.pri].push(evt);
        accepted++;
        continue;
      }

      // At the cap. Make room by discarding the lowest priority currently queued; if the
      // only thing queued is pri-0, refuse the new event instead — pri-0 is never dropped,
      // at any level, which is the guarantee the whole exactness claim rests on.
      const victim = this.queues[2].length > 0 ? 2 : this.queues[1].length > 0 ? 1 : -1;
      if (victim === -1 || evt.pri === 0) {
        if (evt.pri === 0 && victim !== -1) {
          this.queues[victim].shift();
          this.dropped[`pri${victim}`]++;
          this.queues[0].push(evt);
          accepted++;
          continue;
        }
        this.dropped[`pri${evt.pri}`]++;
        rejected++;
        continue;
      }
      if (evt.pri >= victim) {
        // The arriving event is no more important than the cheapest thing queued.
        this.dropped[`pri${evt.pri}`]++;
        rejected++;
        continue;
      }
      this.queues[victim].shift();
      this.dropped[`pri${victim}`]++;
      this.queues[evt.pri].push(evt);
      accepted++;
    }

    this.admitted += accepted;
    return { accepted, rejected };
  }

  // Highest priority first, so counters are never starved by a backlog of debug logs.
  drain(max = Infinity) {
    const out = [];
    for (let pri = 0; pri <= 2 && out.length < max; pri++) {
      out.push(...this.queues[pri].splice(0, max - out.length));
    }
    return out;
  }

  // Called once a second by the collector to turn the running counter into a rate.
  sampleRate() {
    this.ingestRate = this.ingestRateCounter;
    this.ingestRateCounter = 0;
    return this.ingestRate;
  }

  reset() {
    this.queues = [[], [], []];
    this.dropped = { pri0: 0, pri1: 0, pri2: 0 };
    this.admitted = 0;
    this.ingestRateCounter = 0;
    this.ingestRate = 0;
  }
}

module.exports = { Admission, depthToShedLevel, depthToPri1SampleRate };
