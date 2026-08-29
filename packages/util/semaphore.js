'use strict';

const MAX_WAIT_SAMPLES = 512;

class QueueFullError extends Error {
  constructor(queueMax) {
    super(`resource queue full (${queueMax} already waiting)`);
    this.name = 'QueueFullError';
    this.code = 'QUEUE_FULL';
    this.status = 503;
  }
}

class AcquireTimeoutError extends Error {
  constructor(timeoutMs) {
    super(`timed out after ${timeoutMs}ms waiting for a permit`);
    this.name = 'AcquireTimeoutError';
    this.code = 'ACQUIRE_TIMEOUT';
    this.status = 503;
  }
}

class Semaphore {
  constructor({ size, queueMax, acquireTimeoutMs }) {
    this.size = size;
    this.queueMax = queueMax;
    this.acquireTimeoutMs = acquireTimeoutMs;

    this.inUse = 0;
    this.waiters = [];

    this.granted = 0;
    this.rejected = 0;
    this.timedOut = 0;
    this.waitSamples = [];
  }

  get queueDepth() {
    return this.waiters.length;
  }

  recordWait(ms) {
    this.waitSamples.push(ms);
    if (this.waitSamples.length > MAX_WAIT_SAMPLES) this.waitSamples.shift();
  }

  acquire() {
    if (this.inUse < this.size) {
      this.inUse++;
      this.granted++;
      this.recordWait(0);
      return Promise.resolve();
    }

    if (this.waiters.length >= this.queueMax) {
      this.rejected++;
      return Promise.reject(new QueueFullError(this.queueMax));
    }

    return new Promise((resolve, reject) => {
      const waiter = {
        resolve,
        reject,
        enqueuedAt: performance.now(),
        timer: null
      };

      waiter.timer = setTimeout(() => {
        const i = this.waiters.indexOf(waiter);
        if (i !== -1) this.waiters.splice(i, 1);
        this.timedOut++;
        reject(new AcquireTimeoutError(this.acquireTimeoutMs));
      }, this.acquireTimeoutMs);

      this.waiters.push(waiter);
    });
  }

  release() {
    if (this.waiters.length > 0) {
      const waiter = this.waiters.shift();

      clearTimeout(waiter.timer);
      this.granted++;
      this.recordWait(performance.now() - waiter.enqueuedAt);
      waiter.resolve();

      return;
    }

    this.inUse--;
  }

  stats() {
    return {
      poolSize: this.size,
      poolInUse: this.inUse,
      queueDepth: this.waiters.length,
      queueWaitP99Ms: quantile(this.waitSamples, 0.99),
      granted: this.granted,
      rejected: this.rejected,
      timedOut: this.timedOut,
    };
  }

  resetStats() {
    this.granted = 0;
    this.rejected = 0;
    this.timedOut = 0;
    this.waitSamples = [];
  }
}

function quantile(samples, q) {
  if (samples.length === 0) return 0;
  const sorted = [...samples].sort((a, b) => a - b);
  const i = Math.min(sorted.length - 1, Math.floor(q * sorted.length));
  return Number(sorted[i].toFixed(2));
}

module.exports = { Semaphore, QueueFullError, AcquireTimeoutError };