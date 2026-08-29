'use strict';

const { startService } = require('../_shared/service');
const { Semaphore } = require('../../packages/util/semaphore');
const tuning = require('../../config/tuning.json');

// The shared resource. auth, checkout and payments all query it, and they all contend
// for the same twelve permits. This is the object of study.

const { poolSize, queueMax, serviceTimeMs, compactionServiceTimeMs } = tuning.datastore;

const pool = new Semaphore({ size: poolSize, queueMax, acquireTimeoutMs: 2000 });

// Service time is an awaited interval, not a synchronous spin, because a connection
// pool's service time is I/O wait. On a single-threaded runtime a spin would hold the
// event loop for the entire interval, so permits could never overlap: twelve permits
// would behave exactly like one and the datastore would cap at 1/W = 50 queries/s
// instead of N/W = 600. Holding the permit across the await is what makes the pool,
// rather than the event loop, the binding constraint — and therefore what makes
// utilisation rho = lambda*W/N mean what it says.
const serviceInterval = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

let compaction = false;
let arrivals = 0;

// Service time is measured, never assumed. The configured value is what we ask for;
// what we actually get depends on the host's timer granularity — on Windows the system
// tick is 15.6ms, so a requested 20ms interval really costs about 31ms. rho = lambda*W/N
// is a headline number, so W has to be the W that happened, not the W we asked for.
let serviceTimeTotal = 0;
let serviceTimeCount = 0;
let lastWindow = { arrivalRate: 0, serviceTimeMs: serviceTimeMs, utilisation: 0 };

// TASK #7 (handoff #10) — the F2 compaction branch.
//
// Compaction raises this store's service time from serviceTimeMs to
// compactionServiceTimeMs. That is the whole fault: each query occupies its connection
// for longer, so the pool drains more slowly and utilisation rises.
//
// A share of the interval was briefly burned as real CPU on the theory that a compacting
// store also competes for the processor. Measurement killed that idea: burning inside the
// query handler saturates this service's event loop, which stretches EVERY awaited
// interval, and the measured service time went to 562ms against a configured 45ms with
// utilisation swinging between 0.99 and 2.77. The fault has to be a service-time change
// and nothing else, or it stops being the thing the specification describes.
//
// F2 must look like F1 from the outside — pool saturated, auth slow, retries amplified,
// bystanders failing — and differ only in where auth's time goes. Under F2 auth's
// downstreamP99 rises while its selfP99 stays flat, which is the discriminator the causal
// probe measures.
function currentServiceTimeMs() {
  return compaction ? compactionServiceTimeMs : serviceTimeMs;
}

startService({
  svc: 'datastore',

  routes(app) {
    app.post('/query', async (req, res) => {
      arrivals++;
      // The permit covers the query itself and nothing else — it is released before the
      // response is serialised, so pool occupancy measures work, not HTTP overhead.
      await pool.acquire();
      const startedService = performance.now();
      try {
        await serviceInterval(currentServiceTimeMs());
      } finally {
        serviceTimeTotal += performance.now() - startedService;
        serviceTimeCount++;
        pool.release();
      }
      res.json({ ok: true, rows: 1 });
    });
  },

  chaos: {
    compaction: ({ on }) => {
      compaction = Boolean(on);
      console.log(`chaos: compaction ${compaction ? 'ON' : 'off'}`);
      return { ok: true, compaction };
    },
    reset: () => {
      compaction = false;
      arrivals = 0;
      pool.resetStats();
      return { ok: true };
    },
  },

  admin: {},

  health: () => ({
    faults: compaction ? ['compaction'] : [],
    pool: pool.stats(),
    window: lastWindow,
  }),

  // The shared resource publishes itself on pri-0 every second, so poolInUse, queueDepth,
  // queue wait and utilisation are exact at every shed level.
  resource: () => ({
    poolSize: pool.size,
    poolInUse: pool.inUse,
    queueDepth: pool.queueDepth,
    queueWaitP99Ms: pool.stats().queueWaitP99Ms,
    arrivalRate: lastWindow.arrivalRate,
    serviceTimeMs: lastWindow.serviceTimeMs,
    utilisation: lastWindow.utilisation,
  }),
});

// One line per second. Before any UI exists, this is how we watch the pool saturate.
setInterval(() => {
  const s = pool.stats();
  const lambda = arrivals;
  arrivals = 0;

  // Fall back to the configured value only when nothing completed this second, so an
  // idle window does not report a service time of zero.
  const W = serviceTimeCount > 0 ? serviceTimeTotal / serviceTimeCount : currentServiceTimeMs();
  serviceTimeTotal = 0;
  serviceTimeCount = 0;

  lastWindow = {
    arrivalRate: lambda,
    serviceTimeMs: Number(W.toFixed(2)),
    utilisation: Number(((lambda * W) / 1000 / poolSize).toFixed(3)),
  };

  console.log(
    `pool ${s.poolInUse}/${s.poolSize} queue=${s.queueDepth} wait_p99=${s.queueWaitP99Ms}ms ` +
      `lambda=${lambda}/s W=${lastWindow.serviceTimeMs}ms rho=${lastWindow.utilisation.toFixed(2)} ` +
      `rejected=${s.rejected} timedOut=${s.timedOut}`
  );
}, 1000).unref();

module.exports = { pool };
