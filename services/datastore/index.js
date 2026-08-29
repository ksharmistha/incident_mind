'use strict';

const { startService, burnCpu } = require('../_shared/service');
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
// Compaction makes every query more expensive. The extra cost is split deliberately: the
// service interval rises from serviceTimeMs to compactionServiceTimeMs, and a slice of it
// is burned as real CPU inside the query handler, because a compacting store genuinely
// competes for the CPU as well as holding its connection longer.
//
// F2 must be observationally similar to F1 from the outside — pool saturated, auth slow,
// retries amplified, bystanders failing — and differ only in where auth's time goes.
// Under F2 auth's downstreamP99 rises while its selfP99 stays flat, which is exactly the
// discriminator the causal probe measures.
const COMPACTION_CPU_SHARE = 0.2;

function currentServiceTimeMs() {
  return compaction ? compactionServiceTimeMs : serviceTimeMs;
}

function currentCpuBurnMs() {
  return compaction ? compactionServiceTimeMs * COMPACTION_CPU_SHARE : 0;
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
        const cpuMs = currentCpuBurnMs();
        if (cpuMs > 0) burnCpu(cpuMs);
        await serviceInterval(Math.max(0, currentServiceTimeMs() - cpuMs));
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
