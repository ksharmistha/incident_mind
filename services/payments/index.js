'use strict';

const { startService, callJson } = require('../_shared/service');
const { PORTS } = require('../../packages/contracts');

// payments is a bystander. Nothing ever injects a fault here. It queries the datastore
// like everyone else, which is the only reason it can fail during an auth incident.

const DATASTORE = `http://127.0.0.1:${PORTS.datastore}/query`;

startService({
  svc: 'payments',

  routes(app, { counters }) {
    app.post('/charge', async (req, res) => {
      counters.call('datastore', 'query');
      counters.attempt('datastore', 'query');
      const ledger = await callJson(DATASTORE, { op: 'ledger-write', orderId: req.body?.orderId }, 2000);
      if (ledger.status !== 200) {
        res.status(503).json({ error: 'LEDGER_UNAVAILABLE', upstream: ledger.status });
        return;
      }
      res.json({ ok: true, charged: req.body?.amount ?? 0 });
    });
  },

  chaos: { reset: () => ({ ok: true }) },
  admin: {},
  health: () => ({}),
});
