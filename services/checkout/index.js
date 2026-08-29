'use strict';

const { startService, callJson } = require('../_shared/service');
const { PORTS } = require('../../packages/contracts');

// checkout is the other bystander, and it is the one the demo watches. It never talks to
// auth and auth never talks to it. It fails because its datastore query queues behind
// auth's amplified token lookups.

const DATASTORE = `http://127.0.0.1:${PORTS.datastore}/query`;
const PAYMENTS = `http://127.0.0.1:${PORTS.payments}/charge`;

startService({
  svc: 'checkout',

  routes(app, { counters }) {
    app.post('/checkout', async (req, res) => {
      counters.call('datastore', 'query');
      counters.attempt('datastore', 'query');
      const cart = await callJson(DATASTORE, { op: 'cart-read', userId: req.body?.userId }, 2000);
      if (cart.status !== 200) {
        res.status(503).json({ error: 'CART_UNAVAILABLE', upstream: cart.status });
        return;
      }
      counters.call('payments', 'charge');
      counters.attempt('payments', 'charge');
      const charge = await callJson(PAYMENTS, { orderId: req.body?.orderId, amount: 4999 }, 2000);
      if (charge.status !== 200) {
        res.status(503).json({ error: 'CHARGE_FAILED', upstream: charge.status });
        return;
      }
      res.json({ ok: true, orderId: req.body?.orderId });
    });
  },

  chaos: { reset: () => ({ ok: true }) },
  admin: {},
  health: () => ({}),
});
