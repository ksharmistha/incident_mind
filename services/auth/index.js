'use strict';

const { startService, callJson } = require('../_shared/service');
const { PORTS } = require('../../packages/contracts');
const tuning = require('../../config/tuning.json');

// auth is on the path of EVERY gateway request, and it makes exactly one token lookup
// against the datastore per verify. That single downstream call is what turns retry
// amplification against auth into query pressure on a pool checkout also needs.

const DATASTORE = `http://127.0.0.1:${PORTS.datastore}/query`;

let version = '2.4.0';

// A token carries a handful of claims. Both validator versions walk the same claims and
// differ only in how much work they do per claim — the fault is a real code path, not a
// flag that adds a sleep.
function claimsFor(token) {
  return [
    { k: 'sub', v: token },
    { k: 'aud', v: 'incidentmind-checkout' },
    { k: 'iss', v: 'auth.incidentmind.internal' },
    { k: 'scp', v: 'checkout:write payments:write browse:read' },
    { k: 'jti', v: `${token}-0451` },
    { k: 'org', v: 'signal-labs' },
  ];
}

const CLAIM_PATTERN = /^[a-z0-9:. -]+$/i;

// v2.4.1 added versioned claims: a claim may carry a trailing version tag, as in
// "billing.read:v2". A claim is accepted if it parses as a versioned scope OR if it
// satisfies the legacy character rule, which is why the verdict is unchanged.
//
// Every character this pattern can consume — alphanumerics, dot, hyphen, space, colon —
// is also accepted by CLAIM_PATTERN, and both are anchored, so the set of strings it
// matches is a strict subset of the legacy set. That makes `scope || legacy` identical
// to `legacy` for every possible input: a performance regression, not a behaviour change.
//
// The regression itself: the segment group and the [a-z0-9.\- ]+ inside it can both
// consume the same characters, so there is no single way to carve a value into segments.
// For an unversioned claim the engine has to try every segmentation before it can
// conclude the claim does not match, and the cost of that search is what saturates the
// event loop under load. The {1,4} bound keeps the search polynomial — measured at ~40us
// per claim set. Raising it to 5 costs 5x and to 6 costs 18x, which would make per-call
// cost depend violently on claim length and the fault unreproducible between runs.
const SCOPE_VERSION_PATTERN = /^(?:[a-z0-9.\- ]+:?){1,4}v[0-9]+$/i;

// v2.4.0 — one linear pass per claim. ~0.2ms.
function verifyTokenV240(claims, workUnits) {
  let ok = true;
  for (let pass = 0; pass < workUnits; pass++) {
    for (const claim of claims) {
      if (!CLAIM_PATTERN.test(claim.v)) ok = false;
    }
  }
  return ok;
}

// v2.4.1 — tries the versioned-scope grammar first, falls back to the legacy rule.
// Same shape and same verdict as v2.4.0; the cost is in rejecting unversioned claims.
function verifyTokenV241(claims, workUnits) {
  let ok = true;
  for (let pass = 0; pass < workUnits; pass++) {
    for (const claim of claims) {
      if (!SCOPE_VERSION_PATTERN.test(claim.v) && !CLAIM_PATTERN.test(claim.v)) ok = false;
    }
  }
  return ok;
}

function verifyToken(token) {
  const claims = claimsFor(token);
  const workUnits = tuning.auth.workUnits[version];
  return version === '2.4.1' ? verifyTokenV241(claims, workUnits) : verifyTokenV240(claims, workUnits);
}

function setVersion(next) {
  if (!tuning.auth.workUnits[next]) throw new Error(`unknown auth version "${next}"`);
  version = next;
  console.log(`auth now serving ${version}`);
  return { ok: true, version };
}

// Exported so the CPU cost of each validator can be measured in isolation, without
// HTTP or the datastore in the way. The boot is guarded so requiring this file for a
// measurement does not try to bind port 4001.
module.exports = { verifyToken, verifyTokenV240, verifyTokenV241, claimsFor, setVersion };

if (require.main !== module) return;

startService({
  svc: 'auth',

  routes(app, { counters }) {
    app.post('/verify', async (req, res) => {
      const token = req.body?.token ?? 'anonymous';
      const valid = verifyToken(token);
      // ONE token lookup per verify. THE coupling. One logical call, one attempt: auth
      // does not retry, so any amplification on this edge would be a bug.
      counters.call('datastore', 'query');
      counters.attempt('datastore', 'query');
      const lookup = await callJson(DATASTORE, { op: 'token-lookup', token }, 2000);
      if (lookup.status !== 200) {
        res.status(503).json({ error: 'TOKEN_LOOKUP_FAILED', upstream: lookup.status });
        return;
      }
      res.json({ valid, version });
    });
  },

  chaos: {
    version: ({ version: next }) => setVersion(next),
    reset: () => setVersion('2.4.0'),
  },

  admin: {
    version: ({ version: next }) => setVersion(next),
  },

  health: () => ({ version, faults: version === '2.4.1' ? ['slow-validator'] : [] }),
});
