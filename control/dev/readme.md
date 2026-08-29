# control/dev — TEMPORARY

Everything in this directory is a stand-in for files P1 owns but has not pushed yet.
It exists so the control plane can be built in parallel, before the repo lands.

| File                 | Stands in for                  | Delete when |
|----------------------|--------------------------------|-------------|
| `contracts-local.js` | `packages/contracts/index.js`  | repo arrives |
| `tuning-local.json`  | `config/tuning.json`           | repo arrives |

Nothing under `control/agents/` may import these directly. They are reached only
through `control/adapters/contracts.js` and `control/adapters/tuning.js`, so the
switch to the real files is a one-line change in each adapter.

Deletion is part of the repo-arrival checklist, not the H8 freeze — these die early.
