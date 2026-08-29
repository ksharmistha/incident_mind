# IncidentMind V2 — Control Plane

A control plane for distributed-systems incidents. It reads a live telemetry stream, decides
something is wrong, ranks what is causing it, and — when observation alone cannot separate two
plausible causes — runs a bounded, reversible experiment on the live system to find out which is
real. Remediation goes through an approval gate, and whether anything actually recovered is decided
from telemetry, never from the executor's HTTP response.

```
Telemetry → Detector → Scorer → Experimenter → Planner → Executor → Verifier → Console
```

Built for the Signal Labs AI Hackday, distributed systems track. This repository is the **P2** half:
the control plane, the console, and the shared contracts. See [Scope](#scope) for what is not here.

---

## What makes it interesting

**It intervenes instead of guessing.** Two different faults — a CPU regression in a service, and
contention on a shared datastore — produce nearly identical symptoms: the same pool saturation, the
same retry amplification, the same downstream services failing. Correlation cannot separate them. So
when the top two hypotheses land within the ambiguity threshold, the Experimenter publishes what it
expects to see, sheds a bounded fraction of traffic for five seconds, measures the result, and
updates its belief. The predictions are published *before* the intervention fires — that ordering is
what makes it an experiment rather than a claim.

**Execution is not recovery.** `APPLIED` means the data plane accepted an instruction. Whether the
incident is over is a separate question, answered by the Verifier reading the windows that follow.
The function that decides recovery is never handed the ActionRecord — the invariant is enforced by
its signature, not by discipline.

**Nothing is learned.** Every number is a constant from `config/tuning.json`, a named constant in
`control/adapters/tuning.js`, or computed deterministically from telemetry. There is no model. The
scoring is four weighted terms you can read off the screen and argue with, which matters when the
output is a recommendation to change production.

---

## Repository layout

| Path | What it is |
|---|---|
| `packages/contracts/index.js` | Frozen schemas and validators. The interface both halves of the project were built against. |
| `config/tuning.json` | Every threshold in one file. Read-only to the control plane. |
| `control/agents/` | The six agents: detector, scorer, experimenter, planner, executor, verifier. |
| `control/adapters/` | The only code that talks to anything external — contracts, tuning, telemetry (WS in), admin (HTTP out). |
| `control/server.js` | Port 4200. The supervisor loop, `/health`, `/reset`, `/approve`, and the ControlState WebSocket. |
| `control/state.js` | The entire control-plane state: one in-memory object, broadcast on change. |
| `console/` | React + Vite dashboard on port 5173. Two WebSockets, no charting library. |
| `tools/dev-windows.js` | Synthetic telemetry source. Development only — see [Limitations](#limitations). |
| `tools/demo.js` | One-command end-to-end demo. |
| `tools/calibrate.js` | Scorer calibration harness. Refuses to run without real fixtures. |

Deeper documentation, including the safety model in full, is in
[`control/README.md`](control/README.md).

---

## Running it

Requires **Node 24.x** (native `fetch` with `AbortSignal.timeout`). Install from the committed
lockfile:

```bash
npm ci
cd console && npm ci && cd ..
```

### One command

```bash
node tools/demo.js --dev --scenario f2 --approve
```

Starts the telemetry source and the control plane, narrates each lifecycle transition, issues an
approval when asked, prints the final verdict, and cleans up its child processes.

Flags: `--scenario healthy|spike|f1|f2` · `--approve` · `--interval <ms>` · `--seed <n>`

### With the console

Three terminals:

```bash
node tools/dev-windows.js --dev --scenario f2 --interval 500   # telemetry on :4100
IM_VALIDATE=1 node control/server.js                           # control plane on :4200
cd console && npm run dev                                      # console on :5173
```

Then open <http://127.0.0.1:5173>. Give it about 25 seconds for the incident to develop.

`IM_VALIDATE=1` turns on contract validation of every inbound window and every published object. It
logs loudly and never throws. Leave it on during development.

### Three demos worth running

1. **Healthy** — `--scenario healthy`. No incident, no hypotheses, no plan, no action.
2. **Incident requiring approval** — `--scenario f2`. An incident opens, causes are ranked, a plan
   appears with a rollback gated to a human. Nothing executes until you approve.
3. **Applied action, telemetry-based verdict** — `--scenario f2 --approve`. The action is applied
   with HTTP 200, and the Verifier then reads the windows that follow. The same 200 yields
   `RECOVERED` when telemetry recovers and `FAILED` when it does not.

---

## Endpoints

| | |
|---|---|
| `GET /health` | `{ svc: "control", up: true }` |
| `POST /reset` | Clears incident, hypotheses, probe, plan, verdict; aborts an in-flight probe. |
| `POST /approve` | `{ optionId }` — records consent for one option on one incident, then executes. |
| `WS /stream` (:4200) | `ControlState { incident, hypotheses[], probe, plan, verdict }`, full snapshot on connect. |
| `WS /stream` (:4100) | `WindowAggregate`, one per second, produced by the collector. |

Outbound, the control plane touches the data plane only through `POST /admin/*`, composed from a
four-entry allowlist in `control/adapters/admin.js`. The operator-only fault-injection routes are
unrepresentable from control-plane code.

---

## Safety model

- **Bounded probes.** Fraction ≤ 0.2, duration ≤ 5000 ms, taken from the frozen contract and
  enforced in three independent places. One probe in flight at a time.
- **Server-side expiry.** The probe ends on the data plane's own timer, so it ends even if the
  control plane dies mid-experiment.
- **Publish before execute.** Predictions and `publishedAt` reach the console before the
  intervention fires.
- **Approval gate.** Irreversible actions are `HUMAN` at any confidence. `HUMAN` is never
  downgraded; the only way past it is a recorded approval, scoped to one option on one incident.
- **Autonomy gate.** Reversible actions may run unattended only above `autonomyConfidence`, and only
  when they are the plan's own recommendation. `effectiveConfidence = posterior × observationConfidence`,
  so when the pipeline goes blind the system blocks itself.
- **Idempotency.** Execution identity is `incidentId:optionId`. A retried approval returns the
  existing ActionRecord and makes no second call.
- **Telemetry gaps → INCONCLUSIVE.** Missing windows are counted, never interpolated. Missing
  evidence is never treated as evidence of failure.

---

## Testing

459 assertions across twelve suites cover the agents individually, the adapters against a real HTTP
mock, and the whole loop end to end — including reset in every reachable state, WebSocket behaviour
under a stalled subscriber, and fifteen safety invariants.

The suites live outside this repository (they were run against it during development). The two
in-repo harnesses are:

```bash
node tools/calibrate.js              # scorer calibration — needs fixtures/, see below
node tools/demo.js --dev --scenario f1
```

---

## Scope

This repository contains the **P2** half of a two-person build. The data plane — five services, the
load generator, and the collector that produces `WindowAggregate` — is **P1's** work and is not
merged here yet.

Consequently:

- `npm run mesh` and `npm run reset` in `package.json` point at `tools/mesh.js` and `tools/reset.js`,
  which P1 owns and which do not exist in this tree.
- The `:4100` telemetry stream is served locally by `tools/dev-windows.js`.
- `packages/telemetry/`, `packages/util/`, `services/`, `collector/` and `loadgen/` are absent.

The control plane cannot tell the difference — the dev source binds the collector's port and speaks
the same contract — but it means everything demonstrated here runs against synthetic input.

---

## Limitations

**Scorer calibration is incomplete.** Tuning the four scoring weights requires recorded
`WindowAggregate` streams from real runs of the mesh (`fixtures/cascade-f1.jsonl`,
`cascade-f2.jsonl`). Those fixtures do not exist yet. `tools/calibrate.js` refuses to run without
them rather than quietly substituting synthetic data.

Consequently the chain

```
incident → genuinely ambiguous hypotheses → automatic probe → causal discrimination
```

is **unproven on real data**. On the current corpus the top-two margin is about 0.27 against a 0.10
threshold, so the Experimenter correctly refuses to probe and the ambiguity → probe transition never
fires by itself. Probe discrimination has been demonstrated only against constructed ambiguous
hypotheses, over the real admin adapter.

Analysis in `tools/calibrate.js` shows this is structural rather than a matter of nudging a number:
no single weight change closes the gap, and removing the shared-resource penalty entirely still
leaves a margin of 0.145.

**Everything `tools/dev-windows.js` produces is shaped input, not measurement.** It exists so the
control plane could be built before the collector did. No threshold derived against it means
anything until it is re-measured against the real mesh.

**State is in memory.** A control-plane restart loses the current incident. That is an accepted
limitation of the build window, not an oversight.

---

## Design notes

The system is deliberately deterministic and auditable end to end. Two decisions are worth calling
out because they cost effort and were kept anyway:

**There is no change-correlation term in the scorer.** Adding one would let a deploy marker resolve
the ambiguity for free, and the probe would become decoration. The ambiguity is kept real so the
experiment is necessary.

**The Planner cannot act.** It has no HTTP client and does not import the admin adapter. If planning
and executing lived in one component, "wait for approval" would be a branch someone could get wrong;
split, it is a property of the architecture.
