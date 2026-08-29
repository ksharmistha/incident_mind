# IncidentMind V2 — Control Plane (P2)

The control plane consumes the collector's telemetry, decides what is wrong, and — when
observation alone cannot tell — runs a bounded experiment on the live system to find out.
It acts only through `POST /admin/*`, and it decides whether anything got better by reading
telemetry, never by reading its own HTTP responses.

## Architecture

```
collector :4100  ──WS /stream──▶  one WindowAggregate per second
                                        │
   adapters/telemetry.js  validate → 120-window ring (selected by windowId, never wall clock)
                                        │
   Detector      EWMA baseline per service; flags on p99 > baseline×3 or errRate > 0.02,
                 sustained; opens an incident at ≥2 flagged services or >5% user-facing errors
        ▼
   Scorer        four weighted terms over the observed graph → ranked Hypotheses + posteriors
        ▼
   Experimenter  if the top two are within probeMarginThreshold and confidence allows:
                 publish predictions, THEN shed ≤20% of an edge for ≤5s, measure, update belief
        ▼
   Planner       rollback | isolate | restart, with predicted recovers[]/degrades[] from
                 graph reachability, behind the autonomy gate. Describes; never acts.
        ▼
   Executor      the only component that acts, and only through adapters/admin.js
        ▼        ActionRecord: APPLIED or EXECUTION_FAILED. Never claims success.
   Verifier      observes post-action windows FROM THE PIPELINE ONLY
        ▼        RECOVERED | PARTIAL | FAILED | INCONCLUSIVE
   control :4200 ──WS /stream──▶ ControlState { incident, hypotheses[], probe, plan, verdict }
```

Inbound HTTP on `:4200`: `GET /health`, `POST /reset`, `POST /approve {optionId}`,
`GET /debug/status` (development only), and `WS /stream`.

## Running it locally

```
npm install                                  # from the committed lockfile
node tools/dev-windows.js --dev --scenario f2    # synthetic telemetry on the collector's port
IM_VALIDATE=1 node control/server.js             # the control plane on :4200
```

Or drive the whole loop with one command:

```
node tools/demo.js --dev --scenario f2 --approve
```

`--scenario healthy|spike|f1|f2` · `--approve` · `--interval <ms>`

### Three demos worth running

1. **Healthy** — `node tools/demo.js --dev --scenario healthy`
   No incident, no hypotheses, no plan, no action. The loop stays quiet.

2. **Incident requiring approval** — `node tools/demo.js --dev --scenario f2`
   Incident opens, hypotheses are ranked, a plan appears with a rollback recommended and
   gated to a human. Nothing executes. Add `--approve` to let it through.

3. **Applied action, telemetry-based verdict** — `node tools/demo.js --dev --scenario f2 --approve`
   The action is applied (HTTP 200) and the Verifier then reads the windows that follow.
   The same 200 produces `RECOVERED` when telemetry recovers and `FAILED` when it does not.

## Safety model

- **Bounded probes.** Fraction ≤ 0.2 and duration ≤ 5000 ms, taken from the frozen contract,
  clamped by the adapter and rejected by the service. One probe in flight at a time.
- **Server-side expiry.** The probe ends on the data plane's own timer, so it ends even if
  the control plane dies mid-experiment.
- **Publish before execute.** A probe's predictions and `publishedAt` reach the console
  before the intervention fires. That ordering is the falsifiability claim.
- **Approval gate.** Irreversible actions (`rollback`, `restart`) are `HUMAN` at any
  confidence. `HUMAN` is never downgraded; the only way past it is a recorded approval,
  scoped to one option on one incident.
- **Autonomy gate.** Reversible actions may run unattended only above
  `autonomyConfidence`, and only when they are the plan's own recommendation. Below the
  threshold everything is `BLOCKED` with a stated reason.
- **Idempotency.** Execution identity is `incidentId:optionId`. A retried approval returns
  the existing ActionRecord and makes no second call.
- **Execution is not recovery.** `APPLIED` means the data plane accepted an instruction.
  Whether the system improved is decided only by `classifyVerdict`, which is given
  telemetry summaries and the plan's prediction — never an ActionRecord or an HTTP status.
- **Telemetry gaps → INCONCLUSIVE.** Missing windows are counted, never interpolated, and
  missing evidence is never treated as evidence of failure.
- **One write path.** `adapters/admin.js` composes URLs from a four-entry allowlist, so the
  operator-only fault-injection routes are unrepresentable from here.

## Current limitation — read this before trusting a demo

**M7 calibration is blocked: the real fixtures (`fixtures/cascade-f1.jsonl`,
`cascade-f2.jsonl`) do not exist yet.**

Consequently the chain

```
incident → genuinely ambiguous hypotheses → automatic probe → causal discrimination
```

is **UNPROVEN on real data**. On the synthetic corpus the top-two margin is ~0.27 against a
0.10 threshold, so the Experimenter correctly refuses to probe and the ambiguity → probe
transition never fires by itself. Probe discrimination has been demonstrated only against
constructed ambiguous hypotheses (F1 → −64.9% → H1, F2 → −7.4% → H2) over the real admin
adapter.

Everything `tools/dev-windows.js` produces is **shaped input, not measurement**. It exists
so the control plane could be built before the collector existed, and it is deleted at the
feature freeze. No threshold derived against it means anything until it is re-measured
against the real mesh.

Run `node tools/calibrate.js` once fixtures land; it refuses to run without them rather
than quietly substituting synthetic data.
