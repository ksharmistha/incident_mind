import React, { useEffect, useMemo, useRef, useState } from 'react';
import { useSocket } from './sockets.js';
import {
  PipelineStrip, TelemetryPanel, IncidentPanel, HypothesesPanel,
  ProbePanel, PlanPanel, ActionPanel, VerificationPanel,
} from './panels.jsx';

const COLLECTOR_WS = 'ws://127.0.0.1:4100/stream';
const CONTROL_WS = 'ws://127.0.0.1:4200/stream';

// The stage the control loop is in, derived entirely from ControlState. It is a view of
// existing data, not a state machine of its own — the backend has no such enum, and the
// console must not invent transitions the backend never made.
export function phaseOf(s) {
  if (!s || !s.incident) return 'IDLE';
  if (s.verdict) return s.verdict.verdict;
  if (s.plan?.verification) return 'VERIFYING';
  if (s.plan?.executing) return 'EXECUTING';
  if (s.plan?.action) return 'EXECUTED';
  if (s.plan) return s.plan.recommendedOptionId ? 'AWAITING_APPROVAL' : 'PLANNED';
  if (s.probe) return 'PROBING';
  if (s.hypotheses?.length) return 'HYPOTHESES';
  return 'INCIDENT';
}

export default function App() {
  const control = useSocket(CONTROL_WS);
  const collector = useSocket(COLLECTOR_WS);

  const state = control.message;
  const window = collector.message;
  const phase = phaseOf(state);

  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState(null);
  const [confirming, setConfirming] = useState(false);
  const history = useRef([]);

  // A short trail of phases, so a viewer who looks away can still see the path taken.
  useEffect(() => {
    const trail = history.current;
    if (trail[trail.length - 1] !== phase) {
      trail.push(phase);
      if (trail.length > 12) trail.shift();
    }
  }, [phase]);

  async function call(path, body, label) {
    setBusy(true);
    try {
      const res = await fetch(path, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(body ?? {}),
      });
      const json = await res.json().catch(() => null);
      setNotice({ ok: res.ok, label, status: res.status, json });
    } catch (err) {
      setNotice({ ok: false, label, status: 0, json: { error: err.message } });
    } finally {
      setBusy(false);
    }
  }

  const onApprove = (optionId) => call('/approve', { optionId }, `approve ${optionId}`);
  const onReset = () => { setConfirming(false); history.current = []; setNotice(null); call('/reset', {}, 'reset'); };

  const staleMs = control.lastAt ? Date.now() - control.lastAt : null;

  return (
    <div className="app">
      <header className="header">
        <div className="brand">
          <strong>IncidentMind</strong> <span className="dim">control plane</span>
        </div>
        <div className={`phase phase-${phase}`}>{phase.replace(/_/g, ' ')}</div>
        <div className="links">
          <Conn label="control :4200" on={control.connected} />
          <Conn label="collector :4100" on={collector.connected} />
          {confirming ? (
            <span className="confirm">
              Reset control-plane state?
              <button className="btn danger" disabled={busy} onClick={onReset}>Confirm</button>
              <button className="btn" onClick={() => setConfirming(false)}>Cancel</button>
            </span>
          ) : (
            <button className="btn" disabled={busy} onClick={() => setConfirming(true)}>Reset</button>
          )}
        </div>
      </header>

      <PipelineStrip phase={phase} state={state} trail={history.current} />

      {notice && (
        <div className={`notice ${notice.ok ? 'good' : 'bad'}`}>
          <strong>{notice.label}</strong> → HTTP {notice.status}
          {notice.json?.error && <> — {notice.json.error}</>}
          {notice.json?.duplicate && <> — duplicate, returning the existing record</>}
          {notice.json?.actionRecord && <> — {notice.json.actionRecord.actionId} {notice.json.actionRecord.outcome}</>}
          <button className="btn tiny" onClick={() => setNotice(null)}>dismiss</button>
        </div>
      )}

      <main className="grid">
        <section className="col">
          <TelemetryPanel window={window} connected={collector.connected} />
          <IncidentPanel incident={state?.incident} />
        </section>
        <section className="col">
          <HypothesesPanel hypotheses={state?.hypotheses} />
          <ProbePanel probe={state?.probe} hypotheses={state?.hypotheses} phase={phase} />
        </section>
        <section className="col">
          <PlanPanel plan={state?.plan} phase={phase} busy={busy} onApprove={onApprove} />
          <ActionPanel plan={state?.plan} />
          <VerificationPanel plan={state?.plan} verdict={state?.verdict} />
        </section>
      </main>

      <footer className="footer">
        {control.connected
          ? <>live · last control update {staleMs === null ? '—' : `${Math.round(staleMs / 1000)}s ago`}</>
          : <span className="bad-text">control stream disconnected — reconnecting</span>}
        <span className="dim"> · APPLIED means the data plane accepted the action. Only the verdict reflects telemetry.</span>
      </footer>
    </div>
  );
}

function Conn({ label, on }) {
  return <span className={`conn ${on ? 'on' : 'off'}`}><i /> {label}</span>;
}
