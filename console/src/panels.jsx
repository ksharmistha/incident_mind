import React from 'react';

const STAGES = ['Telemetry', 'Detector', 'Scorer', 'Experimenter', 'Planner', 'Executor', 'Verifier'];

// Which agent owns the current phase. Derived from ControlState only.
const STAGE_OF = {
  IDLE: 0, INCIDENT: 1, HYPOTHESES: 2, PROBING: 3, PLANNED: 4, AWAITING_APPROVAL: 4,
  EXECUTING: 5, EXECUTED: 5, VERIFYING: 6,
  RECOVERED: 6, PARTIAL: 6, FAILED: 6, INCONCLUSIVE: 6,
};

export function PipelineStrip({ phase, state, trail }) {
  const active = STAGE_OF[phase] ?? 0;
  return (
    <div className="strip">
      {STAGES.map((name, i) => (
        <React.Fragment key={name}>
          <div className={`stage ${i === active ? 'active' : ''} ${i < active ? 'done' : ''}`}>
            <span className="stage-name">{name}</span>
            <span className="stage-note">{noteFor(i, state, phase)}</span>
          </div>
          {i < STAGES.length - 1 && <span className="arrow">→</span>}
        </React.Fragment>
      ))}
      <div className="trail">{trail.join(' → ')}</div>
    </div>
  );
}

function noteFor(i, s, phase) {
  if (!s) return '';
  switch (i) {
    case 1: return s.incident ? s.incident.id : 'no incident';
    case 2: return s.hypotheses?.length ? `${s.hypotheses.length} ranked` : '';
    case 3: return s.probe ? (s.probe.phase ?? 'published') : (s.hypotheses?.length ? 'not required' : '');
    case 4: return s.plan ? `${s.plan.options.length} options` : '';
    case 5: return s.plan?.action ? s.plan.action.outcome : (s.plan?.executing ? 'executing' : '');
    case 6: return s.verdict ? s.verdict.verdict : (s.plan?.verification ? 'observing' : '');
    default: return '';
  }
}

function Card({ title, right, children, tone }) {
  return (
    <div className={`card ${tone ? `tone-${tone}` : ''}`}>
      <div className="card-head"><h2>{title}</h2>{right}</div>
      <div className="card-body">{children}</div>
    </div>
  );
}
const Empty = ({ children }) => <p className="empty">{children}</p>;
const n = (v, d = 1) => (typeof v === 'number' && Number.isFinite(v) ? v.toFixed(d) : '—');
const pct = (v) => (typeof v === 'number' && Number.isFinite(v) ? `${(v * 100).toFixed(1)}%` : '—');

/* ---------------------------------------------------------------- telemetry */

export function TelemetryPanel({ window: w, connected }) {
  if (!w) {
    return <Card title="Live telemetry"><Empty>{connected ? 'waiting for the first window…' : 'collector stream not connected'}</Empty></Card>;
  }
  const services = Object.entries(w.services ?? {});
  const oc = w.observationConfidence;
  return (
    <Card title="Live telemetry" right={<span className="badge">win {w.windowId}</span>}>
      <div className="kv">
        <div><span>window</span><b>{w.windowId}</b></div>
        <div><span>range</span><b>{fmtRange(w.tStart, w.tEnd)}</b></div>
        <div><span>confidence</span><b className={oc < 0.4 ? 'bad-text' : ''}>{n(oc, 2)}</b></div>
        <div><span>shed level</span><b>{w.pipeline?.shedLevel ?? '—'}</b></div>
        <div><span>ingest</span><b>{w.pipeline?.ingestRate ?? '—'}/s</b></div>
        <div><span>pri-0 dropped</span><b>{w.pipeline?.dropped?.pri0 ?? '—'}</b></div>
      </div>
      <table className="tbl">
        <thead><tr><th>service</th><th>state</th><th>p99</th><th>selfP99</th><th>err</th></tr></thead>
        <tbody>
          {services.map(([name, s]) => (
            <tr key={name}>
              <td>{name}</td>
              <td><span className={`st st-${s?.state ?? 'UNKNOWN'}`}>{s?.state ?? 'UNKNOWN'}</span></td>
              <td>{n(s?.p99)}</td>
              <td>{n(s?.selfP99)}</td>
              <td>{pct(s?.errRate)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </Card>
  );
}

function fmtRange(a, b) {
  if (typeof a !== 'number' || typeof b !== 'number') return '—';
  const t = (ms) => new Date(ms).toISOString().slice(11, 19);
  return `${t(a)} → ${t(b)}`;
}

/* ---------------------------------------------------------------- incident */

export function IncidentPanel({ incident }) {
  if (!incident) return <Card title="Incident"><Empty>No active incident</Empty></Card>;
  return (
    <Card title="Incident" tone="warn" right={<span className="badge">{incident.id}</span>}>
      <p className="reason">{incident.reason}</p>
      <div className="kv">
        <div><span>opened at window</span><b>{incident.openedWindowId ?? '—'}</b></div>
        <div><span>latest window</span><b>{incident.lastWindowId ?? '—'}</b></div>
      </div>
      <div className="chips">
        {(incident.services ?? []).map((s) => <span key={s} className="chip warn">{s}</span>)}
      </div>
      {Array.isArray(incident.candidates) && incident.candidates.length > 0 && (
        <table className="tbl">
          <thead><tr><th>candidate</th><th>state</th><th>selfP99</th><th>downP99</th><th>first degraded</th></tr></thead>
          <tbody>
            {incident.candidates.map((c) => (
              <tr key={c.service}>
                <td>{c.service}</td>
                <td><span className={`st st-${c.state ?? 'UNKNOWN'}`}>{c.state ?? '—'}</span></td>
                <td>{n(c.selfP99)}</td>
                <td>{n(c.downstreamP99)}</td>
                <td>{c.firstDegradedAt ? new Date(c.firstDegradedAt).toISOString().slice(11, 19) : '—'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </Card>
  );
}

/* ---------------------------------------------------------------- hypotheses */

export function HypothesesPanel({ hypotheses }) {
  if (!hypotheses?.length) return <Card title="Hypotheses"><Empty>No hypotheses — nothing to diagnose</Empty></Card>;
  const margin = hypotheses.length >= 2 ? hypotheses[0].score - hypotheses[1].score : null;
  return (
    <Card title="Ranked causes" right={margin !== null && <span className="badge">margin {margin.toFixed(3)}</span>}>
      <table className="tbl">
        <thead><tr><th>#</th><th>service</th><th>failure mode</th><th>score</th><th>post.</th>
          <th title="temporalPrecedence">prec</th><th title="upstreamness">upst</th>
          <th title="amplificationTarget">amp</th><th title="sharedResourcePenalty">srp</th></tr></thead>
        <tbody>
          {hypotheses.map((h, i) => (
            <tr key={h.id} className={i === 0 ? 'top' : ''}>
              <td>{h.id}</td>
              <td><b>{h.rootCauseService}</b></td>
              <td className="mode">{h.failureMode}</td>
              <td><b>{n(h.score, 3)}</b></td>
              <td>{n(h.posterior, 3)}</td>
              <td>{n(h.terms?.temporalPrecedence, 2)}</td>
              <td>{n(h.terms?.upstreamness, 2)}</td>
              <td>{n(h.terms?.amplificationTarget, 2)}</td>
              <td>{n(h.terms?.sharedResourcePenalty, 2)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="statement">{hypotheses[0].statement}</p>
    </Card>
  );
}

/* ---------------------------------------------------------------- probe */

export function ProbePanel({ probe, hypotheses, phase }) {
  // Four distinct situations the backend genuinely distinguishes. The console must not
  // blur them: "not needed" is a decision, not a failure.
  if (!probe) {
    if (!hypotheses?.length) return <Card title="Causal probe"><Empty>Not applicable — no hypotheses yet</Empty></Card>;
    const margin = hypotheses.length >= 2 ? hypotheses[0].score - hypotheses[1].score : null;
    return (
      <Card title="Causal probe">
        <Empty>
          No probe run. {margin !== null
            ? <>Top-two margin <b>{margin.toFixed(3)}</b> — observation separated the candidates, so no experiment was needed.</>
            : <>Only one candidate; nothing to discriminate.</>}
        </Empty>
        <p className="dim small">The control plane publishes its reason for not probing; it is a decision, not an omission.</p>
      </Card>
    );
  }

  const r = probe.result;
  const tone = r ? (r.inconclusive ? 'warn' : 'good') : 'info';
  const status = !r ? (probe.phase ?? 'published') : (r.inconclusive ? 'INCONCLUSIVE' : `matched ${r.matched}`);

  return (
    <Card title="Causal probe" tone={tone} right={<span className="badge">{probe.id}</span>}>
      <div className={`probe-status ${tone}`}>{status}</div>
      <div className="kv">
        <div><span>intervention</span><b>shed {probe.intervention?.fraction} of {probe.intervention?.target} → {probe.intervention?.upstream}</b></div>
        <div><span>duration</span><b>{probe.intervention?.durationMs} ms</b></div>
        <div><span>discriminator</span><b>{probe.discriminator}</b></div>
        <div><span>published at</span><b>{probe.publishedAt ? new Date(probe.publishedAt).toISOString().slice(11, 23) : '—'}</b></div>
      </div>
      <div className="predictions">
        {Object.entries(probe.predictions ?? {}).map(([id, p]) => (
          <div key={id} className={r && r.matched === id ? 'pred matched' : 'pred'}>
            <b>{id}</b> {p.direction} {p.magnitude}
          </div>
        ))}
      </div>
      <p className="dim small">Predictions were published before the intervention ran.</p>
      {r && (
        <div className="kv">
          <div><span>measured delta</span><b>{n(r.measuredDeltaPct, 1)}%</b></div>
          <div><span>posteriors</span><b>{Object.entries(r.posteriors ?? {}).map(([k, v]) => `${k} ${v}`).join('  ')}</b></div>
          {r.reason && <div className="wide"><span>reason</span><b>{r.reason}</b></div>}
        </div>
      )}
      {probe.baselineWindows?.length > 0 && (
        <p className="dim small">baseline windows {JSON.stringify(probe.baselineWindows)}
          {probe.measureWindows?.length > 0 && <> · measured {JSON.stringify(probe.measureWindows)}</>}</p>
      )}
    </Card>
  );
}

/* ---------------------------------------------------------------- plan */

export function PlanPanel({ plan, phase, busy, onApprove }) {
  if (!plan) return <Card title="Plan"><Empty>No plan — the evidence does not yet name a root cause</Empty></Card>;
  const settled = !!(plan.action || plan.executing || plan.verification);
  return (
    <Card title="Plan" right={<span className="badge">effConf {n(plan.effectiveConfidence, 3)}</span>}>
      {plan.basis && (
        <p className="dim small">
          {plan.basis.rootCauseService} · {plan.basis.failureMode} · {plan.basis.evidence}
        </p>
      )}
      {plan.options.map((o) => {
        const recommended = o.id === plan.recommendedOptionId;
        const canApprove = o.autonomy === 'HUMAN' && !settled && !busy;
        return (
          <div key={o.id} className={`option ${recommended ? 'recommended' : ''}`}>
            <div className="option-head">
              <b>{o.actionType}</b> <span className="dim">on</span> <b>{o.target}</b>
              <span className={`badge autonomy-${o.autonomy}`}>{o.autonomy}</span>
              {o.reversible ? <span className="badge">reversible</span> : <span className="badge irrev">irreversible</span>}
              {recommended && <span className="badge rec">recommended</span>}
            </div>
            <div className="option-id">{o.id}{Object.keys(o.params ?? {}).length > 0 && <> · {JSON.stringify(o.params)}</>}</div>
            <div className="pred">
              <span className="good-text">recovers</span> {o.predicted?.recovers?.join(', ') || '—'}
              <span className="bad-text"> degrades</span> {o.predicted?.degrades?.join(', ') || '—'}
            </div>
            {o.gateReason && <div className="gate">{o.gateReason}</div>}
            {o.autonomy === 'HUMAN' && (
              <button className="btn approve" disabled={!canApprove} onClick={() => onApprove(o.id)}>
                {settled ? 'action already issued' : busy ? 'working…' : `Approve ${o.actionType}`}
              </button>
            )}
            {o.autonomy === 'BLOCKED' && <div className="dim small">Blocked — approval is not offered.</div>}
          </div>
        );
      })}
    </Card>
  );
}

/* ---------------------------------------------------------------- action */

export function ActionPanel({ plan }) {
  const a = plan?.action;
  const executing = plan?.executing;
  if (!a && !executing) {
    return <Card title="Action"><Empty>{plan ? 'Pending approval — nothing has been issued' : 'No action'}</Empty></Card>;
  }
  if (!a) {
    return (
      <Card title="Action" tone="info">
        <div className="probe-status info">EXECUTING</div>
        <div className="kv"><div><span>action</span><b>{executing.actionId}</b></div>
          <div><span>target</span><b>{executing.actionType} on {executing.target}</b></div></div>
      </Card>
    );
  }
  const applied = a.outcome === 'APPLIED';
  return (
    <Card title="Action" tone={applied ? 'info' : 'bad'}>
      <div className={`probe-status ${applied ? 'info' : 'bad'}`}>{a.outcome}</div>
      <div className="kv">
        <div><span>action</span><b>{a.actionId}</b></div>
        <div><span>target</span><b>{a.actionType ?? '—'} on {a.target}</b></div>
        <div><span>http status</span><b>{a.httpStatus ?? 'none'}</b></div>
        <div><span>issued</span><b>{a.issuedAt ? new Date(a.issuedAt).toISOString().slice(11, 19) : '—'}</b></div>
      </div>
      {a.error && <p className="bad-text small">{a.error}</p>}
      {applied && (
        <p className="caveat">
          Action applied — awaiting telemetry verification.
          <br /><span className="dim">The data plane accepted the instruction. That is not a claim that anything recovered.</span>
        </p>
      )}
    </Card>
  );
}

/* ---------------------------------------------------------------- verdict */

const VERDICT_TONE = { RECOVERED: 'good', PARTIAL: 'warn', FAILED: 'bad', INCONCLUSIVE: 'warn' };

export function VerificationPanel({ plan, verdict }) {
  const v = plan?.verification;
  if (!verdict && !v) return <Card title="Verification"><Empty>Not started — no applied action to verify</Empty></Card>;

  if (!verdict) {
    return (
      <Card title="Verification" tone="info">
        <div className="probe-status info">VERIFYING</div>
        <div className="kv">
          <div><span>watching from window</span><b>{v.anchorWindowId}</b></div>
          <div><span>windows observed</span><b>{v.observed} / {v.needed}</b></div>
          <div><span>gaps</span><b>{v.gaps}</b></div>
          <div className="wide"><span>expected to recover</span><b>{v.predicted?.recovers?.join(', ') || '—'}</b></div>
        </div>
        <p className="dim small">Reading telemetry only. The executor's HTTP response is not an input here.</p>
      </Card>
    );
  }

  const tone = VERDICT_TONE[verdict.verdict] ?? 'warn';
  return (
    <Card title="Verification" tone={tone}>
      <div className={`verdict ${tone}`}>{verdict.verdict}</div>
      <p className="reason">{verdict.reason}</p>
      <div className="kv">
        <div><span>action</span><b>{verdict.actionId}</b></div>
        <div><span>baseline windows</span><b>{JSON.stringify(verdict.baselineWindows ?? [])}</b></div>
        <div className="wide"><span>observed windows</span><b>{JSON.stringify(verdict.observedWindows ?? [])}</b></div>
      </div>
      {Array.isArray(verdict.detail) && verdict.detail.length > 0 && (
        <table className="tbl">
          <thead><tr><th>predicted service</th><th>before</th><th>after</th></tr></thead>
          <tbody>
            {verdict.detail.map((d) => (
              <tr key={d.service}>
                <td>{d.service}</td>
                <td><span className={`st st-${d.before}`}>{d.before}</span></td>
                <td><span className={`st st-${d.after}`}>{d.after}</span></td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {verdict.regressions?.length > 0 && (
        <p className="bad-text small">
          regressions: {verdict.regressions.map((r) => `${r.service} ${r.before}→${r.after}`).join(', ')}
        </p>
      )}
      <p className="dim small">Decided from the windows above, not from the action's HTTP status.</p>
    </Card>
  );
}
