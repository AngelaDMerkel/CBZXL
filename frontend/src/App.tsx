import { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";

type Json = Record<string, any>;
type View = "overview" | "attention" | "history" | "settings";
type ToastValue = { kind: "success" | "error"; message: string } | null;

async function api<T = Json>(path: string, options: RequestInit = {}): Promise<T> {
  const headers = new Headers(options.headers);
  if (options.body && typeof options.body === "string" && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }
  const response = await fetch(path, { ...options, headers });
  if (!response.ok) {
    let message = `Request failed (${response.status})`;
    try {
      const payload = await response.json();
      if (typeof payload.detail === "string") message = payload.detail;
    } catch { /* non-JSON error */ }
    throw new Error(message);
  }
  return response.status === 204 ? ({} as T) : response.json();
}

function formatBytes(value?: number | null) {
  const bytes = Number(value || 0);
  const absolute = Math.abs(bytes);
  if (absolute >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(absolute >= 10 * 1024 ** 3 ? 1 : 2)} GB`;
  if (absolute >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  if (absolute >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes.toLocaleString()} B`;
}

function formatGb(value?: number | null) {
  return formatBytes(Number(value || 0) * 1024 ** 3);
}

function dateTime(value?: string | null) {
  if (!value) return "Not yet";
  return new Date(value).toLocaleString(undefined, {
    month: "short", day: "numeric", hour: "numeric", minute: "2-digit",
  });
}

function duration(secondsValue?: number | null) {
  const seconds = Math.max(0, Number(secondsValue || 0));
  if (!seconds) return "Calculating";
  if (seconds < 60) return `${Math.round(seconds)} sec`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.round((seconds % 3600) / 60)}m`;
}

function archiveName(path = "") {
  return path.split(/[\\/]/).pop() || path;
}

function label(value = "") {
  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function Logo() {
  return <svg viewBox="0 0 28 28" role="img" aria-label="Manga page being compressed">
    <rect x="9" y="3" width="10" height="22" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1.5" />
    <rect x="11" y="5.5" width="6" height="6" rx=".5" fill="none" stroke="currentColor" strokeWidth="1.1" />
    <path d="M11 14h2.4v3.6H11zm3.6 0H17v3.6h-2.4zM11 21h6M2 14h5m-2.5-2.5L7 14l-2.5 2.5M26 14h-5m2.5-2.5L21 14l2.5 2.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
  </svg>;
}

function Toast({ value }: { value: ToastValue }) {
  return value ? <div className={`toast ${value.kind}`} role="status">{value.message}</div> : null;
}

function LiveConversion({ initial, refresh, notify }: {
  initial: Json;
  refresh: () => void;
  notify: (message: string, kind?: "success" | "error") => void;
}) {
  const [detail, setDetail] = useState<Json>({ job: initial, events: [] });
  useEffect(() => {
    const load = () => api(`/api/jobs/${initial.id}`).then(setDetail).catch(() => undefined);
    load();
    const timer = window.setInterval(load, 1800);
    return () => window.clearInterval(timer);
  }, [initial.id]);
  const job = detail.job || initial;
  const percent = Number(job.smooth_percent ?? job.percent ?? 0);
  const stop = async () => {
    try {
      await api(`/api/jobs/${job.id}/cancel`, { method: "POST" });
      notify("The worker is stopping safely");
      refresh();
    } catch (error) {
      notify((error as Error).message, "error");
    }
  };
  return <section className="run-card">
    <div className="run-top">
      <div className="run-copy">
        <span className="status-pill"><i />{job.status === "queued" ? "Waiting for worker" : label(job.current_stage || "Converting")}</span>
        <h2>{job.current_archive ? archiveName(job.current_archive) : "Discovering your library"}</h2>
        <code>{job.current_archive || "Preparing the conversion queue…"}</code>
      </div>
      <button className="button secondary" onClick={stop} disabled={job.status === "stopping"}>
        {job.status === "stopping" ? "Stopping…" : "Stop"}
      </button>
    </div>
    <div className="progress-track" aria-label={`${percent.toFixed(1)} percent complete`}><span style={{ width: `${percent}%` }} /></div>
    <div className="run-facts">
      <div><span>Overall</span><strong>{Math.round(percent)}%</strong></div>
      <div><span>Remaining</span><strong>{duration(job.eta_seconds)}</strong></div>
      <div><span>Throughput</span><strong>{job.rate_per_minute ? `${job.rate_per_minute}/min` : "Learning"}</strong></div>
      <div><span>Reclaimed</span><strong>{formatGb(job.net_gb)}</strong></div>
    </div>
  </section>;
}

function ManualConversion({ bootstrap, onQueued, notify }: {
  bootstrap: Json;
  onQueued: () => void;
  notify: (message: string, kind?: "success" | "error") => void;
}) {
  const profile = bootstrap.profiles?.find((row: Json) => row.is_default) || bootstrap.profiles?.[0] || {};
  const defaults = profile.options || {};
  const [open, setOpen] = useState(false);
  const [scope, setScope] = useState<"library" | "failed">("library");
  const [policy, setPolicy] = useState<"lossless" | "smart" | "recheck">("lossless");
  const [effort, setEffort] = useState(Number(defaults.effort ?? 7));
  const [threads, setThreads] = useState(Number(defaults.threads ?? 4));
  const [backup, setBackup] = useState(Boolean(defaults.backup));
  const [busy, setBusy] = useState(false);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    const options: Json = {
      effort,
      threads,
      backup,
      no_convert: policy === "recheck",
      recheck_all: policy === "recheck",
      smart_distance: policy === "smart",
      distance: policy === "smart" ? 0.5 : null,
    };
    try {
      await api("/api/jobs", {
        method: "POST",
        body: JSON.stringify({
          library_id: bootstrap.libraries?.[0]?.id,
          profile_id: profile.id,
          scope,
          options,
        }),
      });
      notify("Conversion queued");
      setOpen(false);
      onQueued();
    } catch (error) {
      notify((error as Error).message, "error");
    } finally {
      setBusy(false);
    }
  };

  return <section className={`manual-conversion breathing ${open ? "open" : ""}`}>
    {!open ? <button className="manual-launch" type="button" onClick={() => setOpen(true)}>Begin Manual Conversion</button> :
      <form className="manual-form" onSubmit={submit}>
        <div className="choice-stack">
          <div className="choice-nav" role="group" aria-label="Conversion scope">
            <button type="button" className={scope === "library" ? "selected" : ""} onClick={() => setScope("library")}>▤ Entire Library</button>
            <button type="button" className={scope === "failed" ? "selected" : ""} onClick={() => setScope("failed")}>△ Failed</button>
          </div>
          <div className="choice-nav" role="group" aria-label="Conversion policy">
            <button type="button" className={policy === "lossless" ? "selected" : ""} onClick={() => setPolicy("lossless")}>✓ Balanced lossless</button>
            <button type="button" className={policy === "smart" ? "selected" : ""} onClick={() => setPolicy("smart")}>✦ Smart lossy fallback</button>
            <button type="button" className={policy === "recheck" ? "selected" : ""} onClick={() => setPolicy("recheck")}>⌕ Recheck without conversion</button>
          </div>
        </div>
        <div className="manual-fields">
          <label><span>Encoder effort · {effort}</span><input type="range" min="0" max="11" value={effort} onChange={(event) => setEffort(Number(event.target.value))} /></label>
          <label><span>CPU budget</span><select value={threads} onChange={(event) => setThreads(Number(event.target.value))}>{[1, 2, 4, 6, 8, 12, 16].map((value) => <option key={value} value={value}>{value} threads</option>)}</select></label>
        </div>
        <label className="switch"><input type="checkbox" checked={backup} onChange={(event) => setBackup(event.target.checked)} /><i /><span>Back up originals</span></label>
        <footer className="manual-actions">
          <button className="button ghost" type="button" onClick={() => setOpen(false)}>Cancel</button>
          <button className="button primary" type="submit" disabled={busy}>{busy ? "Queuing…" : "Begin conversion"}</button>
        </footer>
      </form>}
  </section>;
}

function Overview({ dashboard, bootstrap, refresh, notify }: {
  dashboard: Json;
  bootstrap: Json;
  refresh: () => void;
  notify: (message: string, kind?: "success" | "error") => void;
}) {
  const active = dashboard.active_jobs?.[0];
  const totals = dashboard.totals || {};
  return <div className="page overview">
    {active ? <LiveConversion initial={active} refresh={refresh} notify={notify} /> :
      <ManualConversion bootstrap={bootstrap} onQueued={refresh} notify={notify} />}
    <section className="stats-grid" aria-label="Lifetime optimization statistics">
      <div><span>Gross reduction</span><strong>{formatGb(totals.saved_gb)}</strong></div>
      <div><span>Pages optimized</span><strong>{Number(totals.page_count || 0).toLocaleString()}</strong></div>
      <div><span>Archives optimized</span><strong>{Number(totals.optimized_count || 0).toLocaleString()}</strong></div>
      <div><span>Need attention</span><strong>{Number(dashboard.review?.total || 0).toLocaleString()}</strong></div>
    </section>
  </div>;
}

function issueContext(issue: Json) {
  if (issue.kind === "archive_grew") return {
    title: "No conversion candidate was beneficial",
    text: "Larger candidates were discarded. The original archive remains unchanged.",
    recommended: "Dismiss until the next full rescan",
  };
  if (issue.kind === "verification_failed") return {
    title: "The converted page failed verification",
    text: "The candidate archive was rejected before replacement. The original remains intact.",
    recommended: "Compatibility retry",
  };
  if (issue.entry_path) return {
    title: "This image could not be processed",
    text: "The archive remains intact. You can keep, replace, or explicitly delete this page.",
    recommended: "Replace or preserve the page",
  };
  if (issue.kind === "unsafe_archive_path" || issue.kind === "filename_collision") return {
    title: "Automatic repair would require changing a name",
    text: "CBZXL will not rename archive entries. This item needs external correction.",
    recommended: "Correct the source archive",
  };
  return {
    title: "The archive could not be converted",
    text: "The original was preserved. A safer retry may resolve an encoder or resource failure.",
    recommended: "Compatibility retry",
  };
}

function IssuePreview({ issue }: { issue: Json }) {
  const [failed, setFailed] = useState(false);
  if (!issue.preview_url || failed) return <figure className="issue-preview unavailable"><span>⌁</span><figcaption>{issue.entry_path ? "Preview unavailable" : "Archive-level issue"}</figcaption></figure>;
  return <figure className="issue-preview"><img src={issue.preview_url} alt={`Problem page ${issue.entry_path}`} onError={() => setFailed(true)} /><figcaption><code>{issue.entry_path}</code></figcaption></figure>;
}

const remedyLabels: Record<string, string> = {
  compatibility: "Compatibility retry",
  preserve: "Keep page, convert rest",
  replace: "Replace image",
  delete: "Delete image",
  lossless: "Lossless retry",
  retry: "Default retry",
  dismiss: "Accept unchanged",
};

function Attention({ refreshDashboard, notify }: {
  refreshDashboard: () => void;
  notify: (message: string, kind?: "success" | "error") => void;
}) {
  const [data, setData] = useState<Json>({ rows: [], total: 0 });
  const [open, setOpen] = useState<number | null>(null);
  const [busy, setBusy] = useState<number | null>(null);
  const fileInputs = useRef<Record<number, HTMLInputElement | null>>({});
  const load = useCallback(() => api("/api/issues").then(setData).catch((error) => notify(error.message, "error")), [notify]);
  useEffect(() => { load(); }, [load]);

  const act = async (issue: Json, action: string) => {
    if (action === "delete" && !window.confirm(`Delete ${issue.entry_path} from ${archiveName(issue.path)}? The archive name will not change.`)) return;
    setBusy(issue.id);
    try {
      await api(`/api/issues/${issue.id}/action`, { method: "POST", body: JSON.stringify({ action }) });
      notify(action === "dismiss" ? "Dismissed until the next full rescan" : "Repair queued");
      if (action === "dismiss") setOpen(null);
      await load();
      refreshDashboard();
    } catch (error) {
      notify((error as Error).message, "error");
    } finally {
      setBusy(null);
    }
  };

  const replace = async (issue: Json, file?: File) => {
    if (!file) return;
    setBusy(issue.id);
    try {
      await api(`/api/issues/${issue.id}/replacement`, {
        method: "PUT",
        headers: { "Content-Type": file.type || "application/octet-stream" },
        body: file,
      });
      notify("Replacement validated and queued");
      await load();
      refreshDashboard();
    } catch (error) {
      notify((error as Error).message, "error");
    } finally {
      setBusy(null);
    }
  };

  if (!data.rows?.length) return <div className="empty-state"><span>✓</span><h2>Nothing needs attention.</h2><p>Failures and exceptions will appear here with actions tailored to their cause.</p></div>;
  return <div className="issue-list">
    {data.rows.map((issue: Json) => {
      const context = issueContext(issue);
      const expanded = open === issue.id;
      return <article className={`issue ${expanded ? "expanded" : ""}`} key={issue.id}>
        <div className="issue-summary">
          <div><strong>{archiveName(issue.path)}</strong><span className={issue.kind === "archive_grew" ? "" : "error-copy"}>{issue.message}</span></div>
          <time>{dateTime(issue.updated_at)}</time>
          <div className="issue-actions">
            <button className="button primary" disabled={busy === issue.id || issue.status === "queued"} onClick={() => act(issue, "retry")}>{issue.status === "queued" ? "Queued" : "Retry"}</button>
            <button className="button secondary" aria-expanded={expanded} onClick={() => setOpen(expanded ? null : issue.id)}>{expanded ? "Close" : "Inspect"}</button>
            <button className="button ghost" disabled={busy === issue.id} onClick={() => act(issue, "dismiss")}>Dismiss</button>
          </div>
        </div>
        {expanded && <div className={`issue-detail ${issue.entry_path ? "" : "without-preview"}`}>
          {issue.entry_path && <IssuePreview issue={issue} />}
          <div className="diagnosis">
            <div><h3>{context.title}</h3><p>{context.text}</p></div>
            <div className="diagnosis-facts">
              <div><span>Failure stage</span><strong>{label(issue.stage)}</strong></div>
              <div><span>Recommended</span><strong>{context.recommended}</strong></div>
            </div>
            {issue.detail && <details><summary>Technical detail</summary><code>{issue.detail}</code></details>}
            <input className="visually-hidden" type="file" accept="image/*" ref={(node) => { fileInputs.current[issue.id] = node; }} onChange={(event) => replace(issue, event.target.files?.[0])} />
            <div className="remedy-grid">
              {(issue.remedies || []).map((remedy: string) => remedy === "replace" ?
                <button className="button secondary block" key={remedy} disabled={busy === issue.id} onClick={() => fileInputs.current[issue.id]?.click()}>{remedyLabels[remedy]}</button> :
                <button className={`button block ${remedy === "compatibility" || remedy === "lossless" ? "primary" : remedy === "delete" ? "danger" : "secondary"}`} key={remedy} disabled={busy === issue.id} onClick={() => act(issue, remedy)}>{remedyLabels[remedy] || label(remedy)}</button>)}
            </div>
          </div>
        </div>}
      </article>;
    })}
  </div>;
}

function HistoryDetail({ id }: { id: string }) {
  const [detail, setDetail] = useState<Json | null>(null);
  useEffect(() => { api(`/api/jobs/${id}`).then(setDetail).catch(() => setDetail({ events: [] })); }, [id]);
  if (!detail) return <div className="history-loading" />;
  const job = detail.job || {};
  return <div className="history-detail">
    <div className="history-facts">
      <div><span>Duration</span><strong>{duration(job.elapsed_seconds)}</strong></div>
      <div><span>Processed</span><strong>{job.processed || 0}</strong></div>
      <div><span>Skipped</span><strong>{job.skipped || 0}</strong></div>
      <div><span>Failed</span><strong>{job.failed || 0}</strong></div>
    </div>
    <div className="event-list">{(detail.events || []).slice(0, 20).map((event: Json) => <div key={event.id}><i className={event.level} /><time>{dateTime(event.created_at)}</time><strong>{event.message}</strong>{event.archive_path && <code>{event.archive_path}</code>}</div>)}</div>
  </div>;
}

function History({ notify }: { notify: (message: string, kind?: "success" | "error") => void }) {
  const [rows, setRows] = useState<Json[]>([]);
  const [open, setOpen] = useState<string | null>(null);
  const load = useCallback(() => api("/api/jobs?limit=100").then((value) => setRows(value.rows)).catch((error) => notify(error.message, "error")), [notify]);
  useEffect(() => { load(); }, [load]);
  return <div className="history-list">
    {rows.map((row) => <article key={row.id}>
      <button className="history-summary" onClick={() => setOpen(open === row.id ? null : row.id)} aria-expanded={open === row.id}>
        <span className={`history-dot ${row.status}`} />
        <div><strong>{row.source === "schedule" ? "Scheduled conversion" : row.source === "retry" ? "Archive repair" : "Library conversion"}</strong><time>{dateTime(row.started_at || row.queued_at)}</time></div>
        <span>{row.processed || 0} converted · {row.failed || 0} failed</span>
        <strong>{formatGb(row.net_gb)}</strong>
        <span className="status-label">{label(row.status)}</span>
        <b>{open === row.id ? "−" : "+"}</b>
      </button>
      {open === row.id && <HistoryDetail id={row.id} />}
    </article>)}
  </div>;
}

function Settings({ bootstrap, reload, notify }: {
  bootstrap: Json;
  reload: () => void;
  notify: (message: string, kind?: "success" | "error") => void;
}) {
  const [schedules, setSchedules] = useState<Json[]>([]);
  const [newOpen, setNewOpen] = useState(false);
  const [form, setForm] = useState({ name: "Library maintenance", cadence: "daily", hour: 4, minute: 0 });
  const load = useCallback(() => api("/api/schedules").then((value) => setSchedules(value.rows)).catch((error) => notify(error.message, "error")), [notify]);
  useEffect(() => { load(); }, [load]);
  const profile = bootstrap.profiles?.find((row: Json) => row.is_default) || bootstrap.profiles?.[0];
  const save = async (event: FormEvent) => {
    event.preventDefault();
    try {
      await api("/api/schedules", {
        method: "POST",
        body: JSON.stringify({
          ...form,
          enabled: true,
          weekday: form.cadence === "weekly" ? 0 : null,
          day_of_month: form.cadence === "monthly" ? 1 : null,
          library_id: bootstrap.libraries?.[0]?.id,
          profile_id: profile?.id,
        }),
      });
      notify("Schedule created");
      setNewOpen(false);
      load();
      reload();
    } catch (error) { notify((error as Error).message, "error"); }
  };
  const toggle = async (row: Json) => {
    try {
      await api(`/api/schedules/${row.id}`, { method: "PUT", body: JSON.stringify({ ...row, enabled: !row.enabled }) });
      notify(row.enabled ? "Schedule paused" : "Schedule enabled");
      load();
      reload();
    } catch (error) { notify((error as Error).message, "error"); }
  };
  const remove = async (row: Json) => {
    if (!window.confirm(`Delete ${row.name}? Its history will remain.`)) return;
    try {
      await api(`/api/schedules/${row.id}`, { method: "DELETE" });
      notify("Schedule deleted");
      load();
      reload();
    } catch (error) { notify((error as Error).message, "error"); }
  };
  return <div className="settings-page">
    <section className="settings-grid">
      <div><span>Library</span><strong><code>{bootstrap.libraries?.[0]?.root_path || "/data"}</code></strong></div>
      <div><span>Conversion policy</span><strong>{profile?.name || "Balanced lossless"}</strong></div>
      <div><span>Schedule</span><strong>{schedules.find((row) => row.enabled)?.description || "Not scheduled"}</strong></div>
    </section>
    <section className="schedule-section">
      <header><div><h2>Schedules</h2><p>Recurring conversions use the same verified pipeline.</p></div><button className="button primary" onClick={() => setNewOpen(!newOpen)}>{newOpen ? "Close" : "New schedule"}</button></header>
      {newOpen && <form className="schedule-form" onSubmit={save}>
        <label><span>Name</span><input value={form.name} onChange={(event) => setForm({ ...form, name: event.target.value })} required /></label>
        <label><span>Cadence</span><select value={form.cadence} onChange={(event) => setForm({ ...form, cadence: event.target.value })}><option value="daily">Daily</option><option value="weekly">Weekly</option><option value="monthly">Monthly</option></select></label>
        <label><span>Hour</span><select value={form.hour} onChange={(event) => setForm({ ...form, hour: Number(event.target.value) })}>{Array.from({ length: 24 }, (_, hour) => <option key={hour} value={hour}>{new Date(2000, 0, 1, hour).toLocaleTimeString([], { hour: "numeric" })}</option>)}</select></label>
        <button className="button primary" type="submit">Save schedule</button>
      </form>}
      <div className="schedule-list">{schedules.map((row) => <article key={row.id} className={!row.enabled ? "disabled" : ""}><div><strong>{row.name}</strong><span>{row.description}</span></div><span>{row.enabled ? `Next ${dateTime(row.next_run_at)}` : "Paused"}</span><div><button className="button secondary" onClick={() => toggle(row)}>{row.enabled ? "Pause" : "Enable"}</button><button className="button ghost danger" onClick={() => remove(row)}>Delete</button></div></article>)}</div>
    </section>
  </div>;
}

export default function App() {
  const initialHash = location.hash.slice(1) as View;
  const [view, setView] = useState<View>(["overview", "attention", "history", "settings"].includes(initialHash) ? initialHash : "overview");
  const [bootstrap, setBootstrap] = useState<Json>({ libraries: [], profiles: [], schedules: [] });
  const [dashboard, setDashboard] = useState<Json>({});
  const [toast, setToast] = useState<ToastValue>(null);
  const loadBootstrap = useCallback(() => api("/api/bootstrap").then(setBootstrap), []);
  const loadDashboard = useCallback(() => api("/api/dashboard").then(setDashboard).catch(() => undefined), []);
  useEffect(() => {
    loadBootstrap();
    loadDashboard();
    const timer = window.setInterval(loadDashboard, 3500);
    return () => window.clearInterval(timer);
  }, [loadBootstrap, loadDashboard]);
  const notify = useCallback((message: string, kind: "success" | "error" = "success") => {
    setToast({ message, kind });
    window.setTimeout(() => setToast(null), 3800);
  }, []);
  const navigate = (next: View) => {
    setView(next);
    location.hash = next;
    window.scrollTo({ top: 0, behavior: "smooth" });
  };
  const attention = Number(dashboard.review?.total || 0);
  const active = dashboard.active_jobs?.length > 0;
  const unavailable = bootstrap.database_status === "unavailable";
  const nav = useMemo(() => [
    { id: "overview" as View, label: "Overview" },
    { id: "attention" as View, label: "Attention", count: attention },
    { id: "history" as View, label: "History" },
    { id: "settings" as View, label: "Settings" },
  ], [attention]);
  return <div className="app-shell">
    <header className="app-header">
      <button className="brand" onClick={() => navigate("overview")}><span><Logo /></span><strong>CBZXL</strong></button>
      <nav aria-label="Primary navigation">{nav.map((item) => <button key={item.id} className={view === item.id ? "active" : ""} onClick={() => navigate(item.id)}>{item.label}{item.count ? <b>{item.count.toLocaleString()}</b> : null}</button>)}</nav>
      <span className={`service-state ${unavailable ? "error" : active ? "busy" : ""}`}><i />{unavailable ? "Database unavailable" : active ? "Converting" : "Ready"}</span>
    </header>
    <main className="app-content">
      {unavailable && <section className="database-alert" role="alert"><strong>The web interface is online, but the database could not be opened.</strong><code>{bootstrap.database_error}</code></section>}
      {!unavailable && view === "overview" && <Overview dashboard={dashboard} bootstrap={bootstrap} refresh={loadDashboard} notify={notify} />}
      {!unavailable && view === "attention" && <Attention refreshDashboard={loadDashboard} notify={notify} />}
      {!unavailable && view === "history" && <History notify={notify} />}
      {!unavailable && view === "settings" && <Settings bootstrap={bootstrap} reload={loadBootstrap} notify={notify} />}
    </main>
    <Toast value={toast} />
  </div>;
}
