import { FormEvent, ReactNode, useCallback, useEffect, useMemo, useState } from "react";

type View = "overview" | "library" | "activity" | "automations" | "data";
type Json = Record<string, any>;

async function api<T = Json>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options?.headers || {}) },
  });
  if (!response.ok) {
    let message = `Request failed (${response.status})`;
    try {
      const body = await response.json();
      message = typeof body.detail === "string" ? body.detail : message;
    } catch { /* response had no JSON body */ }
    throw new Error(message);
  }
  return response.status === 204 ? ({} as T) : response.json();
}

function formatBytes(value: number | null | undefined, signed = false) {
  const bytes = Number(value || 0);
  const abs = Math.abs(bytes);
  const prefix = signed && bytes > 0 ? "+" : "";
  if (abs >= 1024 ** 3) return `${prefix}${(bytes / 1024 ** 3).toFixed(abs >= 10 * 1024 ** 3 ? 1 : 2)} GB`;
  if (abs >= 1024 ** 2) return `${prefix}${(bytes / 1024 ** 2).toFixed(1)} MB`;
  if (abs >= 1024) return `${prefix}${(bytes / 1024).toFixed(1)} KB`;
  return `${prefix}${bytes.toLocaleString()} B`;
}

function formatGb(value: number | null | undefined) {
  return formatBytes(Number(value || 0) * 1024 ** 3);
}

function dateTime(value?: string | null) {
  if (!value) return "Not yet";
  return new Date(value).toLocaleString(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
}

function relativeTime(value?: string | null) {
  if (!value) return "Never";
  const seconds = Math.round((new Date(value).getTime() - Date.now()) / 1000);
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  if (Math.abs(seconds) < 90) return formatter.format(seconds, "second");
  const minutes = Math.round(seconds / 60);
  if (Math.abs(minutes) < 90) return formatter.format(minutes, "minute");
  const hours = Math.round(minutes / 60);
  if (Math.abs(hours) < 48) return formatter.format(hours, "hour");
  return formatter.format(Math.round(hours / 24), "day");
}

function duration(value?: number | null) {
  const seconds = Math.max(0, Number(value || 0));
  if (!seconds) return "—";
  if (seconds < 60) return `${Math.round(seconds)} sec`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  return `${Math.floor(seconds / 3600)}h ${Math.round((seconds % 3600) / 60)}m`;
}

function label(value = "") {
  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function archiveName(path = "") {
  return path.split(/[\\/]/).pop() || path;
}

function Status({ value }: { value: string }) {
  return <span className={`status status-${value}`}><i />{label(value)}</span>;
}

function PageIntro({ kicker, title, detail, action }: { kicker: string; title: string; detail: string; action?: ReactNode }) {
  return <header className="page-intro">
    <div><span className="kicker">{kicker}</span><h1>{title}</h1><p>{detail}</p></div>
    {action}
  </header>;
}

function Empty({ title, detail, action }: { title: string; detail: string; action?: ReactNode }) {
  return <div className="empty"><span className="empty-glyph">○</span><h3>{title}</h3><p>{detail}</p>{action}</div>;
}

function Modal({ title, eyebrow, onClose, children, wide = false }: { title: string; eyebrow: string; onClose: () => void; children: ReactNode; wide?: boolean }) {
  useEffect(() => {
    const close = (event: KeyboardEvent) => event.key === "Escape" && onClose();
    window.addEventListener("keydown", close);
    return () => window.removeEventListener("keydown", close);
  }, [onClose]);
  return <div className="modal-backdrop" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
    <section className={`modal ${wide ? "modal-wide" : ""}`} role="dialog" aria-modal="true" aria-label={title}>
      <header><div><span className="kicker">{eyebrow}</span><h2>{title}</h2></div><button className="close-button" onClick={onClose} aria-label="Close">×</button></header>
      {children}
    </section>
  </div>;
}

function Toast({ value }: { value: { kind: string; message: string } | null }) {
  return value ? <div className={`toast toast-${value.kind}`} role="status"><i />{value.message}</div> : null;
}

function ProgressRing({ percent }: { percent: number }) {
  const bounded = Math.max(0, Math.min(100, Number(percent || 0)));
  return <div className="progress-ring" style={{ background: `conic-gradient(var(--accent) ${bounded * 3.6}deg, var(--line-soft) 0deg)` }}>
    <div><strong>{Math.round(bounded)}%</strong><span>complete</span></div>
  </div>;
}

function MomentumBars({ samples }: { samples: Json[] }) {
  const values = samples.slice(-18).map((sample, index, rows) => index ? Math.max(0, Number(sample.completed || 0) - Number(rows[index - 1].completed || 0)) : 0);
  const max = Math.max(1, ...values);
  return <span className="momentum-bars" aria-label="Recent conversion momentum">{values.map((value, index) => <i key={index} style={{ height: `${20 + value / max * 80}%` }} />)}</span>;
}

function LiveConversion({ job, onStop }: { job: Json; onStop: (id: string) => void }) {
  const [detail, setDetail] = useState<Json>({ job, events: [] });
  useEffect(() => {
    setDetail((current) => ({ ...current, job }));
    const load = () => api(`/api/jobs/${job.id}`).then(setDetail).catch(() => undefined);
    load();
    const timer = window.setInterval(load, 2500);
    return () => window.clearInterval(timer);
  }, [job.id, job]);
  const current = detail.job || job;
  const livePercent = Number(current.smooth_percent ?? current.percent ?? 0);
  const events = (detail.events || []).slice(0, 4);
  return <section className="live-conversion">
    <div className="live-main">
      <ProgressRing percent={livePercent} />
      <div className="live-copy">
        <div className="live-heading"><span className="live-dot" /><span>{current.status === "queued" ? "Waiting for worker" : label(current.current_stage || "converting")}</span></div>
        <h2 title={current.current_archive}>{current.current_archive ? archiveName(current.current_archive) : "Discovering your library"}</h2>
        <p className="current-path">{current.current_archive || "The worker is preparing the conversion queue."}</p>
        <div className="linear-progress"><span style={{ width: `${livePercent}%` }} /></div>
        <p className="progress-copy"><strong>{current.completed?.toLocaleString() || 0}</strong> of {current.archives_found?.toLocaleString() || "—"} archives{current.total_pages ? ` · page ${current.current_page || 0} of ${current.total_pages}` : ""}</p>
      </div>
      <button className="quiet-button stop-button" disabled={current.status === "stopping"} onClick={() => onStop(current.id)}>{current.status === "stopping" ? "Stopping…" : "Stop"}</button>
    </div>
    <div className="live-facts">
      <div><span>Reclaimed</span><strong>{formatGb(current.net_gb)}</strong></div>
      <div><span>Throughput</span><strong>{current.rate_per_minute ? `${current.rate_per_minute}/min` : "Learning"}</strong><MomentumBars samples={detail.progress || []} /></div>
      <div><span>Time remaining</span><strong>{current.eta_seconds ? duration(current.eta_seconds) : "Calculating"}</strong></div>
      <div><span>Exceptions</span><strong className={current.failed ? "negative" : ""}>{current.failed || 0}</strong></div>
    </div>
    {events.length > 0 && <div className="event-line" aria-label="Latest conversion activity">{events.map((event: Json) => <span key={event.id} className={`event-${event.level}`}><i />{event.message}</span>)}</div>}
  </section>;
}

function OutcomeReport({ data }: { data: Json }) {
  const total = Math.max(1, Number(data.totals?.archive_count || 0));
  const outcomes = data.outcomes || [];
  const colors: Json = { processed: "#1f7353", skipped: "#9bb3a7", failed: "#c85b58", discovered: "#dec66b" };
  return <section className="report-block">
    <header><div><span className="kicker">Library health</span><h2>What the last scan left behind</h2></div><span>{Number(data.totals?.archive_count || 0).toLocaleString()} indexed</span></header>
    <div className="outcome-track">{outcomes.map((row: Json) => <span key={row.status} title={`${label(row.status)}: ${row.count}`} style={{ width: `${Math.max(1, row.count / total * 100)}%`, background: colors[row.status] || "#839187" }} />)}</div>
    <div className="outcome-legend">{outcomes.map((row: Json) => <div key={row.status}><i style={{ background: colors[row.status] || "#839187" }} /><strong>{Number(row.count).toLocaleString()}</strong><span>{label(row.status)}</span></div>)}</div>
  </section>;
}

function RecentActivity({ rows, onOpen }: { rows: Json[]; onOpen: () => void }) {
  return <section className="recent-block">
    <header><div><span className="kicker">Recent activity</span><h2>One line per run. Nothing to decipher.</h2></div><button className="text-button" onClick={onOpen}>See full history <span>→</span></button></header>
    {!rows.length ? <Empty title="No runs yet" detail="Your first conversion will leave a clear, durable record here." /> : <div className="run-list compact">{rows.slice(0, 5).map((row) => <article className="run-row" key={row.id}>
      <span className={`run-mark mark-${row.status}`} />
      <div className="run-primary"><strong>{row.source === "schedule" ? "Scheduled library scan" : row.source === "retry" ? "Archive recheck" : "Library scan"}</strong><span>{dateTime(row.started_at || row.queued_at)}</span></div>
      <div className="run-summary"><span>{row.processed || 0} converted</span>{row.failed ? <span className="negative">{row.failed} failed</span> : <span>No failures</span>}</div>
      <strong className={(row.net_gb || 0) < 0 ? "negative" : "positive"}>{formatGb(row.net_gb)}</strong>
      <Status value={row.status} />
    </article>)}</div>}
  </section>;
}

function Overview({ data, onScan, onNavigate, notify, refresh }: { data: Json; onScan: () => void; onNavigate: (view: View) => void; notify: (message: string, kind?: string) => void; refresh: () => void }) {
  const active = data.active_jobs?.[0];
  const stop = async (id: string) => {
    try { await api(`/api/jobs/${id}/cancel`, { method: "POST" }); notify("The worker is stopping safely"); refresh(); }
    catch (error) { notify((error as Error).message, "error"); }
  };
  return <div className="page-stack overview-page">
    <PageIntro kicker="Overview" title="Your comic library, under control." detail="Convert carefully, recover space, and keep a record you can actually understand." action={<button className="primary-button" onClick={onScan}><span>＋</span> Start conversion</button>} />
    {active ? <LiveConversion job={active} onStop={stop} /> : <section className="ready-state">
      <div><span className="ready-orbit"><i /></span><div><span className="kicker">Worker ready</span><h2>Nothing is running.</h2><p>Start a conversion when you’re ready. Existing work and schedules are safely stored.</p></div></div>
      <button className="quiet-button" onClick={onScan}>Choose what to run <span>→</span></button>
    </section>}
    <section className="storage-story">
      <div className="storage-total"><span className="kicker">Lifetime impact</span><strong>{formatGb(data.totals?.net_gb)}</strong><p>net space reclaimed</p></div>
      <div className="storage-details">
        <div><span>Gross reduction</span><strong>{formatGb(data.totals?.saved_gb)}</strong></div>
        <div><span>Pages optimized</span><strong>{Number(data.totals?.page_count || 0).toLocaleString()}</strong></div>
        <div><span>Average reduction</span><strong>{Number(data.totals?.average_percent_saved || 0).toFixed(1)}%</strong></div>
        <div><span>Needs attention</span><strong>{Number((data.review?.failed || 0) + (data.review?.grew || 0)).toLocaleString()}</strong></div>
      </div>
    </section>
    <OutcomeReport data={data} />
    <RecentActivity rows={data.recent_jobs || []} onOpen={() => onNavigate("activity")} />
  </div>;
}

function Library({ notify }: { notify: (message: string, kind?: string) => void }) {
  const [data, setData] = useState<Json>({ rows: [], total: 0, pages: 1 });
  const [search, setSearch] = useState("");
  const [status, setStatus] = useState("all");
  const [review, setReview] = useState("all");
  const [sort, setSort] = useState("updated");
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const load = useCallback(() => {
    setLoading(true);
    const query = new URLSearchParams({ search, status, review, sort, page: String(page), page_size: "50" });
    api(`/api/archives?${query}`).then(setData).catch((error) => notify(error.message, "error")).finally(() => setLoading(false));
  }, [search, status, review, sort, page, notify]);
  useEffect(() => { const timer = window.setTimeout(load, 180); return () => window.clearTimeout(timer); }, [load]);
  const act = async (id: number, action: string) => {
    try { await api(`/api/archives/${id}/review`, { method: "POST", body: JSON.stringify({ action, note: "" }) }); notify(action === "queue_recheck" ? "Archive queued for recheck" : "Exception resolved"); load(); }
    catch (error) { notify((error as Error).message, "error"); }
  };
  const setFilter = (setter: (value: string) => void) => (event: React.ChangeEvent<HTMLSelectElement>) => { setter(event.target.value); setPage(1); };
  return <div className="page-stack">
    <PageIntro kicker="Library" title="Every archive. One useful view." detail="Find an archive, see what happened, and take action without reading a spreadsheet." />
    <section className="library-tools">
      <label className="search-field"><span>⌕</span><input value={search} onChange={(event) => { setSearch(event.target.value); setPage(1); }} placeholder="Search by title or folder" aria-label="Search archives" /></label>
      <div className="filter-group"><select value={status} onChange={setFilter(setStatus)} aria-label="Filter by status"><option value="all">Any outcome</option><option value="processed">Converted</option><option value="failed">Failed</option></select><select value={review} onChange={setFilter(setReview)} aria-label="Filter by review"><option value="all">All records</option><option value="needs_review">Needs attention</option><option value="reviewed">Resolved</option></select><select value={sort} onChange={setFilter(setSort)} aria-label="Sort archives"><option value="updated">Recently updated</option><option value="saved">Most space saved</option><option value="path">Title A–Z</option><option value="duration">Longest conversion</option></select></div>
      <span className="result-count">{Number(data.total || 0).toLocaleString()} archives</span>
    </section>
    <section className="archive-list-shell">
      {loading ? <div className="skeleton-list">{Array.from({ length: 7 }, (_, i) => <i key={i} />)}</div> : !data.rows.length ? <Empty title="Nothing found" detail="Try another title or clear one of the filters." /> : <div className="archive-list">{data.rows.map((row: Json) => <article className="archive-row" key={row.id}>
        <div className={`archive-state state-${row.status}`}><span>{row.status === "failed" ? "!" : row.review_status === "reviewed" ? "✓" : "·"}</span></div>
        <div className="archive-title"><strong title={row.path}>{archiveName(row.path)}</strong><span title={row.path}>{row.path}</span></div>
        <div className="archive-fact"><span>Format</span><strong>{row.dominant_type || "Unknown"}</strong><small>{row.image_count ? `${row.image_count} pages` : ""}</small></div>
        <div className="archive-fact"><span>Space change</span><strong className={(row.bytes_saved || 0) < 0 ? "negative" : "positive"}>{formatBytes(row.bytes_saved)}</strong><small>{row.percent_saved == null ? "" : `${Number(row.percent_saved).toFixed(1)}%`}</small></div>
        <div className="archive-fact archive-updated"><span>Updated</span><strong>{relativeTime(row.updated_at)}</strong></div>
        <div className="archive-actions">{row.review_status !== "reviewed" && (row.status === "failed" || row.bytes_saved < 0) && <button onClick={() => act(row.id, "reviewed")}>Resolve</button>}<button onClick={() => act(row.id, "queue_recheck")}>Recheck</button></div>
      </article>)}</div>}
      <footer className="pagination"><button disabled={page <= 1} onClick={() => setPage((value) => value - 1)}>← Previous</button><span>{page} / {data.pages}</span><button disabled={page >= data.pages} onClick={() => setPage((value) => value + 1)}>Next →</button></footer>
    </section>
  </div>;
}

function RunDetail({ id }: { id: string }) {
  const [detail, setDetail] = useState<Json | null>(null);
  useEffect(() => { api(`/api/jobs/${id}`).then(setDetail).catch(() => setDetail({ events: [], results: [] })); }, [id]);
  if (!detail) return <div className="detail-loading" />;
  const events = detail.events || [];
  return <div className="run-detail">
    <div className="detail-facts"><div><span>Duration</span><strong>{duration(detail.job?.elapsed_seconds)}</strong></div><div><span>Processed</span><strong>{detail.job?.processed || 0}</strong></div><div><span>Skipped</span><strong>{detail.job?.skipped || 0}</strong></div><div><span>Failed</span><strong>{detail.job?.failed || 0}</strong></div></div>
    <div className="event-timeline">{events.length ? events.slice(0, 12).map((event: Json) => <div key={event.id}><i className={`event-dot event-${event.level}`} /><span>{dateTime(event.created_at)}</span><strong>{event.message}</strong>{event.archive_path && <small>{event.archive_path}</small>}</div>) : <p>No detailed events were recorded for this run.</p>}</div>
  </div>;
}

function Activity({ notify }: { notify: (message: string, kind?: string) => void }) {
  const [rows, setRows] = useState<Json[]>([]);
  const [open, setOpen] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const load = useCallback(() => api("/api/jobs?limit=100").then((value) => setRows(value.rows)).catch((error) => notify(error.message, "error")).finally(() => setLoading(false)), [notify]);
  useEffect(() => { load(); const timer = window.setInterval(load, 3500); return () => window.clearInterval(timer); }, [load]);
  const act = async (row: Json) => {
    const active = ["queued", "running", "stopping"].includes(row.status);
    try { await api(`/api/jobs/${row.id}/${active ? "cancel" : "retry"}`, { method: "POST" }); notify(active ? "Stop requested" : "A fresh run is queued"); load(); }
    catch (error) { notify((error as Error).message, "error"); }
  };
  return <div className="page-stack">
    <PageIntro kicker="Activity" title="A history that reads like a story." detail="Open any run to see its timing, outcome, and important events in order." action={<button className="quiet-button" onClick={load}>Refresh</button>} />
    <section className="history-shell">{loading ? <div className="skeleton-list"><i /><i /><i /><i /></div> : !rows.length ? <Empty title="No activity yet" detail="Runs will appear here as soon as they are queued." /> : rows.map((row) => <article className={`history-item ${open === row.id ? "open" : ""}`} key={row.id}>
      <button className="history-summary" onClick={() => setOpen(open === row.id ? null : row.id)} aria-expanded={open === row.id}>
        <span className={`run-mark mark-${row.status}`} />
        <div><span>{dateTime(row.started_at || row.queued_at)}</span><strong>{row.source === "schedule" ? "Scheduled conversion" : row.source === "retry" ? "Archive recheck" : "Manual conversion"}</strong></div>
        <div className="history-progress"><span><i style={{ width: `${row.percent}%` }} /></span><small>{row.completed} of {row.archives_found || "—"}</small></div>
        <div className="history-impact"><span>Net impact</span><strong className={(row.net_gb || 0) < 0 ? "negative" : "positive"}>{formatGb(row.net_gb)}</strong></div>
        <Status value={row.status} /><b>{open === row.id ? "−" : "+"}</b>
      </button>
      {open === row.id && <div className="history-expanded"><RunDetail id={row.id} /><button onClick={() => act(row)} disabled={row.status === "stopping"}>{["queued", "running", "stopping"].includes(row.status) ? "Stop this run" : "Run these settings again"}</button></div>}
    </article>)}</section>
  </div>;
}

const initialSchedule = { name: "Library maintenance", enabled: true, cadence: "daily", interval_value: 6, interval_unit: "hours", weekday: 6, day_of_month: 1, hour: 4, minute: 0 };

function ScheduleForm({ bootstrap, existing, onClose, onSaved, notify }: { bootstrap: Json; existing?: Json; onClose: () => void; onSaved: () => void; notify: (message: string, kind?: string) => void }) {
  const [form, setForm] = useState<Json>({ ...initialSchedule, ...(existing || {}), library_id: existing?.library_id || bootstrap.libraries?.[0]?.id, profile_id: existing?.profile_id || bootstrap.profiles?.[0]?.id });
  const set = (key: string, value: any) => setForm((current: Json) => ({ ...current, [key]: value }));
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    try { await api(existing ? `/api/schedules/${existing.id}` : "/api/schedules", { method: existing ? "PUT" : "POST", body: JSON.stringify(form) }); notify(existing ? "Schedule updated" : "Schedule created"); onSaved(); onClose(); }
    catch (error) { notify((error as Error).message, "error"); }
  };
  return <Modal title={existing ? "Edit schedule" : "Create a schedule"} eyebrow="Automation" onClose={onClose}><form className="form" onSubmit={submit}>
    <div className="form-row"><label className="field grow"><span>Name</span><input value={form.name} onChange={(event) => set("name", event.target.value)} required /></label><label className="toggle"><input type="checkbox" checked={form.enabled} onChange={(event) => set("enabled", event.target.checked)} /><i /><span>Enabled</span></label></div>
    <fieldset><legend>When should it run?</legend><div className="schedule-sentence"><span>Every</span><select value={form.cadence} onChange={(event) => set("cadence", event.target.value)}><option value="daily">day</option><option value="weekly">week</option><option value="monthly">month</option><option value="interval">interval</option></select>{form.cadence === "weekly" && <><span>on</span><select value={form.weekday} onChange={(event) => set("weekday", Number(event.target.value))}>{["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"].map((day, index) => <option value={index} key={day}>{day}</option>)}</select></>}{form.cadence === "monthly" && <><span>on</span><select value={form.day_of_month} onChange={(event) => set("day_of_month", Number(event.target.value))}>{Array.from({ length: 31 }, (_, index) => <option value={index + 1} key={index}>day {index + 1}</option>)}<option value={-1}>the last day</option></select></>}{form.cadence === "interval" ? <><span>each</span><select value={form.interval_value} onChange={(event) => set("interval_value", Number(event.target.value))}>{Array.from({ length: 24 }, (_, i) => <option value={i + 1} key={i}>{i + 1}</option>)}</select><select value={form.interval_unit} onChange={(event) => set("interval_unit", event.target.value)}><option value="hours">hours</option><option value="days">days</option></select></> : <><span>at</span><select value={form.hour} onChange={(event) => set("hour", Number(event.target.value))}>{Array.from({ length: 24 }, (_, hour) => <option value={hour} key={hour}>{new Date(2000, 0, 1, hour).toLocaleTimeString([], { hour: "numeric" })}</option>)}</select><select value={form.minute} onChange={(event) => set("minute", Number(event.target.value))}>{[0, 15, 30, 45].map((minute) => <option value={minute} key={minute}>:{String(minute).padStart(2, "0")}</option>)}</select></>}</div></fieldset>
    <div className="form-grid"><label className="field"><span>Library</span><select value={form.library_id} onChange={(event) => set("library_id", event.target.value)}>{bootstrap.libraries?.map((row: Json) => <option key={row.id} value={row.id}>{row.name}</option>)}</select></label><label className="field"><span>Conversion profile</span><select value={form.profile_id} onChange={(event) => set("profile_id", event.target.value)}>{bootstrap.profiles?.map((row: Json) => <option key={row.id} value={row.id}>{row.name}</option>)}</select></label></div>
    <footer><button type="button" onClick={onClose}>Cancel</button><button className="primary-button" type="submit">Save schedule</button></footer>
  </form></Modal>;
}

function Automations({ bootstrap, reloadBootstrap, notify }: { bootstrap: Json; reloadBootstrap: () => void; notify: (message: string, kind?: string) => void }) {
  const [rows, setRows] = useState<Json[]>([]);
  const [editing, setEditing] = useState<Json | null | undefined>(undefined);
  const load = useCallback(() => api("/api/schedules").then((value) => setRows(value.rows)), []);
  useEffect(() => { load(); }, [load]);
  const remove = async (id: string) => {
    if (!window.confirm("Delete this schedule? Its run history will remain.")) return;
    try { await api(`/api/schedules/${id}`, { method: "DELETE" }); notify("Schedule deleted"); load(); reloadBootstrap(); }
    catch (error) { notify((error as Error).message, "error"); }
  };
  return <div className="page-stack">
    <PageIntro kicker="Automations" title="Set it once. Let it stay quiet." detail={`Schedules use ${bootstrap.timezone || "your configured timezone"} and are stored with the rest of your portable data.`} action={<button className="primary-button" onClick={() => setEditing(null)}>＋ New schedule</button>} />
    <section className="schedule-list">{rows.length ? rows.map((row) => <article className={!row.enabled ? "disabled" : ""} key={row.id}>
      <div className="schedule-when"><span>{row.enabled ? "Next" : "Paused"}</span><strong>{row.enabled ? relativeTime(row.next_run_at) : "—"}</strong></div>
      <div className="schedule-main"><span className="kicker">{row.library_name}</span><h2>{row.name}</h2><p>{row.description}</p></div>
      <div className="schedule-meta"><span>Profile</span><strong>{row.profile_name}</strong><small>{row.run_count} past runs</small></div>
      <Status value={row.enabled ? "enabled" : "disabled"} />
      <div className="schedule-actions"><button onClick={() => setEditing(row)}>Edit</button><button className="danger" onClick={() => remove(row.id)}>Delete</button></div>
    </article>) : <Empty title="No schedules" detail="Add a recurring conversion without writing a cron expression." action={<button className="quiet-button" onClick={() => setEditing(null)}>Create your first schedule</button>} />}</section>
    {editing !== undefined && <ScheduleForm bootstrap={bootstrap} existing={editing || undefined} onClose={() => setEditing(undefined)} onSaved={() => { load(); reloadBootstrap(); }} notify={notify} />}
  </div>;
}

function MigrationMapping({ report }: { report: Json }) {
  const schema = report.schema || {};
  return <div className="mapping-view">
    <div className="mapping-summary"><span className={report.integrity === "ok" ? "check" : "error"}>{report.integrity === "ok" ? "✓" : "!"}</span><div><strong>{report.integrity === "ok" ? "Database is healthy" : "Integrity check failed"}</strong><p>{report.mapping?.compatible_tables || 0} of {report.mapping?.available_tables || 0} detected tables can be translated.</p></div></div>
    <div className="mapping-list">{Object.entries(schema).map(([name, value]: [string, any]) => <article key={name} className={!value.present ? "missing" : ""}>
      <header><div><span>Old</span><strong>{name}</strong></div><b>→</b><div><span>New</span><strong>{value.target}</strong></div><i className={value.compatible ? "check" : "muted"}>{value.compatible ? "✓" : "—"}</i></header>
      {value.present ? <div className="column-summary"><span>{value.mapped_columns.length} fields mapped</span>{value.unmapped_columns.length ? <span>{value.unmapped_columns.length} preserved as unsupported</span> : <span>All recognized</span>}{value.missing_required.length ? <strong>Missing: {value.missing_required.join(", ")}</strong> : null}</div> : <p>Table not present; it will be skipped.</p>}
    </article>)}</div>
  </div>;
}

function DataMigration({ bootstrap, notify }: { bootstrap: Json; notify: (message: string, kind?: string) => void }) {
  const [path, setPath] = useState("/data/converted_archives.db");
  const [report, setReport] = useState<Json | null>(null);
  const [imports, setImports] = useState<Json[]>([]);
  const [busy, setBusy] = useState(false);
  const loadImports = useCallback(() => api("/api/database/imports").then((value) => setImports(value.rows)).catch(() => undefined), []);
  useEffect(() => { loadImports(); }, [loadImports]);
  const inspect = async () => {
    setBusy(true); setReport(null);
    try { const result = await api("/api/database/import", { method: "POST", body: JSON.stringify({ path, dry_run: true }) }); setReport(result); notify("Legacy schema read successfully"); }
    catch (error) { notify((error as Error).message, "error"); }
    finally { setBusy(false); }
  };
  const migrate = async () => {
    setBusy(true);
    try { const result = await api("/api/database/import", { method: "POST", body: JSON.stringify({ path, dry_run: false }) }); setReport(result); notify(result.already_imported ? "This source was already migrated" : "New database built and records imported"); loadImports(); }
    catch (error) { notify((error as Error).message, "error"); }
    finally { setBusy(false); }
  };
  const done = !!report?.imported || !!report?.already_imported;
  return <div className="page-stack">
    <PageIntro kicker="Data" title="Move forward without leaving history behind." detail="CBZXL reads the old SQLite file in place, explains the mapping, and builds the new schema separately." action={<a className="quiet-button link-button" href="/api/database/export">Export current database</a>} />
    <section className="migration-shell">
      <aside className="migration-steps"><div className="active"><b>1</b><span><strong>Choose source</strong><small>Point to the old database</small></span></div><div className={report ? "active" : ""}><b>2</b><span><strong>Review mapping</strong><small>See exactly what translates</small></span></div><div className={done ? "active" : ""}><b>3</b><span><strong>Build & import</strong><small>Create the new record set</small></span></div></aside>
      <div className="migration-workspace">
        <span className="kicker">Legacy migration</span><h2>{done ? "Migration complete" : report ? "Review before building" : "Where is the old database?"}</h2>
        {!report && <><p className="migration-lead">The source is opened read-only. CBZXL will not rename, edit, or delete it.</p><label className="path-input"><span>SQLite path inside the container</span><div><input value={path} onChange={(event) => setPath(event.target.value)} spellCheck={false} /><button className="primary-button" onClick={inspect} disabled={busy || !path.trim()}>{busy ? "Reading…" : "Read schema"}</button></div></label></>}
        {report && <><MigrationMapping report={report} /><div className="record-preview"><div><strong>{Number(report.tables?.converted_archives || 0).toLocaleString()}</strong><span>archives</span></div><div><strong>{Number(report.tables?.runs || 0).toLocaleString()}</strong><span>runs</span></div><div><strong>{Number(report.tables?.archive_results || 0).toLocaleString()}</strong><span>results</span></div></div><div className="migration-actions"><button onClick={() => { setReport(null); }} disabled={busy}>Choose another file</button>{!done && <button className="primary-button" onClick={migrate} disabled={busy || !report.mapping?.ready}>{busy ? "Building…" : "Build new database"}</button>}{done && <span className="migration-done"><i>✓</i>{report.already_imported ? "Already imported" : `${Object.values(report.imported || {}).reduce((sum: number, value: any) => sum + Number(value || 0), 0).toLocaleString()} records imported`}</span>}</div></>}
      </div>
    </section>
    <section className="data-foot"><div><span className="kicker">Current database</span><strong>{bootstrap.database_path}</strong><p>One portable file contains runs, archives, reviews, profiles, schedules, and events.</p></div>{imports.length > 0 && <div><span className="kicker">Previous migrations</span><strong>{imports.length}</strong><p>Most recent {relativeTime(imports[0].finished_at || imports[0].started_at)}</p></div>}</section>
  </div>;
}

function ScanForm({ bootstrap, onClose, onQueued, notify }: { bootstrap: Json; onClose: () => void; onQueued: () => void; notify: (message: string, kind?: string) => void }) {
  const profile = bootstrap.profiles?.find((row: Json) => row.is_default) || bootstrap.profiles?.[0];
  const [profileId, setProfileId] = useState(profile?.id || "");
  const [options, setOptions] = useState<Json>({ ...(profile?.options || {}) });
  const [advanced, setAdvanced] = useState(false);
  const set = (key: string, value: any) => setOptions((current: Json) => ({ ...current, [key]: value }));
  const chooseProfile = (id: string) => { setProfileId(id); const found = bootstrap.profiles?.find((row: Json) => row.id === id); setOptions({ ...(found?.options || {}) }); };
  const submit = async (event: FormEvent) => {
    event.preventDefault();
    try { await api("/api/jobs", { method: "POST", body: JSON.stringify({ profile_id: profileId, library_id: bootstrap.libraries?.[0]?.id, options }) }); notify("Conversion queued"); onQueued(); onClose(); }
    catch (error) { notify((error as Error).message, "error"); }
  };
  const mode = options.smart_distance ? "smart" : options.distance == null ? "lossless" : "lossy";
  return <Modal title="Start a conversion" eyebrow="New run" onClose={onClose} wide><form className="form scan-form" onSubmit={submit}>
    <div className="form-grid"><label className="field"><span>Library</span><select disabled><option>{bootstrap.libraries?.[0]?.name || "Primary library"}</option></select></label><label className="field"><span>Profile</span><select value={profileId} onChange={(event) => chooseProfile(event.target.value)}>{bootstrap.profiles?.map((row: Json) => <option key={row.id} value={row.id}>{row.name}</option>)}</select></label></div>
    <fieldset><legend>Choose the outcome</legend><div className="mode-picker">{[{ id: "lossless", name: "Lossless", copy: "Exact source quality" }, { id: "smart", name: "Smart", copy: "Lossless first; selective 0.5 fallback" }, { id: "lossy", name: "Smaller", copy: "Maximum space recovery" }].map((item) => <button type="button" className={mode === item.id ? "selected" : ""} key={item.id} onClick={() => setOptions((current: Json) => ({ ...current, smart_distance: item.id === "smart", distance: item.id === "lossless" ? null : item.id === "smart" ? .5 : 1 }))}><i>{mode === item.id ? "✓" : ""}</i><strong>{item.name}</strong><span>{item.copy}</span></button>)}</div></fieldset>
    <fieldset><legend>Safety</legend><div className="safety-options"><label><input type="checkbox" checked={!!options.dry_run} onChange={(event) => set("dry_run", event.target.checked)} /><span><strong>Preview only</strong><small>Make no archive changes</small></span></label><label><input type="checkbox" checked={!!options.backup} onChange={(event) => set("backup", event.target.checked)} /><span><strong>Keep originals</strong><small>Save a .bak beside each archive</small></span></label><label><input type="checkbox" checked={!!options.recheck_all} onChange={(event) => set("recheck_all", event.target.checked)} /><span><strong>Recheck all</strong><small>Ignore existing processed state</small></span></label></div></fieldset>
    <button type="button" className="advanced-button" onClick={() => setAdvanced(!advanced)}><span>{advanced ? "Hide" : "Show"} technical settings</span><b>{advanced ? "−" : "+"}</b></button>
    {advanced && <div className="advanced-settings"><label className="field"><span>Encoder effort · {options.effort ?? 7}</span><input type="range" min="0" max="11" value={options.effort ?? 7} onChange={(event) => set("effort", Number(event.target.value))} /></label><label className="field"><span>Total CPU budget</span><select value={options.threads ?? 4} onChange={(event) => set("threads", Number(event.target.value))}>{[1, 2, 4, 6, 8, 12, 16].map((value) => <option key={value}>{value}</option>)}</select></label>{mode === "smart" && <><label className="field"><span>Lossy fallback above MB</span><input type="number" min="0.1" step="0.1" value={options.smart_mb ?? 10} onChange={(event) => set("smart_mb", Number(event.target.value))} /></label><label className="field"><span>Lossy fallback above MP</span><input type="number" min="0.1" step="0.1" value={options.smart_mp ?? 5} onChange={(event) => set("smart_mp", Number(event.target.value))} /></label></>}</div>}
    <footer><p>Each archive is verified before replacement.</p><div><button type="button" onClick={onClose}>Cancel</button><button className="primary-button" type="submit">Queue conversion</button></div></footer>
  </form></Modal>;
}

export default function App() {
  const hashView = location.hash.slice(1) as View;
  const [view, setView] = useState<View>(["overview", "library", "activity", "automations", "data"].includes(hashView) ? hashView : "overview");
  const [bootstrap, setBootstrap] = useState<Json>({ libraries: [], profiles: [], schedules: [] });
  const [dashboard, setDashboard] = useState<Json>({});
  const [scanOpen, setScanOpen] = useState(false);
  const [toast, setToast] = useState<{ kind: string; message: string } | null>(null);
  const loadBootstrap = useCallback(() => api("/api/bootstrap").then(setBootstrap), []);
  const loadDashboard = useCallback(() => api("/api/dashboard").then(setDashboard), []);
  useEffect(() => { loadBootstrap(); loadDashboard(); const timer = window.setInterval(loadDashboard, 4000); return () => window.clearInterval(timer); }, [loadBootstrap, loadDashboard]);
  const notify = useCallback((message: string, kind = "success") => { setToast({ message, kind }); window.setTimeout(() => setToast(null), 3800); }, []);
  const navigate = (next: View) => { setView(next); location.hash = next; window.scrollTo({ top: 0, behavior: "smooth" }); };
  const activeCount = dashboard.active_jobs?.length || 0;
  const nav = useMemo(() => [{ id: "overview", label: "Overview" }, { id: "library", label: "Library" }, { id: "activity", label: "Activity", count: activeCount }, { id: "automations", label: "Automations" }, { id: "data", label: "Data" }] as { id: View; label: string; count?: number }[], [activeCount]);
  return <div className="app-shell">
    <header className="app-header"><button className="wordmark" onClick={() => navigate("overview")}><span>CX</span><strong>CBZXL</strong></button><nav aria-label="Primary navigation">{nav.map((item) => <button key={item.id} className={view === item.id ? "active" : ""} onClick={() => navigate(item.id)}>{item.label}{item.count ? <b>{item.count}</b> : null}</button>)}</nav><div className="header-actions"><span className={`worker-state ${activeCount ? "busy" : ""}`}><i />{activeCount ? "Working" : "Ready"}</span><button className="primary-button compact" onClick={() => setScanOpen(true)}>＋ <span>New conversion</span></button></div></header>
    <main className="app-content">
      {view === "overview" && <Overview data={dashboard} onScan={() => setScanOpen(true)} onNavigate={navigate} notify={notify} refresh={loadDashboard} />}
      {view === "library" && <Library notify={notify} />}
      {view === "activity" && <Activity notify={notify} />}
      {view === "automations" && <Automations bootstrap={bootstrap} reloadBootstrap={loadBootstrap} notify={notify} />}
      {view === "data" && <DataMigration bootstrap={bootstrap} notify={notify} />}
    </main>
    {scanOpen && <ScanForm bootstrap={bootstrap} onClose={() => setScanOpen(false)} onQueued={loadDashboard} notify={notify} />}
    <Toast value={toast} />
  </div>;
}
