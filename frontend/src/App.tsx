import { FormEvent, ReactNode, useCallback, useEffect, useMemo, useState } from "react";
import {
  ArcElement, BarElement, CategoryScale, Chart as ChartJS, Filler, Legend, LinearScale,
  LineElement, PointElement, Tooltip,
} from "chart.js";
import { Bar, Doughnut, Line } from "react-chartjs-2";

ChartJS.register(ArcElement, BarElement, CategoryScale, Filler, Legend, LinearScale, LineElement, PointElement, Tooltip);

type View = "dashboard" | "archives" | "runs" | "schedules" | "settings";
type Json = Record<string, any>;

const chartColors = ["#267a58", "#4d8fa8", "#d39a3c", "#be5d62", "#7b70ae", "#80a76a"];
const chartOptions = {
  responsive: true,
  maintainAspectRatio: false,
  plugins: { legend: { labels: { usePointStyle: true, boxWidth: 7, color: "#5f6d66", font: { family: "Inter, system-ui", size: 11 } } } },
  scales: {
    x: { grid: { display: false }, ticks: { color: "#77847d", maxRotation: 0 } },
    y: { grid: { color: "rgba(56, 76, 66, .08)" }, ticks: { color: "#77847d" } },
  },
};

async function api<T = Json>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(path, {
    ...options,
    headers: { "Content-Type": "application/json", ...(options?.headers || {}) },
  });
  if (!response.ok) {
    let message = `Request failed (${response.status})`;
    try { message = (await response.json()).detail || message; } catch { /* no JSON body */ }
    throw new Error(message);
  }
  return response.status === 204 ? ({} as T) : response.json();
}

function formatGb(value: number | null | undefined) {
  const number = Number(value || 0);
  return `${number.toLocaleString(undefined, { maximumFractionDigits: 2 })} GB`;
}

function formatBytes(value: number | null | undefined) {
  const bytes = Number(value || 0);
  const abs = Math.abs(bytes);
  if (abs >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(2)} GB`;
  if (abs >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  if (abs >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes.toLocaleString()} B`;
}

function dateTime(value?: string | null) {
  if (!value) return "—";
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

function statusLabel(value: string) {
  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function Status({ value }: { value: string }) {
  return <span className={`status status-${value}`}>{statusLabel(value)}</span>;
}

function Empty({ title, detail }: { title: string; detail: string }) {
  return <div className="empty"><span className="empty-mark">◎</span><strong>{title}</strong><p>{detail}</p></div>;
}

function Panel({ title, eyebrow, action, children, className = "" }: { title: string; eyebrow?: string; action?: ReactNode; children: ReactNode; className?: string }) {
  return <section className={`panel ${className}`}>
    <header className="panel-head"><div>{eyebrow && <span>{eyebrow}</span>}<h2>{title}</h2></div>{action}</header>
    <div className="panel-body">{children}</div>
  </section>;
}

function Modal({ title, onClose, children, wide = false }: { title: string; onClose: () => void; children: ReactNode; wide?: boolean }) {
  useEffect(() => {
    const listener = (event: KeyboardEvent) => event.key === "Escape" && onClose();
    window.addEventListener("keydown", listener);
    return () => window.removeEventListener("keydown", listener);
  }, [onClose]);
  return <div className="modal-backdrop" role="presentation" onMouseDown={(event) => event.target === event.currentTarget && onClose()}>
    <div className={`modal ${wide ? "modal-wide" : ""}`} role="dialog" aria-modal="true" aria-label={title}>
      <header><div><span>CBZXL</span><h2>{title}</h2></div><button className="icon-btn" onClick={onClose} aria-label="Close dialog">×</button></header>
      {children}
    </div>
  </div>;
}

function Toast({ toast }: { toast: { kind: string; message: string } | null }) {
  if (!toast) return null;
  return <div className={`toast toast-${toast.kind}`} role="status">{toast.message}</div>;
}

function Dashboard({ data, onNavigate, onScan, refresh }: { data: Json; onNavigate: (view: View) => void; onScan: () => void; refresh: () => void }) {
  const totals = data.totals || {};
  const active = data.active_jobs?.[0];
  const trend = data.run_trend || [];
  const trendData = {
    labels: trend.map((row: Json) => new Date(row.date).toLocaleDateString(undefined, { month: "short", day: "numeric" })),
    datasets: [
      { label: "Net saved", data: trend.map((row: Json) => row.net_gb), borderColor: "#267a58", backgroundColor: "rgba(38,122,88,.12)", fill: true, tension: .32, pointRadius: 3 },
      { label: "Increase", data: trend.map((row: Json) => -row.increased_gb), borderColor: "#be5d62", backgroundColor: "transparent", tension: .32, pointRadius: 2 },
    ],
  };
  const outcomeData = {
    labels: (data.outcomes || []).map((row: Json) => statusLabel(row.status)),
    datasets: [{ data: (data.outcomes || []).map((row: Json) => row.count), backgroundColor: chartColors, borderWidth: 0, hoverOffset: 5 }],
  };
  const formatData = {
    labels: (data.formats || []).map((row: Json) => row.label),
    datasets: [{ label: "Archives", data: (data.formats || []).map((row: Json) => row.count), backgroundColor: "#4d8fa8", borderRadius: 5 }],
  };
  return <div className="page-stack">
    <section className="welcome">
      <div><span className="eyebrow">Library overview</span><h1>Your archive operation, at a glance.</h1><p>Monitor storage savings, review exceptions, and keep every conversion traceable.</p></div>
      <button className="primary large" onClick={onScan}>Start a scan</button>
    </section>
    <section className="metrics">
      <article className="metric metric-primary"><span>Net space saved</span><strong>{formatGb(totals.net_gb)}</strong><small>{formatGb(totals.saved_gb)} gross reduction</small></article>
      <article className="metric"><span>Indexed archives</span><strong>{Number(totals.archive_count || 0).toLocaleString()}</strong><small>Across enabled libraries</small></article>
      <article className="metric"><span>Needs review</span><strong>{Number((data.review?.failed || 0) + (data.review?.grew || 0)).toLocaleString()}</strong><small>{data.review?.failed || 0} failed · {data.review?.grew || 0} grew</small></article>
      <article className="metric"><span>Storage increased</span><strong>{formatGb(totals.increased_gb)}</strong><small>Opportunity for tuning</small></article>
    </section>
    {active ? <Panel title="Scan in progress" eyebrow={active.source === "schedule" ? "Scheduled activity" : "Live activity"} action={<Status value={active.status} />} className="active-run">
      <div className="run-overview"><div><strong>{active.current_archive || "Discovering archives…"}</strong><span>{active.completed.toLocaleString()} of {active.archives_found.toLocaleString()} complete</span></div><b>{active.percent}%</b></div>
      <div className="progress"><span style={{ width: `${active.percent}%` }} /></div>
      <div className="run-facts"><span><b>{active.processed}</b> processed</span><span><b>{active.skipped}</b> skipped</span><span><b>{active.failed}</b> failed</span><span><b>{formatGb(active.net_gb)}</b> net</span></div>
    </Panel> : <div className="quiet-banner"><span className="pulse-dot" /><div><strong>The worker is ready</strong><p>No scan is currently running. Scheduled work will begin automatically.</p></div><button className="secondary" onClick={refresh}>Check now</button></div>}
    <section className="chart-grid">
      <Panel title="Storage impact" eyebrow="Last 20 completed scans"><div className="chart-tall">{trend.length ? <Line data={trendData} options={chartOptions} role="img" aria-label="Net storage saved and increased by scan" /> : <Empty title="No trend yet" detail="Completed scans will build a storage trend here." />}</div></Panel>
      <Panel title="Library outcomes" eyebrow="Current archive state"><div className="chart-donut">{data.outcomes?.length ? <Doughnut data={outcomeData} options={{ responsive: true, maintainAspectRatio: false, cutout: "72%", plugins: chartOptions.plugins }} role="img" aria-label="Archive outcomes by status" /> : <Empty title="No archive data" detail="Run or import a scan to see the outcome mix." />}</div></Panel>
      <Panel title="Source formats" eyebrow="What entered the converter"><div className="chart-tall">{data.formats?.length ? <Bar data={formatData} options={{ ...chartOptions, indexAxis: "y" as const }} role="img" aria-label="Archive counts by dominant source format" /> : <Empty title="No format data" detail="Format totals appear after archives are indexed." />}</div></Panel>
    </section>
    <Panel title="Recent scans" eyebrow="Durable run history" action={<button className="text-btn" onClick={() => onNavigate("runs")}>View all</button>}>
      <RunTable rows={data.recent_jobs || []} compact />
    </Panel>
  </div>;
}

function RunTable({ rows, compact = false, onCancel, onRetry }: { rows: Json[]; compact?: boolean; onCancel?: (id: string) => void; onRetry?: (id: string) => void }) {
  if (!rows.length) return <Empty title="No scans yet" detail="Start a manual scan or add a schedule to create run history." />;
  return <div className="table-scroll"><table><thead><tr><th>Started</th><th>Source</th><th>Status</th><th>Progress</th><th>Net impact</th>{!compact && <th />}</tr></thead><tbody>{rows.map((row) => <tr key={row.id}>
    <td><strong>{dateTime(row.started_at || row.queued_at)}</strong><small className="cell-sub">{row.id.slice(0, 8)}</small></td>
    <td className="capitalize">{row.source}</td><td><Status value={row.status} /></td>
    <td><strong>{row.completed} / {row.archives_found || "—"}</strong><small className="cell-sub">{row.failed ? `${row.failed} failed` : `${row.processed} processed`}</small></td>
    <td className={row.net_gb < 0 ? "negative" : "positive"}>{formatGb(row.net_gb)}</td>
    {!compact && <td className="row-actions">{["queued", "running", "stopping"].includes(row.status) ? <button onClick={() => onCancel?.(row.id)} disabled={row.status === "stopping"}>Stop</button> : <button onClick={() => onRetry?.(row.id)}>Run again</button>}</td>}
  </tr>)}</tbody></table></div>;
}

function Runs({ notify }: { notify: (message: string, kind?: string) => void }) {
  const [rows, setRows] = useState<Json[]>([]);
  const [loading, setLoading] = useState(true);
  const load = useCallback(() => api("/api/jobs?limit=100").then((data) => setRows(data.rows)).finally(() => setLoading(false)), []);
  useEffect(() => { load(); const timer = window.setInterval(load, 3000); return () => clearInterval(timer); }, [load]);
  const cancel = async (id: string) => { try { await api(`/api/jobs/${id}/cancel`, { method: "POST" }); notify("Cancellation requested"); load(); } catch (error) { notify((error as Error).message, "error"); } };
  const retry = async (id: string) => { try { await api(`/api/jobs/${id}/retry`, { method: "POST" }); notify("A fresh scan has been queued"); load(); } catch (error) { notify((error as Error).message, "error"); } };
  return <div className="page-stack"><PageTitle eyebrow="Operations" title="Scan history" detail="Every manual and scheduled run remains searchable and auditable." />
    <Panel title="All scans" eyebrow={`${rows.length} most recent`} action={<button className="secondary" onClick={load}>Refresh</button>}>{loading ? <div className="loading-lines" /> : <RunTable rows={rows} onCancel={cancel} onRetry={retry} />}</Panel>
  </div>;
}

function Archives({ notify }: { notify: (message: string, kind?: string) => void }) {
  const [data, setData] = useState<Json>({ rows: [], total: 0, pages: 1 });
  const [search, setSearch] = useState("");
  const [status, setStatus] = useState("all");
  const [review, setReview] = useState("all");
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const load = useCallback(() => {
    setLoading(true);
    const query = new URLSearchParams({ search, status, review, page: String(page), page_size: "50" });
    api(`/api/archives?${query}`).then(setData).finally(() => setLoading(false));
  }, [search, status, review, page]);
  useEffect(() => { const timer = window.setTimeout(load, 180); return () => clearTimeout(timer); }, [load]);
  const act = async (id: number, action: string) => { try { await api(`/api/archives/${id}/review`, { method: "POST", body: JSON.stringify({ action, note: "" }) }); notify(action === "queue_recheck" ? "Recheck queued" : "Review updated"); load(); } catch (error) { notify((error as Error).message, "error"); } };
  return <div className="page-stack"><PageTitle eyebrow="Library" title="Archive inventory" detail="Search current state and preserve a complete processing history." />
    <section className="filter-bar"><label className="search"><span>Search</span><input value={search} onChange={(event) => { setSearch(event.target.value); setPage(1); }} placeholder="Title or folder path…" /></label>
      <label><span>Status</span><select value={status} onChange={(event) => { setStatus(event.target.value); setPage(1); }}><option value="all">All statuses</option><option value="processed">Processed</option><option value="failed">Failed</option></select></label>
      <label><span>Review</span><select value={review} onChange={(event) => { setReview(event.target.value); setPage(1); }}><option value="all">Everything</option><option value="needs_review">Needs review</option><option value="reviewed">Reviewed</option></select></label>
      <div className="filter-count"><strong>{Number(data.total || 0).toLocaleString()}</strong><span>archives</span></div>
    </section>
    <Panel title="Library records" eyebrow="Latest state per archive">{loading ? <div className="loading-lines" /> : !data.rows.length ? <Empty title="Nothing matches" detail="Try a broader search or run a library scan." /> : <div className="table-scroll"><table><thead><tr><th>Archive</th><th>Status</th><th>Format</th><th>Change</th><th>Updated</th><th /></tr></thead><tbody>{data.rows.map((row: Json) => <tr key={row.id}><td className="path-cell"><strong title={row.path}>{row.path.split("/").pop()}</strong><small title={row.path}>{row.path}</small></td><td><Status value={row.status} /></td><td>{row.dominant_type || "—"}<small className="cell-sub">{row.image_count ? `${row.image_count} images` : ""}</small></td><td className={(row.bytes_saved || 0) < 0 ? "negative" : "positive"}><strong>{formatBytes(row.bytes_saved)}</strong><small className="cell-sub">{Number(row.percent_saved || 0).toFixed(1)}%</small></td><td>{dateTime(row.updated_at)}</td><td className="row-actions">{row.review_status !== "reviewed" && (row.status === "failed" || row.bytes_saved < 0) ? <button onClick={() => act(row.id, "reviewed")}>Resolve</button> : null}<button onClick={() => act(row.id, "queue_recheck")}>Recheck</button></td></tr>)}</tbody></table></div>}
      <footer className="pagination"><button disabled={page <= 1} onClick={() => setPage((value) => value - 1)}>Previous</button><span>Page {page} of {data.pages}</span><button disabled={page >= data.pages} onClick={() => setPage((value) => value + 1)}>Next</button></footer>
    </Panel>
  </div>;
}

const initialSchedule = { name: "Library maintenance", enabled: true, cadence: "daily", interval_value: 6, interval_unit: "hours", weekday: 6, day_of_month: 1, hour: 4, minute: 0 };

function ScheduleForm({ bootstrap, existing, onClose, onSaved, notify }: { bootstrap: Json; existing?: Json; onClose: () => void; onSaved: () => void; notify: (message: string, kind?: string) => void }) {
  const [form, setForm] = useState<Json>({ ...initialSchedule, ...(existing || {}), library_id: existing?.library_id || bootstrap.libraries?.[0]?.id, profile_id: existing?.profile_id || bootstrap.profiles?.[0]?.id });
  const set = (key: string, value: any) => setForm((current: Json) => ({ ...current, [key]: value }));
  const submit = async (event: FormEvent) => { event.preventDefault(); try { await api(existing ? `/api/schedules/${existing.id}` : "/api/schedules", { method: existing ? "PUT" : "POST", body: JSON.stringify(form) }); notify(existing ? "Schedule updated" : "Schedule saved"); onSaved(); onClose(); } catch (error) { notify((error as Error).message, "error"); } };
  return <Modal title={existing ? "Edit schedule" : "Create a schedule"} onClose={onClose}><form onSubmit={submit} className="modal-form">
    <div className="form-section"><label className="field field-wide"><span>Schedule name</span><input value={form.name} onChange={(event) => set("name", event.target.value)} required /></label>
      <label className="switch"><input type="checkbox" checked={form.enabled} onChange={(event) => set("enabled", event.target.checked)} /><span /><b>Enabled</b></label></div>
    <div className="form-section"><span className="section-label">When should it run?</span><div className="sentence-builder"><span>Every</span><select value={form.cadence} onChange={(event) => set("cadence", event.target.value)}><option value="daily">day</option><option value="weekly">week</option><option value="monthly">month</option><option value="interval">interval</option></select>
      {form.cadence === "weekly" && <><span>on</span><select value={form.weekday} onChange={(event) => set("weekday", Number(event.target.value))}>{["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"].map((day, index) => <option value={index} key={day}>{day}</option>)}</select></>}
      {form.cadence === "monthly" && <><span>on</span><select value={form.day_of_month} onChange={(event) => set("day_of_month", Number(event.target.value))}>{Array.from({ length: 31 }, (_, index) => <option value={index + 1} key={index}>day {index + 1}</option>)}<option value={-1}>the last day</option></select></>}
      {form.cadence === "interval" ? <><span>each</span><select value={form.interval_value} onChange={(event) => set("interval_value", Number(event.target.value))}>{Array.from({ length: 24 }, (_, index) => <option value={index + 1} key={index}>{index + 1}</option>)}</select><select value={form.interval_unit} onChange={(event) => set("interval_unit", event.target.value)}><option value="hours">hours</option><option value="days">days</option></select></> : <><span>at</span><select value={form.hour} onChange={(event) => set("hour", Number(event.target.value))}>{Array.from({ length: 24 }, (_, hour) => <option value={hour} key={hour}>{new Date(2000, 0, 1, hour).toLocaleTimeString([], { hour: "numeric" })}</option>)}</select><select value={form.minute} onChange={(event) => set("minute", Number(event.target.value))}>{[0, 15, 30, 45].map((minute) => <option value={minute} key={minute}>:{String(minute).padStart(2, "0")}</option>)}</select></>}</div></div>
    <div className="form-grid"><label className="field"><span>Library</span><select value={form.library_id} onChange={(event) => set("library_id", event.target.value)}>{bootstrap.libraries?.map((row: Json) => <option key={row.id} value={row.id}>{row.name}</option>)}</select></label><label className="field"><span>Conversion profile</span><select value={form.profile_id} onChange={(event) => set("profile_id", event.target.value)}>{bootstrap.profiles?.map((row: Json) => <option key={row.id} value={row.id}>{row.name}</option>)}</select></label></div>
    <footer><button type="button" onClick={onClose}>Cancel</button><button className="primary" type="submit">Save schedule</button></footer>
  </form></Modal>;
}

function Schedules({ bootstrap, reloadBootstrap, notify }: { bootstrap: Json; reloadBootstrap: () => void; notify: (message: string, kind?: string) => void }) {
  const [rows, setRows] = useState<Json[]>([]); const [editing, setEditing] = useState<Json | null | undefined>(undefined);
  const load = useCallback(() => api("/api/schedules").then((data) => setRows(data.rows)), []);
  useEffect(() => { load(); }, [load]);
  const remove = async (id: string) => { if (!window.confirm("Delete this schedule? Its past scan history will remain.")) return; try { await api(`/api/schedules/${id}`, { method: "DELETE" }); notify("Schedule deleted"); load(); reloadBootstrap(); } catch (error) { notify((error as Error).message, "error"); } };
  return <div className="page-stack"><PageTitle eyebrow="Automation" title="Schedules" detail="Set a familiar recurring rhythm. CBZXL stores every schedule and every run in the portable database." action={<button className="primary" onClick={() => setEditing(null)}>New schedule</button>} />
    <div className="info-strip"><span>i</span><p><strong>No cron expressions required.</strong> Choose daily, weekly, monthly, or a repeating interval. Times use {bootstrap.timezone || "the configured timezone"}.</p></div>
    <section className="schedule-grid">{rows.length ? rows.map((row) => <article className={`schedule-card ${!row.enabled ? "disabled" : ""}`} key={row.id}><header><div><span className="schedule-icon">↻</span><div><h2>{row.name}</h2><Status value={row.enabled ? "enabled" : "disabled"} /></div></div><button className="more" onClick={() => setEditing(row)}>Edit</button></header><div className="schedule-rule"><span>Runs</span><strong>{row.description}</strong></div><dl><div><dt>Next run</dt><dd>{row.enabled ? relativeTime(row.next_run_at) : "Paused"}</dd></div><div><dt>Last queued</dt><dd>{relativeTime(row.last_enqueued_at)}</dd></div><div><dt>Profile</dt><dd>{row.profile_name}</dd></div><div><dt>Activity</dt><dd>{row.run_count} runs</dd></div></dl><footer><span>{row.library_name}</span><button className="danger-link" onClick={() => remove(row.id)}>Delete</button></footer></article>) : <button className="add-schedule" onClick={() => setEditing(null)}><span>＋</span><strong>Add your first schedule</strong><small>Choose when CBZXL should maintain the library.</small></button>}</section>
    {editing !== undefined && <ScheduleForm bootstrap={bootstrap} existing={editing || undefined} onClose={() => setEditing(undefined)} onSaved={() => { load(); reloadBootstrap(); }} notify={notify} />}
  </div>;
}

function ScanForm({ bootstrap, onClose, onQueued, notify }: { bootstrap: Json; onClose: () => void; onQueued: () => void; notify: (message: string, kind?: string) => void }) {
  const profile = bootstrap.profiles?.find((row: Json) => row.is_default) || bootstrap.profiles?.[0];
  const [profileId, setProfileId] = useState(profile?.id || "");
  const [options, setOptions] = useState<Json>({ ...(profile?.options || {}) });
  const [advanced, setAdvanced] = useState(false);
  const selectProfile = (id: string) => { setProfileId(id); const found = bootstrap.profiles?.find((row: Json) => row.id === id); setOptions({ ...(found?.options || {}) }); };
  const set = (key: string, value: any) => setOptions((current: Json) => ({ ...current, [key]: value }));
  const submit = async (event: FormEvent) => { event.preventDefault(); try { await api("/api/jobs", { method: "POST", body: JSON.stringify({ profile_id: profileId, library_id: bootstrap.libraries?.[0]?.id, options }) }); notify("Scan queued"); onQueued(); onClose(); } catch (error) { notify((error as Error).message, "error"); } };
  return <Modal title="Start a library scan" onClose={onClose} wide><form className="modal-form" onSubmit={submit}><div className="form-grid"><label className="field"><span>Library</span><select disabled><option>{bootstrap.libraries?.[0]?.name || "Primary library"}</option></select></label><label className="field"><span>Conversion profile</span><select value={profileId} onChange={(event) => selectProfile(event.target.value)}>{bootstrap.profiles?.map((row: Json) => <option key={row.id} value={row.id}>{row.name}</option>)}</select></label></div>
    <div className="choice-grid"><label className={`choice ${!options.smart_distance && options.distance == null ? "selected" : ""}`}><input type="radio" name="mode" checked={!options.smart_distance && options.distance == null} onChange={() => setOptions((current: Json) => ({ ...current, smart_distance: false, distance: null }))} /><strong>Lossless</strong><small>Preserve source quality exactly</small></label><label className={`choice ${options.smart_distance ? "selected" : ""}`}><input type="radio" name="mode" checked={!!options.smart_distance} onChange={() => setOptions((current: Json) => ({ ...current, smart_distance: true, distance: .1 }))} /><strong>Smart lossy</strong><small>Target only unusually large images</small></label><label className={`choice ${!options.smart_distance && options.distance != null ? "selected" : ""}`}><input type="radio" name="mode" checked={!options.smart_distance && options.distance != null} onChange={() => setOptions((current: Json) => ({ ...current, smart_distance: false, distance: 1 }))} /><strong>Lossy</strong><small>Maximum storage reduction</small></label></div>
    <div className="safety-row"><label><input type="checkbox" checked={!!options.dry_run} onChange={(event) => set("dry_run", event.target.checked)} /><span><strong>Dry run</strong><small>Preview without changing archives</small></span></label><label><input type="checkbox" checked={!!options.backup} onChange={(event) => set("backup", event.target.checked)} /><span><strong>Backup originals</strong><small>Keep a .bak copy beside each archive</small></span></label><label><input type="checkbox" checked={!!options.recheck_all} onChange={(event) => set("recheck_all", event.target.checked)} /><span><strong>Recheck everything</strong><small>Ignore the current processed state</small></span></label></div>
    <button type="button" className="advanced-toggle" onClick={() => setAdvanced((value) => !value)}>{advanced ? "Hide" : "Show"} advanced settings <span>{advanced ? "−" : "+"}</span></button>
    {advanced && <div className="advanced-grid"><label className="field"><span>Encoder effort · {options.effort}</span><input type="range" min="0" max="11" value={options.effort ?? 7} onChange={(event) => set("effort", Number(event.target.value))} /></label><label className="field"><span>Image workers</span><select value={options.threads ?? 4} onChange={(event) => set("threads", Number(event.target.value))}>{[1, 2, 4, 6, 8, 12, 16].map((value) => <option key={value}>{value}</option>)}</select></label><label className="checkline"><input type="checkbox" checked={!options.no_flatten} onChange={(event) => set("no_flatten", !event.target.checked)} /> Flatten nested folders</label><label className="checkline"><input type="checkbox" checked={!!options.delete_empty} onChange={(event) => set("delete_empty", event.target.checked)} /> Delete archives with no images</label></div>}
    <footer><button type="button" onClick={onClose}>Cancel</button><div><span className="safe-copy">Archives are replaced atomically after verification.</span><button className="primary" type="submit">Queue scan</button></div></footer></form></Modal>;
}

function Settings({ bootstrap, notify }: { bootstrap: Json; notify: (message: string, kind?: string) => void }) {
  const [path, setPath] = useState("/data/converted_archives.db"); const [report, setReport] = useState<Json | null>(null); const [busy, setBusy] = useState(false);
  const inspect = async () => { setBusy(true); try { const result = await api("/api/database/import", { method: "POST", body: JSON.stringify({ path, dry_run: true }) }); setReport(result); notify("Legacy database inspected"); } catch (error) { notify((error as Error).message, "error"); } finally { setBusy(false); } };
  const migrate = async () => { setBusy(true); try { const result = await api("/api/database/import", { method: "POST", body: JSON.stringify({ path, dry_run: false }) }); setReport(result); notify(result.already_imported ? "That database was already imported" : "Legacy data imported"); } catch (error) { notify((error as Error).message, "error"); } finally { setBusy(false); } };
  return <div className="page-stack"><PageTitle eyebrow="System" title="Data & portability" detail="The entire application state—schedules included—lives in one portable SQLite database." />
    <section className="settings-grid"><Panel title="Portable database" eyebrow="Current storage"><div className="database-card"><span className="db-mark">DB</span><div><strong>{bootstrap.database_path}</strong><p>Archives, scan history, reviews, profiles, schedules, and activity.</p></div></div><a className="button primary" href="/api/database/export">Export a safe copy</a><p className="fine-print">Export creates a consistent single-file snapshot even while CBZXL is running.</p></Panel>
      <Panel title="Import existing data" eyebrow="Legacy SQLite migration"><div className="import-box"><label className="field"><span>Legacy database path inside the container</span><input value={path} onChange={(event) => setPath(event.target.value)} /></label><div><button onClick={inspect} disabled={busy}>Inspect first</button>{report && <button className="primary" onClick={migrate} disabled={busy || report.integrity !== "ok"}>Import records</button>}</div></div>{report && <div className="migration-report"><strong>Ready to import</strong><span>Integrity: {report.integrity}</span><span>{report.tables?.converted_archives || 0} archives</span><span>{report.tables?.runs || 0} runs</span><span>{report.tables?.archive_results || 0} results</span></div>}</Panel>
    </section>
    <Panel title="Configured library" eyebrow="Filesystem source"><div className="library-row"><div><strong>{bootstrap.libraries?.[0]?.name}</strong><span>{bootstrap.libraries?.[0]?.root_path}</span></div><Status value={bootstrap.libraries?.[0]?.enabled ? "enabled" : "disabled"} /></div></Panel>
  </div>;
}

function PageTitle({ eyebrow, title, detail, action }: { eyebrow: string; title: string; detail: string; action?: ReactNode }) {
  return <section className="page-title"><div><span className="eyebrow">{eyebrow}</span><h1>{title}</h1><p>{detail}</p></div>{action}</section>;
}

export default function App() {
  const initial = (location.hash.slice(1) as View) || "dashboard";
  const [view, setView] = useState<View>(["dashboard", "archives", "runs", "schedules", "settings"].includes(initial) ? initial : "dashboard");
  const [bootstrap, setBootstrap] = useState<Json>({ libraries: [], profiles: [], schedules: [] });
  const [dashboard, setDashboard] = useState<Json>({});
  const [scanOpen, setScanOpen] = useState(false);
  const [toast, setToast] = useState<{ kind: string; message: string } | null>(null);
  const loadBootstrap = useCallback(() => api("/api/bootstrap").then(setBootstrap), []);
  const loadDashboard = useCallback(() => api("/api/dashboard").then(setDashboard), []);
  useEffect(() => { loadBootstrap(); loadDashboard(); const timer = window.setInterval(loadDashboard, 4000); return () => clearInterval(timer); }, [loadBootstrap, loadDashboard]);
  const notify = useCallback((message: string, kind = "success") => { setToast({ message, kind }); window.setTimeout(() => setToast(null), 3800); }, []);
  const navigate = (next: View) => { setView(next); location.hash = next; window.scrollTo({ top: 0, behavior: "smooth" }); };
  const activeCount = dashboard.active_jobs?.length || 0;
  const nav = [{ id: "dashboard", label: "Overview" }, { id: "archives", label: "Library" }, { id: "runs", label: "Runs", count: activeCount }, { id: "schedules", label: "Schedules" }, { id: "settings", label: "Data & settings" }] as { id: View; label: string; count?: number }[];
  return <div className="app-shell">
    <aside className="sidebar"><div className="brand"><span>CX</span><div><strong>CBZXL</strong><small>Archive operations</small></div></div><nav>{nav.map((item) => <button className={view === item.id ? "active" : ""} key={item.id} onClick={() => navigate(item.id)}><i />{item.label}{item.count ? <b>{item.count}</b> : null}</button>)}</nav><div className="sidebar-foot"><span className={activeCount ? "worker-dot running" : "worker-dot"} /><div><strong>{activeCount ? "Worker active" : "Worker ready"}</strong><small>{bootstrap.timezone || "Loading timezone…"}</small></div></div></aside>
    <main><header className="topbar"><div className="mobile-brand"><strong>CBZXL</strong><span>{nav.find((item) => item.id === view)?.label}</span></div><div className="top-status"><span className={activeCount ? "pulse-dot active" : "pulse-dot"} />{activeCount ? `${activeCount} active scan${activeCount === 1 ? "" : "s"}` : "All systems ready"}</div><button className="primary" onClick={() => setScanOpen(true)}>＋ New scan</button></header><div className="content">
      {view === "dashboard" && <Dashboard data={dashboard} onNavigate={navigate} onScan={() => setScanOpen(true)} refresh={loadDashboard} />}
      {view === "archives" && <Archives notify={notify} />}
      {view === "runs" && <Runs notify={notify} />}
      {view === "schedules" && <Schedules bootstrap={bootstrap} reloadBootstrap={loadBootstrap} notify={notify} />}
      {view === "settings" && <Settings bootstrap={bootstrap} notify={notify} />}
    </div></main>
    {scanOpen && <ScanForm bootstrap={bootstrap} onClose={() => setScanOpen(false)} onQueued={loadDashboard} notify={notify} />}
    <Toast toast={toast} />
  </div>;
}
