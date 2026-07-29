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

function compactNumber(value?: number | null) {
  const number = Number(value || 0);
  return new Intl.NumberFormat(undefined, {
    notation: number >= 100_000 ? "compact" : "standard",
    maximumFractionDigits: 2,
  }).format(number);
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

function LiveConversion({ initial, refresh, notify, onDismiss }: {
  initial: Json;
  refresh: () => void;
  notify: (message: string, kind?: "success" | "error") => void;
  onDismiss?: (job: Json) => void;
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
  const terminal = !["queued", "running", "stopping"].includes(job.status);
  const failed = ["failed", "interrupted"].includes(job.status);
  const terminalTitle = failed
    ? "Conversion stopped unexpectedly"
    : job.status === "partial"
      ? "Conversion completed with exceptions"
      : job.status === "cancelled"
        ? "Conversion stopped"
        : "Conversion complete";
  const statusText = terminal
    ? label(job.status)
    : job.status === "queued"
      ? "Waiting for worker"
      : label(job.current_stage || "Converting");
  const stop = async () => {
    try {
      await api(`/api/jobs/${job.id}/cancel`, { method: "POST" });
      notify("The worker is stopping safely");
      refresh();
    } catch (error) {
      notify((error as Error).message, "error");
    }
  };
  const retry = async () => {
    try {
      await api(`/api/jobs/${job.id}/retry`, { method: "POST" });
      notify("Conversion retry queued");
      refresh();
    } catch (error) {
      notify((error as Error).message, "error");
    }
  };
  return <section className={`run-card ${terminal ? "terminal" : ""} ${failed ? "failed" : ""}`}>
    <div className="run-top">
      <div className="run-copy">
        <span className={`status-pill ${terminal ? "terminal" : ""}`}><i />{statusText}</span>
        <h2>{terminal ? terminalTitle : job.current_archive ? archiveName(job.current_archive) : "Discovering your library"}</h2>
        <code>{terminal ? job.error_message || `${job.completed || 0} of ${job.archives_found || 0} archives completed` : job.current_archive || "Preparing the conversion queue…"}</code>
      </div>
      <div className="run-actions">
        {terminal && failed && <button className="button primary" onClick={retry}>Retry remaining</button>}
        {terminal ? <button className="button secondary" onClick={() => onDismiss?.(job)}>Dismiss</button> :
          <button className="button secondary" onClick={stop} disabled={job.status === "stopping"}>
            {job.status === "stopping" ? "Stopping…" : "Stop"}
          </button>}
      </div>
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

function policyStrategy(options: Json = {}) {
  if (options.integrity_audit) return "audit";
  if (options.no_convert) return "recheck";
  if (options.smart_distance) return "smart";
  if (options.distance !== null && options.distance !== undefined) return "lossy";
  return "lossless";
}

function policyScope(options: Json = {}) {
  if (options.failed_only) return "failed";
  if (options.recheck_all) return "library";
  return "unprocessed";
}

function policySummary(profile: Json) {
  const options = profile.options || {};
  const strategy = policyStrategy(options);
  const effort = Number(options.effort ?? 10);
  const threads = Number(options.threads ?? 4);
  if (strategy === "audit") return `Full CRC check + full JPEG XL decode · ${threads} threads`;
  if (strategy === "recheck") return `Recheck only · ${threads} threads`;
  if (strategy === "smart") {
    return `Lossless first · d${Number(options.distance ?? 0.5)} fallback above ${Number(options.smart_mb ?? 10)} MB or ${Number(options.smart_mp ?? 5)} MP · effort ${effort}`;
  }
  if (strategy === "lossy") return `Lossy d${Number(options.distance)} · effort ${effort} · ${threads} threads`;
  return `Lossless · effort ${effort} · ${threads} threads`;
}

function ManualConversion({ bootstrap, onQueued, notify }: {
  bootstrap: Json;
  onQueued: () => void;
  notify: (message: string, kind?: "success" | "error") => void;
}) {
  const profiles = bootstrap.profiles || [];
  const defaultProfile = profiles.find((row: Json) => row.is_default) || profiles[0] || {};
  const [open, setOpen] = useState(false);
  const [scope, setScope] = useState<"library" | "unprocessed" | "failed">("unprocessed");
  const [profileId, setProfileId] = useState(defaultProfile.id || "");
  const profile = profiles.find((row: Json) => row.id === profileId) || defaultProfile;
  const defaults = profile.options || {};
  const [effort, setEffort] = useState(Number(defaults.effort ?? 10));
  const [threads, setThreads] = useState(Number(defaults.threads ?? 4));
  const [backup, setBackup] = useState(Boolean(defaults.backup));
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!profiles.some((row: Json) => row.id === profileId) && defaultProfile.id) {
      setProfileId(defaultProfile.id);
    }
  }, [defaultProfile.id, profileId, profiles]);

  const chooseProfile = (nextProfile: Json) => {
    const options = nextProfile.options || {};
    setProfileId(nextProfile.id);
    setEffort(Number(options.effort ?? 10));
    setThreads(Number(options.threads ?? 4));
    setBackup(Boolean(options.backup));
  };

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    setBusy(true);
    const options: Json = {
      ...defaults,
      effort,
      threads,
      backup,
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
            <button type="button" className={scope === "unprocessed" ? "selected" : ""} onClick={() => setScope("unprocessed")}>Unprocessed</button>
            <button type="button" className={scope === "failed" ? "selected" : ""} onClick={() => setScope("failed")}>△ Failed</button>
          </div>
          <div className="choice-nav" role="group" aria-label="Conversion policy">
            {profiles.map((row: Json) => <button
              type="button"
              key={row.id}
              className={profile.id === row.id ? "selected" : ""}
              onClick={() => chooseProfile(row)}
            >{policyStrategy(row.options) === "smart" ? "✦" : policyStrategy(row.options) === "audit" ? "◎" : policyStrategy(row.options) === "recheck" ? "⌕" : "✓"} {row.name}</button>)}
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

function DistributionChart({ rows, tone = "blue", label }: {
  rows: Json[];
  tone?: "blue" | "violet";
  label: string;
}) {
  const maximum = Math.max(1, ...rows.map((row) => Number(row.count || 0)));
  return <div className="distribution-chart" role="img" aria-label={label}>
    {rows.map((row: Json) => <div className="distribution-column" key={row.label}>
      <span className={`distribution-bar ${tone}`} style={{ height: `${Math.max(2, Number(row.count || 0) / maximum * 100)}%` }} />
      <span>{row.label}</span>
    </div>)}
  </div>;
}

type PerformancePoint = Json & {
  timestamp: number;
  average_percent_saved: number;
  run_count: number;
};

function performanceData(rows: Json[], range: "90" | "365" | "all"): PerformancePoint[] {
  const ordered = rows
    .map((row: Json) => ({ ...row, timestamp: Date.parse(row.date || "") }) as Json & { timestamp: number })
    .filter((row) => Number.isFinite(row.timestamp))
    .sort((left, right) => left.timestamp - right.timestamp);
  const cumulative: PerformancePoint[] = ordered.map((row) => ({
    ...row,
    average_percent_saved: Number(row.average_percent_saved || 0),
    run_count: 1,
  }) as PerformancePoint);
  const cutoff = range === "all" ? null : Date.now() - Number(range) * 24 * 60 * 60 * 1000;
  const filtered = cumulative.filter((row: Json) => cutoff === null || row.timestamp >= cutoff);
  const visible = filtered.length ? filtered : cumulative.slice(-1);
  const pointLimit = range === "90" ? 13 : 12;
  if (visible.length <= pointLimit) return visible;

  const firstTimestamp = visible[0].timestamp;
  const lastTimestamp = visible[visible.length - 1].timestamp;
  const span = Math.max(1, lastTimestamp - firstTimestamp);
  const buckets = new Map<number, Json[]>();
  visible.forEach((row: Json) => {
    const bucket = Math.min(pointLimit - 1, Math.floor((row.timestamp - firstTimestamp) / span * pointLimit));
    buckets.set(bucket, [...(buckets.get(bucket) || []), row]);
  });
  return [...buckets.entries()]
    .sort(([left], [right]) => left - right)
    .map(([, bucketRows]) => {
      const last = bucketRows[bucketRows.length - 1];
      return {
        ...last,
        run_count: bucketRows.length,
      } as PerformancePoint;
    });
}

function niceChartStep(value: number) {
  if (!Number.isFinite(value) || value <= 0) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const normalized = value / magnitude;
  const factor = [1, 2, 5, 10].reduce((closest, candidate) =>
    Math.abs(candidate - normalized) < Math.abs(closest - normalized) ? candidate : closest
  );
  return factor * magnitude;
}

function PerformanceChart({ rows, range }: { rows: Json[]; range: "90" | "365" | "all" }) {
  const data = performanceData(rows, range);
  if (!data.length) return <div className="chart-empty">Completed runs will build the lifetime trend.</div>;

  const width = 760;
  const height = 280;
  const margin = { top: 22, right: 78, bottom: 38, left: 52 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const netValues = data.map((row: Json) => Number(row.cumulative_net_gb || 0));
  const averageValues = data.map((row: Json) => Number(row.average_percent_saved || 0));
  const netFloor = Math.min(0, ...netValues);
  const netCeiling = Math.max(1, ...netValues);
  const netStep = niceChartStep((netCeiling - netFloor) / 4);
  const netMinimum = Math.floor(netFloor / netStep) * netStep;
  const netMaximum = Math.ceil(netCeiling / netStep) * netStep;
  const averageMinimum = Math.floor(Math.min(...averageValues) / 5) * 5;
  const averageMaximum = Math.max(5, Math.ceil(Math.max(...averageValues) / 5) * 5);
  const firstTimestamp = Number(data[0].timestamp);
  const lastTimestamp = Number(data[data.length - 1].timestamp);
  const timestampSpan = Math.max(1, lastTimestamp - firstTimestamp);
  const x = (row: Json, index: number) => data.length === 1
    ? margin.left + plotWidth
    : margin.left + (Number(row.timestamp) - firstTimestamp) / timestampSpan * plotWidth;
  const netY = (value: number) => margin.top + (netMaximum - value) / Math.max(1, netMaximum - netMinimum) * plotHeight;
  const averageY = (value: number) => margin.top + (averageMaximum - value) / Math.max(1, averageMaximum - averageMinimum) * plotHeight;
  const netPoints = data.map((row: Json, index: number) => `${x(row, index)},${netY(Number(row.cumulative_net_gb || 0))}`).join(" ");
  const averagePoints = data.map((row: Json, index: number) => `${x(row, index)},${averageY(Number(row.average_percent_saved || 0))}`).join(" ");
  const gridValues = Array.from(
    { length: Math.round((netMaximum - netMinimum) / netStep) + 1 },
    (_, index) => netMinimum + netStep * index,
  );
  const tickIndexes = [...new Set([0, Math.floor((data.length - 1) / 2), data.length - 1])];
  const dateFormat = range === "90"
    ? { day: "numeric", month: "short" } as const
    : { month: "short", year: "numeric" } as const;
  const formatDate = (row: Json) => new Date(row.timestamp).toLocaleDateString(undefined, dateFormat);
  const first = data[0];
  const last = data[data.length - 1];
  const displayedRuns = data.reduce((total: number, row: Json) => total + Number(row.run_count || 1), 0);

  return <figure className="performance-figure">
    <svg className="performance-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Net savings and average percentage saved over completed conversion runs">
      <title>Conversion performance over time</title>
      {gridValues.map((value) => <g key={value}>
        <line x1={margin.left} y1={netY(value)} x2={margin.left + plotWidth} y2={netY(value)} />
        <text x={margin.left - 9} y={netY(value) + 4} textAnchor="end">{Math.round(value)} GB</text>
      </g>)}
      <line x1={margin.left} y1={margin.top + plotHeight} x2={margin.left + plotWidth} y2={margin.top + plotHeight} />
      <polyline className="net-line" points={netPoints} />
      <polyline className="average-line" points={averagePoints} />
      {data.map((row: Json, index: number) => <g key={row.id || `${row.date}-${index}`}>
        <circle className="net-point" cx={x(row, index)} cy={netY(Number(row.cumulative_net_gb || 0))} r={index === data.length - 1 ? 4 : 3} />
        <circle className="average-point" cx={x(row, index)} cy={averageY(Number(row.average_percent_saved || 0))} r={index === data.length - 1 ? 4 : 3} />
      </g>)}
      {tickIndexes.map((index) => <text
        key={index}
        x={x(data[index], index)}
        y={height - 10}
        textAnchor={index === 0 ? "start" : index === data.length - 1 ? "end" : "middle"}
      >{formatDate(data[index])}</text>)}
      <text className="chart-value" x={margin.left + plotWidth + 10} y={netY(Number(last.cumulative_net_gb || 0)) + 4}>{Number(last.cumulative_net_gb || 0).toFixed(1)} GB</text>
      <text className="chart-value" x={margin.left + plotWidth + 10} y={averageY(Number(last.average_percent_saved || 0)) + 4}>{Number(last.average_percent_saved || 0).toFixed(1)}%</text>
    </svg>
    <figcaption className="chart-legend">
      <span><i className="net" />Cumulative net savings</span>
      <span><i className="average" />Average percentage saved</span>
      <small>{formatDate(first)}–{formatDate(last)} · {displayedRuns.toLocaleString()} runs · {Number(last.cumulative_net_gb || 0).toFixed(1)} GB saved · {Number(last.average_percent_saved || 0).toFixed(1)}% average</small>
    </figcaption>
  </figure>;
}

function DetailedStatistics({ data }: { data: Json }) {
  const [range, setRange] = useState<"90" | "365" | "all">("all");
  const health = data.health || {};
  const library = data.library || {};
  const outcomes = data.outcomes || [];
  const outcomeTotal = Math.max(1, outcomes.reduce((sum: number, row: Json) => sum + Number(row.count || 0), 0));
  const outcomeClasses = ["one", "two", "three", "four", "five"];
  const summary = data.savings_summary || {};
  const processing = data.processing || {};
  const filetypes = data.filetypes || {};
  const filetypeRows = filetypes.rows || [];
  const totalFiles = Math.max(1, Number(filetypes.total_files || 0));
  const totalStorage = Math.max(1, Number(filetypes.total_storage_bytes || 0));
  const maxFileCount = Math.max(1, ...filetypeRows.map((row: Json) => Number(row.file_count || 0)));
  const maxStorage = Math.max(1, ...filetypeRows.map((row: Json) => Number(row.storage_bytes || 0)));
  const inventoryIncomplete = Number(filetypes.inventoried_archives || 0) < Number(filetypes.archive_count || 0);
  return <div className="detailed-statistics">
    <section className="health-strip" aria-label="Library conversion health">
      <div><span>Library coverage</span><strong>{Number(health.library_coverage_percent || 0).toFixed(1)}%</strong><small>{Number(library.optimized_archives || 0).toLocaleString()} of {Number(library.archives || 0).toLocaleString()} archives</small></div>
      <div><span>Average reduction</span><strong>{Number(health.average_percent_saved || 0).toFixed(1)}%</strong><small>Across optimized archives</small></div>
      <div><span>Originals retained</span><strong>{compactNumber(health.retained_pages)} pages</strong><small>Larger candidates rejected</small></div>
      <div><span>Success rate</span><strong>{Number(health.success_percent || 0).toFixed(1)}%</strong><small>Completed archive attempts</small></div>
    </section>

    <section className="report-section">
      <header className="report-heading">
        <div><h2>Performance over time</h2><p>Cumulative net savings and average percentage saved across completed runs.</p></div>
        <div className="range-controls" role="group" aria-label="Performance range">
          {(["90", "365", "all"] as const).map((value) => <button className={range === value ? "active" : ""} key={value} onClick={() => setRange(value)}>{value === "90" ? "90 days" : value === "365" ? "1 year" : "All time"}</button>)}
        </div>
      </header>
      <PerformanceChart rows={data.run_trend || []} range={range} />
    </section>

    <section className="report-section">
      <header className="report-heading"><div><h2>Library composition</h2><p>The current shape of the indexed library.</p></div></header>
      <div className="library-strip">
        <div><span>Archives</span><strong>{Number(library.archives || 0).toLocaleString()}</strong></div>
        <div><span>Pages</span><strong>{compactNumber(library.pages)}</strong></div>
        <div><span>Source size</span><strong>{formatBytes(library.source_bytes)}</strong></div>
        <div><span>Current size</span><strong>{formatBytes(library.current_bytes)}</strong></div>
        <div><span>JXL coverage</span><strong>{Number(library.jxl_coverage_percent || 0).toFixed(1)}%</strong></div>
      </div>
      <div className="filetype-heading">
        <h3>File-type breakdown</h3>
        <p>Current image files measured by quantity and compressed storage inside their archives.</p>
      </div>
      {filetypeRows.length ? <div className="report-grid filetype-grid">
        <article className="report-panel">
          <header><h3>File count</h3><p>{Number(filetypes.total_files || 0).toLocaleString()} files</p></header>
          <div className="filetype-list" role="img" aria-label={`File count across ${filetypeRows.length} image formats`}>
            {filetypeRows.map((row: Json) => <div className="filetype-row" key={row.label}>
              <div className="filetype-copy">
                <strong>{row.label}</strong>
                <span>{Number(row.file_count || 0).toLocaleString()} · {(Number(row.file_count || 0) / totalFiles * 100).toFixed(1)}%</span>
              </div>
              <span className="effectiveness-track"><i style={{ width: `${Number(row.file_count || 0) / maxFileCount * 100}%` }} /></span>
            </div>)}
          </div>
        </article>
        <article className="report-panel">
          <header><h3>Storage used</h3><p>{formatBytes(filetypes.total_storage_bytes)}</p></header>
          <div className="filetype-list" role="img" aria-label={`Compressed storage across ${filetypeRows.length} image formats`}>
            {filetypeRows.map((row: Json) => <div className="filetype-row" key={row.label}>
              <div className="filetype-copy">
                <strong>{row.label}</strong>
                <span>{formatBytes(row.storage_bytes)} · {(Number(row.storage_bytes || 0) / totalStorage * 100).toFixed(1)}%</span>
              </div>
              <span className="effectiveness-track"><i style={{ width: `${Number(row.storage_bytes || 0) / maxStorage * 100}%` }} /></span>
            </div>)}
          </div>
        </article>
      </div> : <div className="filetype-empty">File-type inventory will appear after the next library scan.</div>}
      {inventoryIncomplete && <p className="filetype-coverage">Inventory currently covers {Number(filetypes.inventoried_archives || 0).toLocaleString()} of {Number(filetypes.archive_count || 0).toLocaleString()} indexed archives. The next library scan will fill the remainder.</p>}
    </section>

    <section className="report-section">
      <header className="report-heading"><div><h2>Compression effectiveness</h2><p>Where savings come from and how pages were handled.</p></div></header>
      <div className="report-grid">
        <article className="report-panel">
          <header><h3>By source format</h3><p>Bar length shows average percentage saved.</p></header>
          <div className="effectiveness-list">
            {(data.formats || []).map((row: Json) => <div className="effectiveness-row" key={row.label}>
              <strong>{row.label}</strong>
              <span className="effectiveness-track"><i style={{ width: `${Math.min(100, Math.max(0, Number(row.average_percent_saved || 0) * 3))}%` }} /></span>
              <span><b>{Number(row.average_percent_saved || 0).toFixed(1)}%</b><small>{formatBytes(row.net_bytes)}</small></span>
            </div>)}
          </div>
        </article>
        <article className="report-panel">
          <header><h3>Conversion outcomes</h3><p>Outcome of every recorded source page.</p></header>
          <div className="outcome-bar" role="img" aria-label="Conversion outcome distribution">
            {outcomes.map((row: Json, index: number) => <i className={outcomeClasses[index]} key={row.label} style={{ width: `${Number(row.count || 0) / outcomeTotal * 100}%` }} />)}
          </div>
          <div className="outcome-list">
            {outcomes.map((row: Json, index: number) => <div key={row.label}><i className={outcomeClasses[index]} /><span>{row.label}</span><strong>{(Number(row.count || 0) / outcomeTotal * 100).toFixed(1)}%</strong></div>)}
          </div>
        </article>
      </div>
      <article className="report-panel distribution-panel">
        <header><h3>Savings distribution</h3><p>Optimized archives grouped by percentage reduction.</p></header>
        <DistributionChart rows={data.savings_distribution || []} label="Archive savings distribution" />
        <div className="distribution-summary">
          <span>Median <strong>{Number(summary.median || 0).toFixed(1)}%</strong></span>
          <span>Average <strong>{Number(summary.average || 0).toFixed(1)}%</strong></span>
          <span>Middle 50% <strong>{Number(summary.middle_50?.[0] || 0).toFixed(1)}–{Number(summary.middle_50?.[1] || 0).toFixed(1)}%</strong></span>
        </div>
      </article>
    </section>

    <section className="report-section">
      <header className="report-heading">
        <div><h2>Processing performance</h2><p>Typical archive duration and policy efficiency.</p></div>
        <div className="heading-metrics">
          <span>Median archive <strong>{duration(processing.median_seconds)}</strong></span>
          <span>90th percentile <strong>{duration(processing.p90_seconds)}</strong></span>
        </div>
      </header>
      <div className="report-grid">
        <article className="report-panel">
          <header><h3>Processing-time distribution</h3><p>Completed archives grouped by elapsed time.</p></header>
          <DistributionChart rows={processing.distribution || []} tone="violet" label="Archive processing-time distribution" />
        </article>
        <article className="report-panel">
          <header><h3>Efficiency by conversion policy</h3><p>Lifetime results for policies that completed runs.</p></header>
          <div className="policy-report-wrap"><table className="policy-report">
            <thead><tr><th>Policy</th><th>Median</th><th>Avg saved</th><th>Success</th></tr></thead>
            <tbody>{(processing.policies || []).map((row: Json) => <tr key={row.name}><td>{row.name}</td><td>{duration(row.median_seconds)}</td><td>{Number(row.average_percent_saved || 0).toFixed(1)}%</td><td>{Number(row.success_percent || 0).toFixed(1)}%</td></tr>)}</tbody>
          </table></div>
        </article>
      </div>
    </section>

    <section className="report-section">
      <header className="report-heading"><div><h2>Attention breakdown</h2><p>Unresolved items grouped by the decision or repair they require.</p></div></header>
      <div className="attention-breakdown">{(data.attention || []).map((row: Json) => <div key={row.id}><span>{row.label}</span><i><b style={{ width: `${Number(row.count || 0) / Math.max(1, ...(data.attention || []).map((item: Json) => Number(item.count || 0))) * 100}%` }} /></i><strong>{Number(row.count || 0).toLocaleString()}</strong></div>)}</div>
    </section>

    <section className="report-section">
      <header className="report-heading"><div><h2>Lifetime processing effort</h2><p>Completed work, including retries and archives retained unchanged.</p></div></header>
      <div className="effort-strip">
        <div><span>Completed runs</span><strong>{Number(data.effort?.completed_runs || 0).toLocaleString()}</strong></div>
        <div><span>Archives attempted</span><strong>{Number(data.effort?.archives_attempted || 0).toLocaleString()}</strong></div>
        <div><span>Pages examined</span><strong>{compactNumber(data.effort?.pages_examined)}</strong></div>
        <div><span>Encoder time</span><strong>{Number(data.effort?.encoder_hours || 0).toLocaleString()} hours</strong></div>
      </div>
    </section>
  </div>;
}

function Overview({ dashboard, bootstrap, refresh, notify }: {
  dashboard: Json;
  bootstrap: Json;
  refresh: () => void;
  notify: (message: string, kind?: "success" | "error") => void;
}) {
  const active = dashboard.active_jobs?.[0];
  const [dismissedRun, setDismissedRun] = useState(() => {
    try { return window.localStorage.getItem("cbzxl-dismissed-run"); } catch { return null; }
  });
  const recentTerminal = (dashboard.recent_jobs || []).find((job: Json) =>
    ["manual", "retry"].includes(job.source) &&
    ["succeeded", "partial", "failed", "cancelled", "interrupted"].includes(job.status)
  );
  const visibleRun = active || (recentTerminal?.id !== dismissedRun ? recentTerminal : null);
  const dismissRun = (job: Json) => {
    setDismissedRun(job.id);
    try { window.localStorage.setItem("cbzxl-dismissed-run", job.id); } catch { /* storage unavailable */ }
  };
  const totals = dashboard.totals || {};
  const [detailsOpen, setDetailsOpen] = useState(() => {
    try { return window.localStorage.getItem("cbzxl-detailed-statistics") === "open"; } catch { return false; }
  });
  const [statistics, setStatistics] = useState<Json | null>(null);
  const [statisticsError, setStatisticsError] = useState("");
  const [statisticsLoading, setStatisticsLoading] = useState(false);
  useEffect(() => {
    if (!detailsOpen || statistics || statisticsLoading || statisticsError) return;
    setStatisticsLoading(true);
    api("/api/statistics")
      .then((payload) => {
        setStatistics(payload);
        setStatisticsError("");
      })
      .catch((error) => setStatisticsError(error.message))
      .finally(() => setStatisticsLoading(false));
  }, [detailsOpen, statistics, statisticsError, statisticsLoading]);
  const toggleDetails = () => {
    const next = !detailsOpen;
    setDetailsOpen(next);
    try { window.localStorage.setItem("cbzxl-detailed-statistics", next ? "open" : "closed"); } catch { /* storage unavailable */ }
  };
  return <div className="page overview">
    {visibleRun ? <LiveConversion key={visibleRun.id} initial={visibleRun} refresh={refresh} notify={notify} onDismiss={dismissRun} /> :
      <ManualConversion bootstrap={bootstrap} onQueued={refresh} notify={notify} />}
    <section className="stats-grid" aria-label="Lifetime optimization statistics">
      <div><span>Net savings</span><strong>{formatGb(totals.net_gb)}</strong></div>
      <div><span>Pages optimized</span><strong>{Number(totals.page_count || 0).toLocaleString()}</strong></div>
      <div><span>Archives optimized</span><strong>{Number(totals.optimized_count || 0).toLocaleString()}</strong></div>
      <div><span>Need attention</span><strong>{Number(dashboard.review?.total || 0).toLocaleString()}</strong></div>
    </section>
    <button className="detailed-statistics-toggle" type="button" aria-expanded={detailsOpen} onClick={toggleDetails}>
      <span>{detailsOpen ? "Hide Detailed Statistics" : "Detailed Statistics"}</span><b>{detailsOpen ? "−" : "+"}</b>
    </button>
    {detailsOpen && <div className="statistics-disclosure">
      {statisticsLoading && <div className="statistics-loading">Building lifetime statistics…</div>}
      {statisticsError && <div className="statistics-error">{statisticsError}<button className="button secondary" onClick={() => setStatisticsError("")}>Retry</button></div>}
      {statistics && <DetailedStatistics data={statistics} />}
    </div>}
  </div>;
}

function issueContext(issue: Json) {
  if (issue.kind === "crc_mismatch") {
    if (issue.diagnostics?.repairable) return {
      title: "This archive can be recovered safely",
      text: "The recovered image fully decodes. CBZXL can repair its ZIP metadata, convert it when eligible, and continue through the rest of the archive without changing names.",
      recommended: "Recover & convert",
    };
    return {
      title: "This CRC failure needs a decision",
      text: "The damaged entry cannot yet be proven intact. Replace it, explicitly delete it, or leave the archive unchanged.",
      recommended: "Inspect recovery status",
    };
  }
  if (issue.kind === "archive_corrupt" && issue.diagnostics?.salvageable) return {
    title: "This archive can be salvaged safely",
    text: "Every entry was recovered from intact local ZIP headers and fully checked. CBZXL can rebuild the archive without changing any names.",
    recommended: "Salvage archive",
  };
  if (issue.kind === "archive_corrupt" && issue.diagnostics?.partial_salvageable) return {
    title: "The complete entries can be recovered",
    text: "The archive ends in a truncated entry. CBZXL can rebuild every preceding entry that was recovered and fully verified, but the incomplete tail will be omitted only with your confirmation.",
    recommended: "Review and rebuild viable entries",
  };
  if (issue.kind === "archive_grew") return {
    title: Number(issue.latest_bytes_saved || 0) > 0
      ? `Latest retry saved ${formatBytes(issue.latest_bytes_saved)}, but the archive remains above its lifetime baseline`
      : Number(issue.latest_bytes_saved || 0) < 0
        ? `Latest attempt added ${formatBytes(Math.abs(issue.latest_bytes_saved))}`
        : `Archive remains ${formatBytes(Math.abs(issue.bytes_saved || 0))} above its lifetime baseline`,
    text: `Lifetime: ${formatBytes(issue.original_size)} → ${formatBytes(issue.final_size)}. Any retry keeps the current archive unless CBZXL produces and verifies a smaller replacement.`,
    recommended: Number(issue.latest_bytes_saved || 0) > 0 ? "Continue reducing or accept" : "Retry lossless",
  };
  if (issue.kind === "verification_failed") return {
    title: "The converted page failed verification",
    text: "The candidate archive was rejected before replacement. The original remains intact.",
    recommended: "Compatibility retry",
  };
  if (issue.kind === "filename_collision" && issue.related_entry_path) return {
    title: "Two images require the same filename",
    text: "Compare both pages and choose the one the archive should keep. CBZXL will remove only the other entry, correct the retained extension when necessary, and verify the rebuilt archive before replacing it.",
    recommended: "Choose one image to keep",
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

function IssuePreview({ issue, related = false }: { issue: Json; related?: boolean }) {
  const [failed, setFailed] = useState(false);
  const previewUrl = related ? issue.related_preview_url : issue.preview_url;
  const entryPath = related ? issue.related_entry_path : issue.entry_path;
  if (!previewUrl || failed) return <figure className="issue-preview unavailable"><span>⌁</span><figcaption>{entryPath ? `Preview unavailable · ${entryPath}` : "Archive-level issue"}</figcaption></figure>;
  return <figure className="issue-preview"><img src={previewUrl} alt={`Conflicting page ${entryPath}`} onError={() => setFailed(true)} /><figcaption><code>{entryPath}</code></figcaption></figure>;
}

const remedyLabels: Record<string, string> = {
  compatibility: "Compatibility retry",
  preserve: "Keep page, convert rest",
  replace: "Replace image",
  delete: "Delete image",
  lossless: "Lossless retry",
  smart_lossy: "Try Smart Lossy",
  restore_original: "Restore original",
  repack: "Repack archive",
  retry: "Default retry",
  rebuild: "Recover & convert",
  salvage: "Salvage archive",
  salvage_partial: "Rebuild viable entries",
  keep_primary: "Keep this image",
  keep_related: "Keep this image",
  dismiss: "Accept unchanged",
};

function Attention({ refreshDashboard, notify }: {
  refreshDashboard: () => void;
  notify: (message: string, kind?: "success" | "error") => void;
}) {
  const [data, setData] = useState<Json>({ rows: [], groups: [], total: 0 });
  const [groupRows, setGroupRows] = useState<Record<string, Json[]>>({});
  const [groupLoading, setGroupLoading] = useState<string | null>(null);
  const [openGroup, setOpenGroup] = useState<string | null>(null);
  const [open, setOpen] = useState<number | null>(null);
  const [busy, setBusy] = useState<number | null>(null);
  const [diagnostics, setDiagnostics] = useState<Record<number, Json>>({});
  const [diagnosticsLoading, setDiagnosticsLoading] = useState<number | null>(null);
  const fileInputs = useRef<Record<number, HTMLInputElement | null>>({});
  const load = useCallback(() => api("/api/issues?limit=1").then(setData).catch((error) => notify(error.message, "error")), [notify]);
  useEffect(() => { load(); }, [load]);

  const loadGroup = useCallback(async (groupId: string) => {
    setGroupLoading(groupId);
    try {
      const payload = await api(`/api/issues?group=${encodeURIComponent(groupId)}&limit=100`);
      setGroupRows((current) => ({ ...current, [groupId]: payload.rows || [] }));
    } catch (error) {
      notify((error as Error).message, "error");
    } finally {
      setGroupLoading(null);
    }
  }, [notify]);

  const toggleGroup = async (groupId: string) => {
    if (openGroup === groupId) {
      setOpenGroup(null);
      setOpen(null);
      return;
    }
    setOpenGroup(groupId);
    setOpen(null);
    if (!groupRows[groupId]) await loadGroup(groupId);
  };

  const refreshOpenGroup = async () => {
    await load();
    if (openGroup) await loadGroup(openGroup);
    refreshDashboard();
  };

  const act = async (issue: Json, action: string) => {
    if (action === "delete" && !window.confirm(`Delete ${issue.entry_path} from ${archiveName(issue.path)}? The archive name will not change.`)) return;
    if (action === "delete_archive" && !window.confirm(`Permanently delete ${archiveName(issue.path)} from the library? This cannot be undone.`)) return;
    if (action === "salvage_partial" && !window.confirm(`Rebuild ${archiveName(issue.path)} using only entries that were recovered and fully verified? The truncated archive tail will be omitted.`)) return;
    if (action === "keep_primary" || action === "keep_related") {
      const kept = action === "keep_primary" ? issue.entry_path : issue.related_entry_path;
      const discarded = action === "keep_primary" ? issue.related_entry_path : issue.entry_path;
      if (!window.confirm(`Keep ${kept} and remove ${discarded} from ${archiveName(issue.path)}? The rebuilt archive will be verified before replacement.`)) return;
    }
    setBusy(issue.id);
    try {
      await api(`/api/issues/${issue.id}/action`, { method: "POST", body: JSON.stringify({ action }) });
      notify(action === "dismiss" ? "Dismissed until the next full rescan" : action === "delete_archive" ? "Archive deleted" : action.startsWith("keep_") ? "Selection queued for verified rebuild" : "Repair queued");
      if (action === "dismiss" || action === "delete_archive") setOpen(null);
      await refreshOpenGroup();
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
      await refreshOpenGroup();
    } catch (error) {
      notify((error as Error).message, "error");
    } finally {
      setBusy(null);
    }
  };

  const toggleInspect = async (issue: Json) => {
    if (open === issue.id) {
      setOpen(null);
      return;
    }
    setOpen(issue.id);
    if (issue.diagnostics_url && !diagnostics[issue.id]) {
      setDiagnosticsLoading(issue.id);
      try {
        const result = await api(issue.diagnostics_url);
        setDiagnostics((current) => ({ ...current, [issue.id]: result }));
      } catch (error) {
        notify((error as Error).message, "error");
      } finally {
        setDiagnosticsLoading(null);
      }
    }
  };

  if (!data.groups?.length) return <div className="empty-state"><span>✓</span><h2>Nothing needs attention.</h2><p>Failures and exceptions will appear here with actions tailored to their cause.</p></div>;
  return <div className="failure-type-list">
    {data.groups.map((group: Json) => {
      const expandedGroup = openGroup === group.id;
      const rows = groupRows[group.id] || [];
      return <article className={`failure-type ${expandedGroup ? "expanded" : ""}`} key={group.id}>
        <div className="failure-type-summary">
          <div>
            <strong>{group.label}</strong>
            <span><b>{group.triage}</b>{group.recommended}</span>
          </div>
          <span className="failure-count">{Number(group.count || 0).toLocaleString()}</span>
          <button className="button ghost" aria-expanded={expandedGroup} onClick={() => toggleGroup(group.id)}>{expandedGroup ? "Close" : "Review"}</button>
        </div>
        {expandedGroup && <div className="failure-type-detail">
          {groupLoading === group.id && <div className="group-loading">Loading affected archives…</div>}
          {rows.map((issue: Json) => {
            const diagnostic = diagnostics[issue.id];
            const hydratedIssue = diagnostic ? { ...issue, diagnostics: diagnostic } : issue;
            const context = issueContext(hydratedIssue);
            const expanded = open === issue.id;
            const collision = issue.kind === "filename_collision" && Boolean(issue.related_entry_path);
            const remedies = (issue.remedies || []).filter((remedy: string) =>
              remedy === "rebuild" ? diagnostic?.repairable :
              remedy === "salvage" ? diagnostic?.salvageable :
              remedy === "salvage_partial" ? diagnostic?.partial_salvageable :
              true
            );
            return <div className={`issue ${expanded ? "expanded" : ""}`} key={issue.id}>
              <div className="issue-summary">
                <div><strong>{archiveName(issue.path)}</strong><span className="error-copy">{issue.message}</span>{issue.entry_path && <code>{issue.entry_path}</code>}</div>
                <time>{dateTime(issue.updated_at)}</time>
                <div className="issue-actions">
                  {!collision && <button className="button primary" disabled={busy === issue.id || issue.status === "queued"} onClick={() => act(issue, "retry")}>{issue.status === "queued" ? "Queued" : "Retry"}</button>}
                  <button className="button secondary" aria-expanded={expanded} onClick={() => toggleInspect(issue)}>{expanded ? "Close" : "Inspect"}</button>
                  {issue.retry_failed && <button className="button ghost danger" disabled={busy === issue.id || issue.status === "queued"} onClick={() => act(issue, "delete_archive")}>Delete archive</button>}
                  <button className="button ghost" disabled={busy === issue.id} onClick={() => act(issue, "dismiss")}>Dismiss</button>
                </div>
              </div>
              {expanded && <div className={`issue-detail ${issue.entry_path ? "" : "without-preview"} ${collision ? "collision-detail" : ""}`}>
                {collision ? <div className="collision-comparison" aria-label="Conflicting images">
                  <div className="collision-choice">
                    <IssuePreview issue={issue} />
                    <button className="button primary block" disabled={busy === issue.id || issue.status === "queued"} onClick={() => act(issue, "keep_primary")}>Keep this image</button>
                  </div>
                  <div className="collision-choice">
                    <IssuePreview issue={issue} related />
                    <button className="button primary block" disabled={busy === issue.id || issue.status === "queued"} onClick={() => act(issue, "keep_related")}>Keep this image</button>
                  </div>
                </div> : issue.entry_path && <IssuePreview issue={issue} />}
                <div className="diagnosis">
                  <div><h3>{context.title}</h3><p>{context.text}</p></div>
                  <div className="diagnosis-facts">
                    <div><span>Failure stage</span><strong>{label(issue.stage)}</strong></div>
                    <div><span>Recommended</span><strong>{context.recommended}</strong></div>
                    {diagnostic?.kind === "crc" && <>
                      <div><span>Stored CRC-32</span><strong><code>{diagnostic.expected_crc32 || "Unavailable"}</code></strong></div>
                      <div><span>Calculated CRC-32</span><strong><code>{diagnostic.calculated_crc32 || "Unavailable"}</code></strong></div>
                      <div><span>Recovery status</span><strong>{diagnostic.image_readable ? "Recovered image decodes correctly" : "Recovered image is unreadable"}</strong></div>
                    </>}
                  </div>
                  {diagnosticsLoading === issue.id && <div className="diagnostic-loading">Checking the failing ZIP entry…</div>}
                  {diagnostic?.entries?.length > 0 && <section className="recovery-assessment">
                    <header>
                      <strong>Recovery assessment</strong>
                      <span>{Number(diagnostic.verified_count || 0).toLocaleString()} of {Number(diagnostic.checked_entries || 0).toLocaleString()} entries verified</span>
                    </header>
                    <div className="recovery-entry-list">
                      {diagnostic.entries.slice(0, 12).map((entry: Json) => <div key={entry.entry_path}>
                        <code>{entry.entry_path}</code>
                        <span className={entry.repairable ? "recoverable" : "decision"}>{entry.repairable ? "Recoverable" : "Needs decision"}</span>
                      </div>)}
                    </div>
                    {diagnostic.entries.length > 12 && <small>And {diagnostic.entries.length - 12} more entries in this recovery plan.</small>}
                  </section>}
                  {(diagnostic?.detail || issue.detail) && <details><summary>Technical detail</summary><code>{diagnostic?.detail || issue.detail}</code></details>}
                  <input className="visually-hidden" type="file" accept="image/*" ref={(node) => { fileInputs.current[issue.id] = node; }} onChange={(event) => replace(issue, event.target.files?.[0])} />
                  {!collision && <div className="remedy-grid">
                    {remedies.map((remedy: string) => remedy === "replace" ?
                      <button className="button secondary block" key={remedy} disabled={busy === issue.id} onClick={() => fileInputs.current[issue.id]?.click()}>{remedyLabels[remedy]}</button> :
                      <button className={`button block ${["compatibility", "lossless", "smart_lossy", "rebuild", "salvage", "salvage_partial"].includes(remedy) ? "primary" : remedy === "delete" ? "danger" : "secondary"}`} key={remedy} disabled={busy === issue.id} onClick={() => act(issue, remedy)}>{remedyLabels[remedy] || label(remedy)}</button>)}
                  </div>}
                </div>
              </div>}
            </div>;
          })}
          {rows.length >= 100 && <div className="group-limit">Showing the 100 most recently updated archives in this failure type.</div>}
        </div>}
      </article>;
    })}
  </div>;
}

function HistoryDetail({ id }: { id: string }) {
  const [detail, setDetail] = useState<Json | null>(null);
  const [filter, setFilter] = useState("all");
  useEffect(() => { api(`/api/jobs/${id}/history`).then(setDetail).catch(() => setDetail({ events: [] })); }, [id]);
  if (!detail) return <div className="history-loading" />;
  const job = detail.job || {};
  const counts = detail.outcome_counts || {};
  const entries = detail.history_entries || [];
  const filteredEntries = filter === "all"
    ? entries
    : entries.filter((entry: Json) => entry.outcome === filter);
  const filters = [
    ["all", "All"],
    ["optimized", "Optimized"],
    ["already_current", "Already current"],
    ["retained", "Retained"],
    ["failed", "Failed"],
  ];
  return <div className="history-detail">
    <div className="history-facts">
      <div><span>Duration</span><strong>{duration(job.elapsed_seconds)}</strong></div>
      <div><span>Checked</span><strong>{Number(counts.all || 0).toLocaleString()}</strong></div>
      <div><span>Optimized</span><strong>{Number(counts.optimized || 0).toLocaleString()}</strong></div>
      <div><span>Already current</span><strong>{Number(counts.already_current || 0).toLocaleString()}</strong></div>
      <div><span>Retained</span><strong>{Number(counts.retained || 0).toLocaleString()}</strong></div>
      <div><span>Failed</span><strong>{Number(counts.failed || 0).toLocaleString()}</strong></div>
    </div>
    <div className="history-outcome-filters" role="group" aria-label="Archive outcome">
      {filters.map(([value, text]) => <button
        type="button"
        className={filter === value ? "selected" : ""}
        key={value}
        onClick={() => setFilter(value)}
      >{text}<span>{Number(counts[value] || 0).toLocaleString()}</span></button>)}
    </div>
    <div className="history-outcomes">
      {filteredEntries.map((entry: Json) => <article className={`history-outcome ${entry.outcome}`} key={entry.id}>
        <span className="history-outcome-mark" aria-hidden="true">
          {entry.outcome === "optimized" ? "✓" : entry.outcome === "retained" ? "◇" : entry.outcome === "failed" ? "!" : "↺"}
        </span>
        <div>
          <header><strong>{entry.label}</strong><time>{dateTime(entry.recorded_at)}</time></header>
          <code>{entry.path}</code>
          <p>{entry.detail}</p>
          <footer>
            {entry.converted_pages > 0 && <span>{Number(entry.converted_pages).toLocaleString()} converted</span>}
            {entry.lossless_pages > 0 && <span>{Number(entry.lossless_pages).toLocaleString()} lossless</span>}
            {entry.lossy_pages > 0 && <span>{Number(entry.lossy_pages).toLocaleString()} smart lossy</span>}
            {entry.retained_pages > 0 && <span>{Number(entry.retained_pages).toLocaleString()} retained</span>}
            {entry.bytes_saved > 0 && <span>{formatBytes(entry.bytes_saved)} saved</span>}
            {entry.previous_processed_at && <span>Previously processed {dateTime(entry.previous_processed_at)}</span>}
            {entry.jxl_effort !== null && entry.jxl_effort !== undefined && <span>Effort {entry.jxl_effort}</span>}
          </footer>
        </div>
      </article>)}
      {!filteredEntries.length && <div className="history-outcome-empty">No {filter === "all" ? "archive outcomes" : filters.find(([value]) => value === filter)?.[1].toLowerCase()} are available in this history window.</div>}
      {Number(counts[filter] || 0) > filteredEntries.length && <small className="history-outcome-limit">Showing the latest {filteredEntries.length.toLocaleString()} of {Number(counts[filter] || 0).toLocaleString()} matching archives.</small>}
    </div>
    <details className="history-technical">
      <summary>Technical activity</summary>
      <div className="event-list">{(detail.events || []).slice(0, 20).map((event: Json) => <div key={event.id}><i className={event.level} /><time>{dateTime(event.created_at)}</time><strong>{event.message}</strong>{event.archive_path && <code>{event.archive_path}</code>}</div>)}</div>
    </details>
  </div>;
}

function History({ notify }: { notify: (message: string, kind?: "success" | "error") => void }) {
  const [rows, setRows] = useState<Json[]>([]);
  const [open, setOpen] = useState<string | null>(null);
  const load = useCallback(() => api("/api/jobs?limit=100").then((value) => setRows(value.rows)).catch((error) => notify(error.message, "error")), [notify]);
  useEffect(() => { load(); }, [load]);
  return <div className="history-list">
    {rows.map((row) => {
      const checked = Number(row.processed || 0) + Number(row.skipped || 0) + Number(row.failed || 0) + Number(row.deleted || 0);
      return <article key={row.id}>
      <button className="history-summary" onClick={() => setOpen(open === row.id ? null : row.id)} aria-expanded={open === row.id}>
        <span className={`history-dot ${row.status}`} />
        <div><strong>{row.source === "schedule" ? "Scheduled conversion" : row.source === "retry" ? "Archive repair" : "Library conversion"}</strong><time>{dateTime(row.started_at || row.queued_at)}</time></div>
        <span>{checked.toLocaleString()} checked · {Number(row.skipped || 0).toLocaleString()} already current · {Number(row.failed || 0).toLocaleString()} failed</span>
        <strong title="Net savings from this run">{formatGb(row.net_gb)}</strong>
        <span className="status-label">{label(row.status)}</span>
        <b>{open === row.id ? "−" : "+"}</b>
      </button>
      {open === row.id && <HistoryDetail id={row.id} />}
    </article>;
    })}
  </div>;
}

function Settings({ bootstrap, reload, notify }: {
  bootstrap: Json;
  reload: () => void;
  notify: (message: string, kind?: "success" | "error") => void;
}) {
  const [schedules, setSchedules] = useState<Json[]>([]);
  const [newOpen, setNewOpen] = useState(false);
  const profiles = bootstrap.profiles || [];
  const defaultProfile = profiles.find((row: Json) => row.is_default) || profiles[0] || {};
  const [form, setForm] = useState({
    name: "Library maintenance",
    cadence: "daily",
    hour: 4,
    minute: 0,
    profile_id: defaultProfile.id || "",
  });
  const [policyOpen, setPolicyOpen] = useState(false);
  const [policyBusy, setPolicyBusy] = useState(false);
  const [policyForm, setPolicyForm] = useState<Json>({});
  const [scheduleProfiles, setScheduleProfiles] = useState<Record<string, string>>({});
  const load = useCallback(() => api("/api/schedules").then((value) => {
    setSchedules(value.rows);
    setScheduleProfiles(Object.fromEntries(value.rows.map((row: Json) => [row.id, row.profile_id])));
  }).catch((error) => notify(error.message, "error")), [notify]);
  useEffect(() => { load(); }, [load]);
  useEffect(() => {
    if (!profiles.some((row: Json) => row.id === form.profile_id) && defaultProfile.id) {
      setForm((current) => ({ ...current, profile_id: defaultProfile.id }));
    }
  }, [defaultProfile.id, form.profile_id, profiles]);

  const schedulePayload = (row: Json, overrides: Json = {}) => ({
    name: row.name,
    enabled: Boolean(row.enabled),
    cadence: row.cadence,
    interval_value: row.interval_value,
    interval_unit: row.interval_unit,
    weekday: row.weekday,
    day_of_month: row.day_of_month,
    hour: Number(row.hour || 0),
    minute: Number(row.minute || 0),
    library_id: row.library_id,
    profile_id: row.profile_id,
    ...overrides,
  });

  const openPolicy = (profile?: Json) => {
    const options = profile?.options || defaultProfile.options || {};
    setPolicyForm({
      id: profile?.id || null,
      name: profile?.name || "",
      strategy: policyStrategy(options),
      effort: Number(options.effort ?? 10),
      threads: Number(options.threads ?? 4),
      distance: Number(options.distance ?? 0.5),
      smart_mb: Number(options.smart_mb ?? 10),
      smart_mp: Number(options.smart_mp ?? 5),
      backup: Boolean(options.backup),
      scope: policyScope(options),
      is_default: Boolean(profile?.is_default),
      base_options: options,
    });
    setPolicyOpen(true);
  };

  const savePolicy = async (event: FormEvent) => {
    event.preventDefault();
    setPolicyBusy(true);
    const strategy = policyForm.strategy;
    const options: Json = {
      ...(policyForm.base_options || {}),
      effort: Number(policyForm.effort),
      threads: Number(policyForm.threads),
      backup: Boolean(policyForm.backup),
      smart_distance: strategy === "smart",
      distance: strategy === "smart" || strategy === "lossy" ? Number(policyForm.distance) : null,
      smart_mb: Number(policyForm.smart_mb),
      smart_mp: Number(policyForm.smart_mp),
      no_convert: strategy === "recheck" || strategy === "audit",
      integrity_audit: strategy === "audit",
      recheck_all: strategy === "audit" || policyForm.scope === "library",
      failed_only: strategy !== "audit" && policyForm.scope === "failed",
      compatibility_mode: false,
      continue_on_page_error: false,
      only_path: null,
      repair_action: null,
      repair_entry: null,
      repair_source: null,
    };
    try {
      await api(policyForm.id ? `/api/profiles/${policyForm.id}` : "/api/profiles", {
        method: policyForm.id ? "PUT" : "POST",
        body: JSON.stringify({
          name: policyForm.name,
          is_default: Boolean(policyForm.is_default),
          options,
        }),
      });
      notify(policyForm.id ? "Conversion policy updated" : "Conversion policy created");
      setPolicyOpen(false);
      await reload();
      await load();
    } catch (error) {
      notify((error as Error).message, "error");
    } finally {
      setPolicyBusy(false);
    }
  };
  const removePolicy = async (profile: Json) => {
    const usedBy = schedules.filter((row) => row.profile_id === profile.id);
    const scheduleNote = usedBy.length
      ? " Schedules using it will switch to the remaining default policy."
      : "";
    if (!window.confirm(`Delete ${profile.name}?${scheduleNote} Its job history will remain.`)) return;
    try {
      await api(`/api/profiles/${profile.id}`, { method: "DELETE" });
      notify("Conversion policy deleted");
      setPolicyOpen(false);
      await reload();
      await load();
    } catch (error) {
      notify((error as Error).message, "error");
    }
  };

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
          profile_id: form.profile_id || defaultProfile.id,
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
      await api(`/api/schedules/${row.id}`, {
        method: "PUT",
        body: JSON.stringify(schedulePayload(row, { enabled: !row.enabled })),
      });
      notify(row.enabled ? "Schedule paused" : "Schedule enabled");
      load();
      reload();
    } catch (error) { notify((error as Error).message, "error"); }
  };
  const saveSchedulePolicy = async (row: Json) => {
    const profileId = scheduleProfiles[row.id] || row.profile_id;
    try {
      await api(`/api/schedules/${row.id}`, {
        method: "PUT",
        body: JSON.stringify(schedulePayload(row, { profile_id: profileId })),
      });
      notify("Schedule policy updated");
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
      <div><span>Default policy</span><strong>{defaultProfile.name || "Balanced lossless"}</strong></div>
      <div><span>Active schedule</span><strong>{schedules.find((row) => row.enabled)?.description || "Not scheduled"}</strong></div>
    </section>

    <section className="policy-section">
      <header>
        <div><h2>Conversion policies</h2><p>Save encoder choices once, then assign them to manual or scheduled runs.</p></div>
        <button className="button primary" type="button" onClick={() => openPolicy()}>New policy</button>
      </header>
      <div className="policy-list">
        {profiles.map((profile: Json) => {
          const usedBy = schedules.filter((row) => row.profile_id === profile.id);
          return <article key={profile.id}>
            <div>
              <div className="policy-name"><strong>{profile.name}</strong>{profile.is_default && <span className="status-label">Default</span>}</div>
              <span>{policySummary(profile)} · {usedBy.length ? `used by ${usedBy.map((row) => row.name).join(", ")}` : "unused"}</span>
            </div>
            <div className="policy-actions">
              <button className="button secondary" type="button" onClick={() => openPolicy(profile)}>Edit</button>
              <button className="button ghost danger" type="button" onClick={() => removePolicy(profile)}>Delete</button>
            </div>
          </article>;
        })}
      </div>
      {policyOpen && <form className="policy-editor" onSubmit={savePolicy}>
        <header>
          <div><h3>{policyForm.id ? "Edit conversion policy" : "New conversion policy"}</h3><p>Archive and image names remain unchanged under every policy.</p></div>
          <button className="button ghost" type="button" onClick={() => setPolicyOpen(false)}>Close</button>
        </header>
        <div className="policy-fields">
          <label><span>Policy name</span><input type="text" value={policyForm.name || ""} onChange={(event) => setPolicyForm({ ...policyForm, name: event.target.value })} required /></label>
          <label><span>Conversion strategy</span><select value={policyForm.strategy || "lossless"} onChange={(event) => {
            const strategy = event.target.value;
            setPolicyForm({
              ...policyForm,
              strategy,
              scope: strategy === "audit" ? "library" : policyForm.scope,
            });
          }}>
            <option value="lossless">Lossless</option>
            <option value="smart">Lossless first, smart lossy fallback</option>
            <option value="lossy">Fixed lossy distance</option>
            <option value="recheck">Recheck without conversion</option>
            <option value="audit">Integrity audit (read only)</option>
          </select></label>
          <label><span>Scope</span><select disabled={policyForm.strategy === "audit"} value={policyForm.strategy === "audit" ? "library" : policyForm.scope || "unprocessed"} onChange={(event) => setPolicyForm({ ...policyForm, scope: event.target.value })}>
            <option value="unprocessed">Unprocessed</option>
            <option value="library">Entire Library</option>
            <option value="failed">Failed</option>
          </select></label>
          <label><span>Encoder effort · {policyForm.effort ?? 10}</span><input type="range" min="0" max="11" value={policyForm.effort ?? 10} onChange={(event) => setPolicyForm({ ...policyForm, effort: Number(event.target.value) })} /></label>
          <label><span>CPU budget</span><select value={policyForm.threads ?? 4} onChange={(event) => setPolicyForm({ ...policyForm, threads: Number(event.target.value) })}>{[1, 2, 4, 6, 8, 12, 16, 24, 32].map((value) => <option key={value} value={value}>{value} threads</option>)}</select></label>
        </div>
        {["smart", "lossy"].includes(policyForm.strategy) && <div className={`policy-lossy-fields ${policyForm.strategy === "lossy" ? "direct" : ""}`}>
          <label><span>Lossy distance · {Number(policyForm.distance ?? 0.5).toFixed(1)}</span><input type="range" min="0.1" max="1" step="0.1" value={policyForm.distance ?? 0.5} onChange={(event) => setPolicyForm({ ...policyForm, distance: Number(event.target.value) })} /></label>
          {policyForm.strategy === "smart" && <>
            <label><span>Minimum source size</span><input type="number" min="0.1" max="1000" step="0.1" value={policyForm.smart_mb ?? 10} onChange={(event) => setPolicyForm({ ...policyForm, smart_mb: Number(event.target.value) })} /><small>MB</small></label>
            <label><span>Minimum resolution</span><input type="number" min="0.1" max="1000" step="0.1" value={policyForm.smart_mp ?? 5} onChange={(event) => setPolicyForm({ ...policyForm, smart_mp: Number(event.target.value) })} /><small>MP</small></label>
          </>}
        </div>}
        <div className="policy-switches">
          <label className="switch"><input type="checkbox" checked={Boolean(policyForm.backup)} onChange={(event) => setPolicyForm({ ...policyForm, backup: event.target.checked })} /><i /><span>Back up originals</span></label>
          <label className="switch"><input type="checkbox" checked={Boolean(policyForm.is_default)} onChange={(event) => setPolicyForm({ ...policyForm, is_default: event.target.checked })} /><i /><span>Use as the default policy</span></label>
        </div>
        <footer>
          <button className="button ghost" type="button" onClick={() => setPolicyOpen(false)}>Cancel</button>
          <button className="button primary" type="submit" disabled={policyBusy}>{policyBusy ? "Saving…" : "Save policy"}</button>
        </footer>
      </form>}
    </section>

    <section className="schedule-section">
      <header><div><h2>Schedules</h2><p>Recurring conversions use the same verified pipeline.</p></div><button className="button primary" onClick={() => setNewOpen(!newOpen)}>{newOpen ? "Close" : "New schedule"}</button></header>
      {newOpen && <form className="schedule-form" onSubmit={save}>
        <label><span>Name</span><input value={form.name} onChange={(event) => setForm({ ...form, name: event.target.value })} required /></label>
        <label><span>Cadence</span><select value={form.cadence} onChange={(event) => setForm({ ...form, cadence: event.target.value })}><option value="daily">Daily</option><option value="weekly">Weekly</option><option value="monthly">Monthly</option></select></label>
        <label><span>Hour</span><select value={form.hour} onChange={(event) => setForm({ ...form, hour: Number(event.target.value) })}>{Array.from({ length: 24 }, (_, hour) => <option key={hour} value={hour}>{new Date(2000, 0, 1, hour).toLocaleTimeString([], { hour: "numeric" })}</option>)}</select></label>
        <label><span>Conversion policy</span><select value={form.profile_id} onChange={(event) => setForm({ ...form, profile_id: event.target.value })}>{profiles.map((profile: Json) => <option key={profile.id} value={profile.id}>{profile.name}</option>)}</select></label>
        <button className="button primary" type="submit">Save schedule</button>
      </form>}
      <div className="schedule-list">{schedules.map((row) => <article key={row.id} className={!row.enabled ? "disabled" : ""}>
        <div><strong>{row.name}</strong><span>{row.description} · {row.enabled ? `next ${dateTime(row.next_run_at)}` : "paused"}</span></div>
        <label className="schedule-policy"><span>Conversion policy</span><select value={scheduleProfiles[row.id] || row.profile_id} onChange={(event) => setScheduleProfiles({ ...scheduleProfiles, [row.id]: event.target.value })}>{profiles.map((profile: Json) => <option key={profile.id} value={profile.id}>{profile.name}</option>)}</select></label>
        <div><button className="button secondary" onClick={() => saveSchedulePolicy(row)} disabled={(scheduleProfiles[row.id] || row.profile_id) === row.profile_id}>Save</button><button className="button secondary" onClick={() => toggle(row)}>{row.enabled ? "Pause" : "Enable"}</button><button className="button ghost danger" onClick={() => remove(row)}>Delete</button></div>
      </article>)}</div>
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
