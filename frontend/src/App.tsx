import { FormEvent, useCallback, useEffect, useMemo, useRef, useState } from "react";

import { Logo, Toast } from "./components/Brand";
import { api, setToken, token } from "./lib/api";
import {
  archiveName,
  compactNumber,
  cpuOptions,
  dateTime,
  duration,
  formatBytes,
  formatGb,
  label,
} from "./lib/format";
import { policyScope, policyStrategy, policySummary } from "./lib/policies";
import type { Json, ToastValue, View } from "./types";

export function liveStage(value = "") {
  if (value === "discovering") return "Discovering";
  if (value === "reconciling") return "Preparing Queue";
  if (value === "archive_retrying") return "Retrying Safely";
  if (["job_started", "archive_started", "page_completed", "archive_completed", "job_progress"].includes(value)) return "Converting";
  return label(value || "Converting");
}

function LiveConversion({ initial, refresh, notify, onDismiss }: {
  initial: Json;
  refresh: () => void;
  notify: (message: string, kind?: "success" | "error") => void;
  onDismiss?: (job: Json) => void;
}) {
  const [detail, setDetail] = useState<Json>({ job: initial, events: [] });
  const [liveError, setLiveError] = useState(false);
  useEffect(() => {
    let stopped = false;
    let timer: number | undefined;
    let controller: AbortController | undefined;
    const schedule = (delay: number) => {
      if (!stopped && document.visibilityState === "visible") {
        timer = window.setTimeout(load, delay);
      }
    };
    const load = async () => {
      if (stopped || document.visibilityState !== "visible") return;
      controller = new AbortController();
      try {
        const payload = await api(`/api/jobs/${initial.id}/live`, { signal: controller.signal });
        if (stopped) return;
        setDetail(payload);
        setLiveError(false);
        const nextJob = payload.job || {};
        if (["queued", "running", "stopping"].includes(nextJob.status)) {
          schedule(nextJob.status === "queued" ? 2500 : 1200);
        } else {
          refresh();
        }
      } catch (error) {
        if (!stopped && (error as Error).name !== "AbortError") {
          setLiveError(true);
          schedule(3000);
        }
      }
    };
    const visibilityChanged = () => {
      if (document.visibilityState === "visible") {
        if (timer) window.clearTimeout(timer);
        load();
      } else {
        if (timer) window.clearTimeout(timer);
        controller?.abort();
      }
    };
    load();
    document.addEventListener("visibilitychange", visibilityChanged);
    return () => {
      stopped = true;
      if (timer) window.clearTimeout(timer);
      controller?.abort();
      document.removeEventListener("visibilitychange", visibilityChanged);
    };
  }, [initial.id, refresh]);
  const job = detail.job || initial;
  const percent = Number(job.progress_percent ?? job.smooth_percent ?? job.percent ?? 0);
  const completed = Number(job.completed || 0);
  const total = Number(job.total || job.archives_found || 0);
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
      : liveStage(job.current_stage);
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
        <code>{terminal ? job.error_message || `${completed.toLocaleString()} of ${total.toLocaleString()} archives completed` : job.current_archive ? `${completed.toLocaleString()} of ${total.toLocaleString()} complete · ${job.current_archive}` : total ? `${total.toLocaleString()} archives found…` : "Preparing the conversion queue…"}</code>
        {liveError && <small>Live progress is reconnecting…</small>}
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
      <div><span>Overall</span><strong>{percent > 0 && percent < 1 ? percent.toFixed(2) : percent < 10 ? percent.toFixed(1) : Math.round(percent)}%</strong></div>
      <div><span>Remaining</span><strong>{completed >= 10 ? duration(job.eta_seconds) : "Estimating"}</strong></div>
      <div><span>Throughput</span><strong>{job.rate_per_minute ? `${Number(job.rate_per_minute).toFixed(1)}/min` : "Learning"}</strong></div>
      <div><span>Reclaimed</span><strong>{formatGb(job.net_gb)}</strong></div>
    </div>
  </section>;
}

function ManualConversion({ bootstrap, onQueued, notify }: {
  bootstrap: Json;
  onQueued: () => void;
  notify: (message: string, kind?: "success" | "error") => void;
}) {
  const profiles = bootstrap.profiles || [];
  const runtimeUnavailable = bootstrap.conversion_runtime_status === "unavailable";
  const cpuLimit = Math.max(1, Number(bootstrap.capabilities?.cpu_limit || 1));
  const availableCpuOptions = cpuOptions(cpuLimit);
  const defaultProfile = profiles.find((row: Json) => row.is_default) || profiles[0] || {};
  const [open, setOpen] = useState(false);
  const [scope, setScope] = useState<"library" | "unprocessed" | "failed">("unprocessed");
  const [libraryId, setLibraryId] = useState(bootstrap.libraries?.[0]?.id || "");
  const [profileId, setProfileId] = useState(defaultProfile.id || "");
  const profile = profiles.find((row: Json) => row.id === profileId) || defaultProfile;
  const defaults = profile.options || {};
  const [effort, setEffort] = useState(Number(defaults.effort ?? 10));
  const [threads, setThreads] = useState(Math.min(cpuLimit, Number(defaults.threads ?? 4)));
  const [backup, setBackup] = useState(Boolean(defaults.backup));
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!profiles.some((row: Json) => row.id === profileId) && defaultProfile.id) {
      setProfileId(defaultProfile.id);
    }
  }, [defaultProfile.id, profileId, profiles]);
  useEffect(() => {
    if (!bootstrap.libraries?.some((library: Json) => library.id === libraryId)) {
      setLibraryId(bootstrap.libraries?.find((library: Json) => library.enabled)?.id || "");
    }
  }, [bootstrap.libraries, libraryId]);

  const chooseProfile = (nextProfile: Json) => {
    const options = nextProfile.options || {};
    setProfileId(nextProfile.id);
    setEffort(Number(options.effort ?? 10));
    setThreads(Math.min(cpuLimit, Number(options.threads ?? 4)));
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
          library_id: libraryId,
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
    {!open ? <button className="manual-launch" type="button" disabled={runtimeUnavailable} onClick={() => setOpen(true)}>{runtimeUnavailable ? "Conversion Runtime Unavailable" : "Begin Manual Conversion"}</button> :
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
          <label><span>CPU budget</span><select value={threads} onChange={(event) => setThreads(Number(event.target.value))}>{availableCpuOptions.map((value) => <option key={value} value={value}>{value} thread{value === 1 ? "" : "s"}</option>)}</select></label>
          {bootstrap.libraries?.length > 1 && <label><span>Library</span><select value={libraryId} onChange={(event) => setLibraryId(event.target.value)}>{bootstrap.libraries.filter((library: Json) => library.enabled).map((library: Json) => <option key={library.id} value={library.id}>{library.name}</option>)}</select></label>}
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
  const netFloor = Math.min(0, ...netValues);
  const netCeiling = Math.max(1, ...netValues);
  const netStep = niceChartStep((netCeiling - netFloor) / 4);
  const netMinimum = Math.floor(netFloor / netStep) * netStep;
  const netMaximum = Math.ceil(netCeiling / netStep) * netStep;
  const firstTimestamp = Number(data[0].timestamp);
  const lastTimestamp = Number(data[data.length - 1].timestamp);
  const timestampSpan = Math.max(1, lastTimestamp - firstTimestamp);
  const x = (row: Json, index: number) => data.length === 1
    ? margin.left + plotWidth
    : margin.left + (Number(row.timestamp) - firstTimestamp) / timestampSpan * plotWidth;
  const netY = (value: number) => margin.top + (netMaximum - value) / Math.max(1, netMaximum - netMinimum) * plotHeight;
  const averageY = (value: number) => margin.top + (100 - Math.min(100, Math.max(0, value))) / 100 * plotHeight;
  const netPoints = data.map((row: Json, index: number) => `${x(row, index)},${netY(Number(row.cumulative_net_gb || 0))}`).join(" ");
  const averagePoints = data.map((row: Json, index: number) => `${x(row, index)},${averageY(Number(row.average_percent_saved || 0))}`).join(" ");
  const gridValues = Array.from(
    { length: Math.round((netMaximum - netMinimum) / netStep) + 1 },
    (_, index) => netMinimum + netStep * index,
  );
  const percentageTicks = [0, 25, 50, 75, 100];
  const tickIndexes = [...new Set([0, Math.floor((data.length - 1) / 2), data.length - 1])];
  const dateFormat = range === "90"
    ? { day: "numeric", month: "short" } as const
    : { month: "short", year: "numeric" } as const;
  const formatDate = (row: Json) => new Date(row.timestamp).toLocaleDateString(undefined, dateFormat);
  const first = data[0];
  const last = data[data.length - 1];
  const displayedRuns = data.reduce((total: number, row: Json) => total + Number(row.run_count || 1), 0);

  return <figure className="performance-figure">
    <svg className="performance-chart" viewBox={`0 0 ${width} ${height}`} role="img" aria-label="Net savings and average reduction across accepted conversion runs">
      <title>Conversion performance over time</title>
      {gridValues.map((value) => <g key={value}>
        <line x1={margin.left} y1={netY(value)} x2={margin.left + plotWidth} y2={netY(value)} />
        <text x={margin.left - 9} y={netY(value) + 4} textAnchor="end">{Math.round(value)} GB</text>
      </g>)}
      {percentageTicks.map((value) => <text
        key={`percentage-${value}`}
        x={margin.left + plotWidth + 10}
        y={averageY(value) + 4}
      >{value}%</text>)}
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
    </svg>
    <figcaption className="chart-legend">
      <span><i className="net" />Cumulative net savings</span>
      <span><i className="average" />Average reduction of accepted conversions</span>
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
      <div><span>Success rate</span><strong>{Number(health.success_percent || 0).toFixed(1)}%</strong><small>Completed conversion attempts</small></div>
    </section>

    <section className="report-section">
      <header className="report-heading">
        <div><h2>Performance over time</h2><p>Cumulative net savings and average reduction across accepted conversions. Retained originals are excluded.</p></div>
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
            <thead><tr><th>Policy</th><th>Average</th><th>Avg saved</th><th>Success</th></tr></thead>
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
  const latestFinishedAt = dashboard.recent_jobs?.[0]?.finished_at || "";
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
  useEffect(() => {
    if (detailsOpen && latestFinishedAt) setStatistics(null);
  }, [detailsOpen, latestFinishedAt]);
  const toggleDetails = () => {
    const next = !detailsOpen;
    setDetailsOpen(next);
    if (next) setStatistics(null);
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
  if (isLikelyCorruptImage(issue)) return {
    title: "Likely corrupt, truncated or malformed",
    text: "CBZXL could not strictly decode this source image. Recovery can attempt to preserve every decodable pixel in a clean metadata-free image; the original archive remains untouched if recovery or verification fails.",
    recommended: "Attempt image recovery",
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

function isLikelyCorruptImage(issue: Json) {
  return issue.kind === "image_corrupt" || (
    issue.kind === "page_conversion_error" &&
    String(issue.message || "").startsWith("JPEG XL encoding failed") &&
    (!issue.detail || issue.detail === issue.message)
  );
}

function issueDisplayMessage(issue: Json) {
  return isLikelyCorruptImage(issue)
    ? "Likely corrupt, truncated or malformed"
    : issue.message;
}

function IssuePreview({ issue, related = false }: { issue: Json; related?: boolean }) {
  const [failed, setFailed] = useState(false);
  const [source, setSource] = useState("");
  const previewUrl = related ? issue.related_preview_url : issue.preview_url;
  const entryPath = related ? issue.related_entry_path : issue.entry_path;
  useEffect(() => {
    if (!previewUrl) return;
    const controller = new AbortController();
    let objectUrl = "";
    const headers = new Headers();
    if (token()) headers.set("Authorization", `Bearer ${token()}`);
    fetch(previewUrl, { headers, signal: controller.signal })
      .then((response) => {
        if (!response.ok) throw new Error("preview unavailable");
        return response.blob();
      })
      .then((blob) => {
        objectUrl = URL.createObjectURL(blob);
        setSource(objectUrl);
      })
      .catch((error) => { if (error.name !== "AbortError") setFailed(true); });
    return () => {
      controller.abort();
      if (objectUrl) URL.revokeObjectURL(objectUrl);
    };
  }, [previewUrl]);
  if (!previewUrl || failed) return <figure className="issue-preview unavailable"><span>⌁</span><figcaption>{entryPath ? `Preview unavailable · ${entryPath}` : "Archive-level issue"}</figcaption></figure>;
  if (!source) return <figure className="issue-preview unavailable"><span>…</span><figcaption>Loading preview · {entryPath}</figcaption></figure>;
  return <figure className="issue-preview"><img src={source} alt={`Conflicting page ${entryPath}`} onError={() => setFailed(true)} /><figcaption><code>{entryPath}</code></figcaption></figure>;
}

const remedyLabels: Record<string, string> = {
  compatibility: "Compatibility retry",
  recover: "Attempt image recovery",
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
  const [groupNext, setGroupNext] = useState<Record<string, number | null>>({});
  const [groupLoading, setGroupLoading] = useState<string | null>(null);
  const [openGroup, setOpenGroup] = useState<string | null>(null);
  const [open, setOpen] = useState<number | null>(null);
  const [busy, setBusy] = useState<number | null>(null);
  const [diagnostics, setDiagnostics] = useState<Record<number, Json>>({});
  const [diagnosticsLoading, setDiagnosticsLoading] = useState<number | null>(null);
  const fileInputs = useRef<Record<number, HTMLInputElement | null>>({});
  const load = useCallback(() => api("/api/issues/summary").then(setData).catch((error) => notify(error.message, "error")), [notify]);
  useEffect(() => { load(); }, [load]);

  const loadGroup = useCallback(async (groupId: string, append = false) => {
    setGroupLoading(groupId);
    try {
      const cursor = append && groupNext[groupId] ? `&before_id=${groupNext[groupId]}` : "";
      const payload = await api(`/api/issues?group=${encodeURIComponent(groupId)}&limit=100${cursor}`);
      setGroupRows((current) => ({
        ...current,
        [groupId]: append ? [...(current[groupId] || []), ...(payload.rows || [])] : payload.rows || [],
      }));
      setGroupNext((current) => ({ ...current, [groupId]: payload.next || null }));
    } catch (error) {
      notify((error as Error).message, "error");
    } finally {
      setGroupLoading(null);
    }
  }, [groupNext, notify]);

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
    if (action === "recover" && !window.confirm(`Attempt tolerant pixel recovery for ${issue.entry_path}? The original archive will remain unchanged unless the recovered image and rebuilt archive both verify successfully.`)) return;
    if (action === "delete" && !window.confirm(`Delete ${issue.entry_path} from ${archiveName(issue.path)}? The archive name will not change.`)) return;
    if (action === "delete_archive" && !window.confirm(`Move ${archiveName(issue.path)} to the library's .cbzxl-trash folder? It will leave the active library but can still be restored manually.`)) return;
    if (action === "salvage_partial" && !window.confirm(`Rebuild ${archiveName(issue.path)} using only entries that were recovered and fully verified? The truncated archive tail will be omitted.`)) return;
    if (action === "keep_primary" || action === "keep_related") {
      const kept = action === "keep_primary" ? issue.entry_path : issue.related_entry_path;
      const discarded = action === "keep_primary" ? issue.related_entry_path : issue.entry_path;
      if (!window.confirm(`Keep ${kept} and remove ${discarded} from ${archiveName(issue.path)}? The rebuilt archive will be verified before replacement.`)) return;
    }
    setBusy(issue.id);
    try {
      await api(`/api/issues/${issue.id}/action`, { method: "POST", body: JSON.stringify({ action }) });
      notify(action === "dismiss" ? "Dismissed until the next full rescan" : action === "delete_archive" ? "Archive moved to recoverable trash" : action.startsWith("keep_") ? "Selection queued for verified rebuild" : "Repair queued");
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
            {Number(group.count || 0) !== Number(group.archive_count || group.count || 0) &&
              <span className="failure-scope">{Number(group.count || 0).toLocaleString()} failing images across {Number(group.archive_count || 0).toLocaleString()} archives</span>}
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
              remedy === "dismiss" || remedy === "delete_archive" ? false :
              remedy === "rebuild" ? diagnostic?.repairable :
              remedy === "salvage" ? diagnostic?.salvageable :
              remedy === "salvage_partial" ? diagnostic?.partial_salvageable :
              true
            );
            return <div className={`issue ${expanded ? "expanded" : ""}`} key={issue.id}>
              <div className="issue-summary">
                <div>
                  <strong>{archiveName(issue.path)}</strong>
                  {openGroup === "images" && Number(issue.archive_issue_count || 0) > 1 &&
                    <span className="archive-failure-count">{Number(issue.archive_issue_count).toLocaleString()} failing images in this archive</span>}
                  <span className="error-copy">{issueDisplayMessage(issue)}</span>
                  {issue.entry_path && <code>{issue.entry_path}</code>}
                </div>
                <time>{dateTime(issue.updated_at)}</time>
                <div className="issue-actions">
                  {!collision && <button className="button primary" disabled={busy === issue.id || issue.status === "queued"} onClick={() => act(issue, "retry")}>{issue.status === "queued" ? "Queued" : issue.kind === "archive_grew" && !issue.backup_available ? "Repack" : "Retry"}</button>}
                  <button className="button secondary" aria-expanded={expanded} onClick={() => toggleInspect(issue)}>{expanded ? "Close" : "Inspect"}</button>
                  {Boolean(issue.retry_failed) && <button className="button ghost danger" disabled={busy === issue.id || issue.status === "queued"} onClick={() => act(issue, "delete_archive")}>Delete archive</button>}
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
                      <button className={`button block ${["compatibility", "recover", "lossless", "smart_lossy", "rebuild", "salvage", "salvage_partial"].includes(remedy) ? "primary" : remedy === "delete" ? "danger" : "secondary"}`} key={remedy} disabled={busy === issue.id} onClick={() => act(issue, remedy)}>{remedyLabels[remedy] || label(remedy)}</button>)}
                  </div>}
                </div>
              </div>}
            </div>;
          })}
          {groupNext[group.id] && <button className="button secondary" type="button" onClick={() => loadGroup(group.id, true)}>Load more</button>}
        </div>}
      </article>;
    })}
  </div>;
}

function HistoryDetail({ id, notify }: { id: string; notify: (message: string, kind?: "success" | "error") => void }) {
  const [detail, setDetail] = useState<Json | null>(null);
  const [filter, setFilter] = useState("all");
  const [pageDetails, setPageDetails] = useState<Record<number, Json[]>>({});
  useEffect(() => { api(`/api/jobs/${id}/history`).then(setDetail).catch(() => setDetail({ events: [] })); }, [id]);
  if (!detail) return <div className="history-loading" />;
  const job = detail.job || {};
  const counts = detail.outcome_counts || {};
  const entries = detail.history_entries || [];
  const filteredEntries = filter === "all"
    ? entries
    : entries.filter((entry: Json) => entry.outcome === filter);
  const loadMore = async () => {
    if (!detail.next_outcome) return;
    const next = await api(`/api/jobs/${id}/history?before_outcome=${detail.next_outcome}`);
    setDetail({
      ...next,
      history_entries: [...entries, ...(next.history_entries || [])],
    });
  };
  const togglePages = async (entry: Json) => {
    if (pageDetails[entry.id]) {
      setPageDetails((current) => {
        const next = { ...current };
        delete next[entry.id];
        return next;
      });
      return;
    }
    try {
      const value = await api(`/api/jobs/${id}/page-decisions?path=${encodeURIComponent(entry.path)}&limit=500`);
      setPageDetails((current) => ({ ...current, [entry.id]: value.rows || [] }));
    } catch (error) { notify((error as Error).message, "error"); }
  };
  const review = async (entry: Json, action: "reviewed" | "reopen" | "recheck") => {
    if (!entry.archive_id) return;
    try {
      await api(`/api/archives/${entry.archive_id}/review`, { method: "POST", body: JSON.stringify({ action }) });
      notify(action === "recheck" ? "Archive recheck queued" : action === "reviewed" ? "Archive marked reviewed" : "Archive review reopened");
    } catch (error) { notify((error as Error).message, "error"); }
  };
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
            <button className="button ghost" type="button" onClick={() => togglePages(entry)}>{pageDetails[entry.id] ? "Hide pages" : "Page decisions"}</button>
            {entry.archive_id && <button className="button ghost" type="button" onClick={() => review(entry, "reviewed")}>Mark reviewed</button>}
            {entry.archive_id && <button className="button ghost" type="button" onClick={() => review(entry, "recheck")}>Recheck</button>}
          </footer>
          {pageDetails[entry.id] && <div className="event-list">{pageDetails[entry.id].map((page: Json) => <div key={page.id}><i className={page.outcome === "failed" ? "error" : "success"} /><code>{page.entry_path}</code><strong>{label(page.outcome)}</strong>{page.reason && <span>{page.reason}</span>}</div>)}{!pageDetails[entry.id].length && <small>No page-level decisions were recorded for this archive.</small>}</div>}
        </div>
      </article>)}
      {!filteredEntries.length && <div className="history-outcome-empty">No {filter === "all" ? "archive outcomes" : filters.find(([value]) => value === filter)?.[1].toLowerCase()} are available in this history window.</div>}
      {Number(counts[filter] || 0) > filteredEntries.length && <small className="history-outcome-limit">Showing the latest {filteredEntries.length.toLocaleString()} of {Number(counts[filter] || 0).toLocaleString()} matching archives.</small>}
      {detail.next_outcome && <button className="button secondary" type="button" onClick={loadMore}>Load more outcomes</button>}
    </div>
    <details className="history-technical">
      <summary>Technical activity</summary>
      <div className="event-list">{(detail.events || []).slice(0, 20).map((event: Json) => <div key={event.id}><i className={event.level} /><time>{dateTime(event.created_at)}</time><strong>{event.message}</strong>{event.archive_path && <code>{event.archive_path}</code>}</div>)}</div>
    </details>
  </div>;
}

function History({ notify }: { notify: (message: string, kind?: "success" | "error") => void }) {
  const [rows, setRows] = useState<Json[]>([]);
  const [next, setNext] = useState<string | null>(null);
  const [open, setOpen] = useState<string | null>(null);
  const load = useCallback((append = false, before?: string) => {
    const cursor = append && before ? `&before_id=${encodeURIComponent(before)}` : "";
    api(`/api/jobs?limit=100${cursor}`).then((value) => {
      setRows((current) => append ? [...current, ...(value.rows || [])] : value.rows || []);
      setNext(value.next || null);
    }).catch((error) => notify(error.message, "error"));
  }, [notify]);
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
      {open === row.id && <HistoryDetail id={row.id} notify={notify} />}
    </article>;
    })}
    {next && <button className="button secondary" type="button" onClick={() => load(true, next)}>Older jobs</button>}
  </div>;
}

function Settings({ bootstrap, reload, notify }: {
  bootstrap: Json;
  reload: () => void;
  notify: (message: string, kind?: "success" | "error") => void;
}) {
  const [schedules, setSchedules] = useState<Json[]>([]);
  const cpuLimit = Math.max(1, Number(bootstrap.capabilities?.cpu_limit || 1));
  const availableCpuOptions = cpuOptions(cpuLimit);
  const [newOpen, setNewOpen] = useState(false);
  const [editingSchedule, setEditingSchedule] = useState<string | null>(null);
  const profiles = bootstrap.profiles || [];
  const defaultProfile = profiles.find((row: Json) => row.is_default) || profiles[0] || {};
  const [form, setForm] = useState({
    name: "Library maintenance",
    cadence: "daily",
    hour: 4,
    minute: 0,
    weekday: 0,
    day_of_month: 1,
    interval_value: 24,
    interval_unit: "hours",
    library_id: bootstrap.libraries?.[0]?.id || "",
    profile_id: defaultProfile.id || "",
  });
  const [policyOpen, setPolicyOpen] = useState(false);
  const [policyBusy, setPolicyBusy] = useState(false);
  const [policyForm, setPolicyForm] = useState<Json>({});
  const [scheduleProfiles, setScheduleProfiles] = useState<Record<string, string>>({});
  const [libraryForm, setLibraryForm] = useState<Json | null>(null);
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
      threads: Math.min(cpuLimit, Number(options.threads ?? 4)),
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
  const saveLibrary = async (event: FormEvent) => {
    event.preventDefault();
    if (!libraryForm) return;
    try {
      await api(libraryForm.id ? `/api/libraries/${libraryForm.id}` : "/api/libraries", {
        method: libraryForm.id ? "PUT" : "POST",
        body: JSON.stringify({
          name: libraryForm.name,
          root_path: libraryForm.root_path,
          enabled: Boolean(libraryForm.enabled),
        }),
      });
      notify(libraryForm.id ? "Library updated" : "Library added");
      setLibraryForm(null);
      await reload();
    } catch (error) { notify((error as Error).message, "error"); }
  };
  const removeLibrary = async (library: Json) => {
    if (!window.confirm(`Delete ${library.name}? Libraries with history cannot be deleted.`)) return;
    try {
      await api(`/api/libraries/${library.id}`, { method: "DELETE" });
      notify("Library deleted");
      await reload();
    } catch (error) { notify((error as Error).message, "error"); }
  };

  const save = async (event: FormEvent) => {
    event.preventDefault();
    try {
      await api(editingSchedule ? `/api/schedules/${editingSchedule}` : "/api/schedules", {
        method: editingSchedule ? "PUT" : "POST",
        body: JSON.stringify({
          ...form,
          enabled: true,
          library_id: form.library_id || bootstrap.libraries?.[0]?.id,
          profile_id: form.profile_id || defaultProfile.id,
        }),
      });
      notify(editingSchedule ? "Schedule updated" : "Schedule created");
      setNewOpen(false);
      setEditingSchedule(null);
      load();
      reload();
    } catch (error) { notify((error as Error).message, "error"); }
  };
  const editSchedule = (row: Json) => {
    setForm({
      name: row.name,
      cadence: row.cadence,
      hour: Number(row.hour || 0),
      minute: Number(row.minute || 0),
      weekday: Number(row.weekday || 0),
      day_of_month: Number(row.day_of_month ?? 1),
      interval_value: Number(row.interval_value || 24),
      interval_unit: row.interval_unit || "hours",
      library_id: row.library_id,
      profile_id: row.profile_id,
    });
    setEditingSchedule(row.id);
    setNewOpen(true);
  };
  const newSchedule = () => {
    if (newOpen && !editingSchedule) {
      setNewOpen(false);
      return;
    }
    setForm({
      name: "Library maintenance",
      cadence: "daily",
      hour: 4,
      minute: 0,
      weekday: 0,
      day_of_month: 1,
      interval_value: 24,
      interval_unit: "hours",
      library_id: bootstrap.libraries?.[0]?.id || "",
      profile_id: defaultProfile.id || "",
    });
    setEditingSchedule(null);
    setNewOpen(true);
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
      <header><div><h2>Libraries</h2><p>Manage container paths without changing the retained interface.</p></div><button className="button primary" type="button" onClick={() => setLibraryForm({ name: "", root_path: "/data", enabled: true })}>Add library</button></header>
      <div className="policy-list">{(bootstrap.libraries || []).map((library: Json) => <article key={library.id}>
        <div><div className="policy-name"><strong>{library.name}</strong>{library.enabled && <span className="status-label">Enabled</span>}</div><span><code>{library.root_path}</code></span></div>
        <div className="policy-actions"><button className="button secondary" type="button" onClick={() => setLibraryForm({ ...library })}>Edit</button><button className="button ghost danger" type="button" onClick={() => removeLibrary(library)}>Delete</button></div>
      </article>)}</div>
      {libraryForm && <form className="policy-editor" onSubmit={saveLibrary}>
        <header><div><h3>{libraryForm.id ? "Edit library" : "Add library"}</h3><p>Paths are resolved inside the container.</p></div><button className="button ghost" type="button" onClick={() => setLibraryForm(null)}>Close</button></header>
        <div className="policy-fields"><label><span>Name</span><input value={libraryForm.name || ""} onChange={(event) => setLibraryForm({ ...libraryForm, name: event.target.value })} required /></label><label><span>Container path</span><input value={libraryForm.root_path || ""} onChange={(event) => setLibraryForm({ ...libraryForm, root_path: event.target.value })} required /></label></div>
        <div className="policy-switches"><label className="switch"><input type="checkbox" checked={Boolean(libraryForm.enabled)} onChange={(event) => setLibraryForm({ ...libraryForm, enabled: event.target.checked })} /><i /><span>Enabled</span></label></div>
        <footer><button className="button ghost" type="button" onClick={() => setLibraryForm(null)}>Cancel</button><button className="button primary" type="submit">Save library</button></footer>
      </form>}
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
          <label><span>CPU budget</span><select value={policyForm.threads ?? 1} onChange={(event) => setPolicyForm({ ...policyForm, threads: Number(event.target.value) })}>{availableCpuOptions.map((value) => <option key={value} value={value}>{value} thread{value === 1 ? "" : "s"}</option>)}</select></label>
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
      <header><div><h2>Schedules</h2><p>Recurring conversions use the same verified pipeline.</p></div><button className="button primary" onClick={newSchedule}>{newOpen && !editingSchedule ? "Close" : "New schedule"}</button></header>
      {newOpen && <form className="schedule-form" onSubmit={save}>
        <label><span>Name</span><input value={form.name} onChange={(event) => setForm({ ...form, name: event.target.value })} required /></label>
        <label><span>Cadence</span><select value={form.cadence} onChange={(event) => setForm({ ...form, cadence: event.target.value })}><option value="daily">Daily</option><option value="weekly">Weekly</option><option value="monthly">Monthly</option><option value="interval">Interval</option></select></label>
        {form.cadence !== "interval" && <>
        <label><span>Hour</span><select value={form.hour} onChange={(event) => setForm({ ...form, hour: Number(event.target.value) })}>{Array.from({ length: 24 }, (_, hour) => <option key={hour} value={hour}>{new Date(2000, 0, 1, hour).toLocaleTimeString([], { hour: "numeric" })}</option>)}</select></label>
        <label><span>Minute</span><input type="number" min="0" max="59" value={form.minute} onChange={(event) => setForm({ ...form, minute: Number(event.target.value) })} /></label>
        </>}
        {form.cadence === "weekly" && <label><span>Weekday</span><select value={form.weekday} onChange={(event) => setForm({ ...form, weekday: Number(event.target.value) })}>{["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"].map((day, index) => <option key={day} value={index}>{day}</option>)}</select></label>}
        {form.cadence === "monthly" && <label><span>Day</span><input type="number" min="-1" max="31" value={form.day_of_month} onChange={(event) => setForm({ ...form, day_of_month: Number(event.target.value) })} /><small>Use -1 for the last day.</small></label>}
        {form.cadence === "interval" && <><label><span>Every</span><input type="number" min="1" value={form.interval_value} onChange={(event) => setForm({ ...form, interval_value: Number(event.target.value) })} /></label><label><span>Unit</span><select value={form.interval_unit} onChange={(event) => setForm({ ...form, interval_unit: event.target.value })}><option value="hours">Hours</option><option value="days">Days</option></select></label></>}
        {bootstrap.libraries?.length > 1 && <label><span>Library</span><select value={form.library_id} onChange={(event) => setForm({ ...form, library_id: event.target.value })}>{bootstrap.libraries.map((library: Json) => <option key={library.id} value={library.id}>{library.name}</option>)}</select></label>}
        <label><span>Conversion policy</span><select value={form.profile_id} onChange={(event) => setForm({ ...form, profile_id: event.target.value })}>{profiles.map((profile: Json) => <option key={profile.id} value={profile.id}>{profile.name}</option>)}</select></label>
        <button className="button ghost" type="button" onClick={() => { setNewOpen(false); setEditingSchedule(null); }}>Cancel</button><button className="button primary" type="submit">{editingSchedule ? "Update schedule" : "Save schedule"}</button>
      </form>}
      <div className="schedule-list">{schedules.map((row) => <article key={row.id} className={!row.enabled ? "disabled" : ""}>
        <div><strong>{row.name}</strong><span>{row.description} · {row.enabled ? `next ${dateTime(row.next_run_at)}` : "paused"}</span></div>
        <label className="schedule-policy"><span>Conversion policy</span><select value={scheduleProfiles[row.id] || row.profile_id} onChange={(event) => setScheduleProfiles({ ...scheduleProfiles, [row.id]: event.target.value })}>{profiles.map((profile: Json) => <option key={profile.id} value={profile.id}>{profile.name}</option>)}</select></label>
        <div><button className="button secondary" onClick={() => editSchedule(row)}>Edit</button><button className="button secondary" onClick={() => saveSchedulePolicy(row)} disabled={(scheduleProfiles[row.id] || row.profile_id) === row.profile_id}>Save policy</button><button className="button secondary" onClick={() => toggle(row)}>{row.enabled ? "Pause" : "Enable"}</button><button className="button ghost danger" onClick={() => remove(row)}>Delete</button></div>
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
  const [authenticationRequired, setAuthenticationRequired] = useState(false);
  const [credential, setCredential] = useState("");
  const [authenticationError, setAuthenticationError] = useState("");
  const [dashboardError, setDashboardError] = useState("");
  const loadBootstrap = useCallback(() => api("/api/bootstrap").then((value) => {
    setBootstrap(value);
    setAuthenticationRequired(false);
    setAuthenticationError("");
  }), []);
  const loadDashboard = useCallback(() => api("/api/dashboard").then((value) => {
    setDashboard(value);
    setDashboardError("");
  }).catch((error) => setDashboardError(error.message)), []);
  useEffect(() => {
    loadBootstrap().catch((error) => {
      if (error.message === "Authentication required") setAuthenticationRequired(true);
      else setAuthenticationError(error.message);
    });
  }, [loadBootstrap]);
  useEffect(() => {
    let stopped = false;
    let timer: number | undefined;
    let controller: AbortController | undefined;
    const schedule = (delay: number) => {
      if (!stopped && document.visibilityState === "visible") {
        timer = window.setTimeout(load, delay);
      }
    };
    const load = async () => {
      if (stopped || document.visibilityState !== "visible") return;
      controller = new AbortController();
      try {
        const payload = await api("/api/dashboard", { signal: controller.signal });
        if (stopped) return;
        setDashboard(payload);
        setDashboardError("");
        const converting = Boolean(payload.active_jobs?.length);
        schedule(view === "overview" ? (converting ? 5000 : 12000) : (converting ? 12000 : 30000));
      } catch (error) {
        if (!stopped && (error as Error).name !== "AbortError") {
          setDashboardError((error as Error).message);
          schedule(15000);
        }
      }
    };
    const visibilityChanged = () => {
      if (document.visibilityState === "visible") {
        if (timer) window.clearTimeout(timer);
        load();
      } else {
        if (timer) window.clearTimeout(timer);
        controller?.abort();
      }
    };
    load();
    document.addEventListener("visibilitychange", visibilityChanged);
    return () => {
      stopped = true;
      if (timer) window.clearTimeout(timer);
      controller?.abort();
      document.removeEventListener("visibilitychange", visibilityChanged);
    };
  }, [view]);
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
  const runtimeUnavailable = bootstrap.conversion_runtime_status === "unavailable";
  const nav = useMemo(() => [
    { id: "overview" as View, label: "Overview" },
    { id: "attention" as View, label: "Attention", count: attention },
    { id: "history" as View, label: "History" },
    { id: "settings" as View, label: "Settings" },
  ], [attention]);
  if (authenticationRequired) return <div className="app-shell"><main className="app-content"><form className="authentication-card" onSubmit={async (event) => {
    event.preventDefault();
    setToken(credential.trim());
    try { await loadBootstrap(); } catch (error) { setAuthenticationRequired(true); setAuthenticationError((error as Error).message); }
  }}><Logo /><h1>Connect to CBZXL</h1><p>Enter the API token configured for this server.</p><label><span>API token</span><input type="password" autoComplete="current-password" value={credential} onChange={(event) => setCredential(event.target.value)} required /></label>{authenticationError && <strong>{authenticationError}</strong>}<button className="button primary" type="submit">Continue</button></form></main></div>;
  return <div className="app-shell">
    <header className="app-header">
      <button className="brand" onClick={() => navigate("overview")}><span><Logo /></span><strong>CBZXL</strong></button>
      <nav aria-label="Primary navigation">{nav.map((item) => <button key={item.id} className={view === item.id ? "active" : ""} onClick={() => navigate(item.id)}>{item.label}{item.count ? <b>{item.count.toLocaleString()}</b> : null}</button>)}</nav>
      <span className={`service-state ${unavailable || runtimeUnavailable ? "error" : active ? "busy" : ""}`}><i />{unavailable ? "Database unavailable" : runtimeUnavailable ? "Conversion unavailable" : active ? "Converting" : "Ready"}</span>
    </header>
    <main className="app-content">
      {dashboardError && <section className="database-alert" role="status"><strong>Live updates are temporarily unavailable.</strong><code>{dashboardError}</code></section>}
      {unavailable && <section className="database-alert" role="alert"><strong>The web interface is online, but the database could not be opened.</strong><code>{bootstrap.database_error}</code></section>}
      {!unavailable && runtimeUnavailable && <section className="database-alert" role="alert"><strong>Lossless conversion is unavailable. New conversion jobs are blocked.</strong><code>{bootstrap.conversion_runtime_error}</code></section>}
      {!unavailable && view === "overview" && <Overview dashboard={dashboard} bootstrap={bootstrap} refresh={loadDashboard} notify={notify} />}
      {!unavailable && view === "attention" && <Attention refreshDashboard={loadDashboard} notify={notify} />}
      {!unavailable && view === "history" && <History notify={notify} />}
      {!unavailable && view === "settings" && <Settings bootstrap={bootstrap} reload={loadBootstrap} notify={notify} />}
    </main>
    <Toast value={toast} />
  </div>;
}
