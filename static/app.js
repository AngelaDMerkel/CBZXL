const state = {
  summary: null,
  progress: null,
  reviewKind: "grew",
  progressTimer: null,
  summaryTimer: null,
};

const $ = (id) => document.getElementById(id);
const PIE_COLORS = ["#2f8f55", "#357fa4", "#b67818", "#bf4f55", "#6d5cae", "#2c9b84", "#8f5c3b", "#6d7b84", "#c06d92", "#6a8f3a", "#4e79a7", "#a5673f"];

function escapeHtml(value) {
  return String(value ?? "").replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;",
  }[char]));
}

function formatGb(value) {
  const n = Number(value || 0);
  return `${n.toLocaleString(undefined, { maximumFractionDigits: 3 })} GB`;
}

function formatMb(value) {
  const n = Number(value || 0);
  return `${n.toLocaleString(undefined, { maximumFractionDigits: 2 })} MB`;
}

function formatPercent(value) {
  if (value === null || value === undefined) return "0%";
  return `${Number(value).toLocaleString(undefined, { maximumFractionDigits: 1 })}%`;
}

function shortDate(value) {
  if (!value) return "No runs";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit" });
}

function formatDateTime(value) {
  if (!value) return "Not started";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString(undefined, { month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function formatDuration(seconds) {
  const total = Math.max(0, Number(seconds || 0));
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const secs = Math.floor(total % 60);
  if (hours) return `${hours}h ${minutes}m`;
  if (minutes) return `${minutes}m ${secs}s`;
  return `${secs}s`;
}

function runStatusLabel(status, running) {
  if (running) return "Running";
  if (status === "finished") return "Finished";
  if (status === "finished_with_errors") return "Finished with errors";
  if (status === "failed") return "Failed";
  if (status === "stopping") return "Stopping";
  if (status === "starting") return "Starting";
  return status ? status.replaceAll("_", " ") : "Idle";
}

function setText(id, value) {
  $(id).textContent = value;
}

function renderBars(container, rows, labelKey, valueKey, options = {}) {
  const max = Math.max(1, ...rows.map((row) => Math.abs(Number(row[valueKey] || 0))));
  container.innerHTML = "";
  if (!rows.length) {
    container.innerHTML = `<div class="empty">No data yet.</div>`;
    return;
  }
  rows.forEach((row) => {
    const value = Number(row[valueKey] || 0);
    const width = Math.max(2, Math.round((Math.abs(value) / max) * 100));
    const fillClass = value < 0 ? "negative" : options.neutral ? "neutral" : "";
    const label = row[labelKey] || "";
    const formatted = options.format ? options.format(value) : value.toLocaleString();
    const el = document.createElement("div");
    el.className = "bar-row";
    el.innerHTML = `
      <span title="${escapeHtml(label)}">${escapeHtml(label)}</span>
      <div class="track"><span class="fill ${fillClass}" style="width:${width}%"></span></div>
      <strong class="${value < 0 ? "negative-text" : "positive-text"}">${escapeHtml(formatted)}</strong>
    `;
    container.appendChild(el);
  });
}

function renderPie(container, rows, labelKey, valueKey, options = {}) {
  const cleanRows = rows
    .map((row) => ({
      label: row[labelKey] || "Unknown",
      value: Number(row[valueKey] || 0),
      raw: row,
    }))
    .filter((row) => Number.isFinite(row.value) && row.value !== 0);
  const total = cleanRows.reduce((sum, row) => sum + Math.abs(row.value), 0);
  container.innerHTML = "";
  if (!cleanRows.length || total <= 0) {
    container.innerHTML = `<div class="empty">No data yet.</div>`;
    return;
  }

  let offset = 0;
  const segments = cleanRows.map((row, index) => {
    const value = Math.abs(row.value);
    const dash = (value / total) * 100;
    const segment = `
      <circle class="pie-segment" cx="50" cy="50" r="42"
        pathLength="100"
        stroke="${PIE_COLORS[index % PIE_COLORS.length]}"
        stroke-dasharray="${dash.toFixed(3)} ${Math.max(0, 100 - dash).toFixed(3)}"
        stroke-dashoffset="${(-offset).toFixed(3)}"></circle>
    `;
    offset += dash;
    return segment;
  }).join("");
  const totalLabel = options.totalLabel || (options.format ? options.format(total) : total.toLocaleString());

  const chart = document.createElement("div");
  chart.className = "pie-viz";
  chart.innerHTML = `
    <div class="pie-ring">
      <svg class="pie-svg" viewBox="0 0 100 100" aria-hidden="true">
        <circle class="pie-base" cx="50" cy="50" r="42" pathLength="100"></circle>
        ${segments}
      </svg>
      <div class="pie-center"><strong>${escapeHtml(totalLabel)}</strong><span>Total</span></div>
    </div>
    <div class="pie-legend"></div>
  `;

  const legend = chart.querySelector(".pie-legend");
  cleanRows.forEach((row, index) => {
    const percent = (Math.abs(row.value) / total) * 100;
    const formatted = options.format ? options.format(row.value) : row.value.toLocaleString();
    const item = document.createElement("div");
    item.className = "pie-item";
    item.innerHTML = `
      <span class="swatch" style="background:${PIE_COLORS[index % PIE_COLORS.length]}"></span>
      <span title="${escapeHtml(row.label)}">${escapeHtml(row.label)}</span>
      <strong>${escapeHtml(formatted)}</strong>
      <em>${percent.toLocaleString(undefined, { maximumFractionDigits: 1 })}%</em>
    `;
    legend.appendChild(item);
  });
  container.appendChild(chart);
}

function tableRows(container, rows, kind) {
  container.innerHTML = "";
  const colspan = kind === "loss" ? 3 : 4;
  if (!rows.length) {
    container.innerHTML = `<tr><td colspan="${colspan}">No data yet.</td></tr>`;
    return;
  }
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    const gb = formatGb(row.gb);
    const percent = formatPercent(row.percent_saved);
    const mode = row.conversion_mode || "Unknown";
    if (kind === "loss") {
      tr.innerHTML = `
        <td class="path" title="${escapeHtml(row.path)}">${escapeHtml(row.path)}</td>
        <td class="numeric negative-text">${gb}</td>
        <td class="numeric negative-text">${percent}</td>
      `;
    } else {
      tr.innerHTML = `
        <td class="path" title="${escapeHtml(row.path)}">${escapeHtml(row.path)}</td>
        <td class="numeric positive-text">${gb}</td>
        <td class="numeric">${percent}</td>
        <td>${escapeHtml(mode)}</td>
      `;
    }
    container.appendChild(tr);
  });
}

function renderSummary(data) {
  state.summary = data;
  const totals = data.totals || {};
  const review = data.review_counts || {};
  const reviewTotal = Number(review.grew_open || 0) + Number(review.failed_open || 0);

  setText("runState", data.running ? `Running ${data.current_run_id || ""}` : "Idle");
  document.body.classList.toggle("is-running", Boolean(data.running));
  setText("netSaved", formatGb(totals.net_gb));
  setText("grossSaved", `${formatGb(totals.saved_gb)} saved, ${formatGb(totals.increased_gb)} increased`);
  setText("reviewTotal", reviewTotal.toLocaleString());
  setText("reviewSplit", `${Number(review.grew_open || 0).toLocaleString()} grew, ${Number(review.failed_open || 0).toLocaleString()} failed`);
  setText("railReview", reviewTotal ? `${reviewTotal.toLocaleString()} review item${reviewTotal === 1 ? "" : "s"}` : "No review items");
  setText("archiveCount", Number(totals.archive_count || 0).toLocaleString());
  setText("failedCount", Number(review.failed_open || 0).toLocaleString());
  setText("formatHint", `${(data.formats || []).length} format groups`);

  $("scanBtn").disabled = data.running;
  $("stopBtn").disabled = !data.running;

  renderBars($("runChart"), (data.run_history || []).slice().reverse().map((run) => ({
    label: shortDate(run.started_at),
    value: run.net_gb,
  })), "label", "value", { format: formatGb });

  renderPie($("distribution"), data.distribution || [], "bucket", "count");
  renderPie($("formats"), data.formats || [], "format", "count");
  renderPie($("compatibility"), (data.compatibility || []).map((row) => ({
    mode: row.mode,
    count: row.count,
  })), "mode", "count");
  renderPie($("sources"), (data.sources || []).map((row) => ({
    source: row.source,
    value: row.net_gb,
  })), "source", "value", { format: formatGb, totalLabel: formatGb((data.sources || []).reduce((sum, row) => sum + Math.abs(Number(row.net_gb || 0)), 0)) });

  tableRows($("topSaved"), data.top_saved || [], "win");
  tableRows($("topIncreased"), data.top_increased || [], "loss");
}

function renderProgress(data) {
  state.progress = data;
  const run = data.run || {};
  const total = Number(run.archives_found || 0);
  const completed = Number(run.completed || 0);
  const percent = Number(run.percent || 0);
  const status = run.status || (data.running ? "running" : "idle");
  const statusLabel = runStatusLabel(status, data.running);
  const remaining = Number(run.remaining || 0);

  setText("runMode", `${statusLabel}${run.mode ? ` · ${run.mode}` : ""}`);
  setText("progressPercent", `${percent.toLocaleString(undefined, { maximumFractionDigits: 1 })}%`);
  setText("progressCounts", total ? `${completed.toLocaleString()} of ${total.toLocaleString()} archives complete` : (data.running ? "Finding archives..." : "No scan run recorded yet"));
  $("progressFill").style.width = `${Math.max(0, Math.min(100, percent))}%`;
  setText("currentArchive", data.current_archive || (data.running ? "Preparing the next archive..." : "No archive currently processing."));
  setText("runStarted", formatDateTime(run.started_at));
  setText("runElapsed", formatDuration(run.elapsed_seconds));
  setText("runRemaining", total ? remaining.toLocaleString() : "Unknown");
  setText("runUpdated", new Date().toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit" }));
  setText("runProcessed", Number(run.processed || 0).toLocaleString());
  setText("runSkipped", Number(run.skipped || 0).toLocaleString());
  setText("runFailed", Number(run.failed || 0).toLocaleString());
  setText("runNet", formatGb(run.net_gb));
  renderRunBanner(data, run, statusLabel, completed, total);

  const activity = $("activity");
  const events = data.events || [];
  if (!events.length) {
    activity.innerHTML = `<div class="empty">No run activity yet.</div>`;
    return;
  }
  activity.innerHTML = events.slice(-18).reverse().map((event) => `
    <div class="activity-item ${event.level}">
      <span></span>
      <p>${escapeHtml(event.message)}</p>
    </div>
  `).join("");
}

function renderRunBanner(data, run, statusLabel, completed, total) {
  const banner = $("runBanner");
  banner.classList.remove("idle", "running", "success", "warning", "error");
  let bannerClass = "idle";
  let title = "No scan is running";
  let detail = "Start a scan to see live progress here.";

  if (data.running) {
    bannerClass = "running";
    title = total ? `Scan running: ${completed.toLocaleString()} of ${total.toLocaleString()} archives` : "Scan running: discovering archives";
    detail = run.mode ? `${run.mode} scan is active. This panel updates every few seconds.` : "A scan is active. This panel updates every few seconds.";
  } else if (run.status === "finished") {
    bannerClass = "success";
    title = "Last scan finished";
    detail = `${completed.toLocaleString()} archive${completed === 1 ? "" : "s"} completed.`;
  } else if (run.status === "finished_with_errors") {
    bannerClass = "warning";
    title = "Last scan finished with errors";
    detail = `${Number(run.failed || 0).toLocaleString()} archive${Number(run.failed || 0) === 1 ? "" : "s"} failed.`;
  } else if (run.status === "failed") {
    bannerClass = "error";
    title = "Last scan failed";
    detail = "Open the activity panel for the most recent error.";
  } else if (run.status === "stopping") {
    bannerClass = "warning";
    title = "Scan is stopping";
    detail = "The worker has been asked to stop and may take a moment to exit.";
  } else if (run.id) {
    title = `Last scan: ${statusLabel}`;
    detail = run.finished_at ? `Finished ${formatDateTime(run.finished_at)}.` : "No active worker is reporting progress.";
  }

  banner.classList.add(bannerClass);
  setText("runBannerTitle", title);
  setText("runBannerDetail", detail);
}

function renderReview(rows) {
  const tbody = $("reviewRows");
  tbody.innerHTML = "";
  if (!rows.length) {
    tbody.innerHTML = `<tr><td colspan="5">Nothing needs review in this queue.</td></tr>`;
    return;
  }
  rows.forEach((row) => {
    const tr = document.createElement("tr");
    const change = state.reviewKind === "grew" ? formatMb(Math.abs(row.change_mb || 0)) : "Failed";
    const changeClass = state.reviewKind === "grew" ? "negative-text" : "warning-text";
    const mode = row.error_message || row.conversion_mode || row.dominant_type || "Unknown";
    tr.innerHTML = `
      <td class="path" title="${escapeHtml(row.path)}">${escapeHtml(row.path)}</td>
      <td class="numeric ${changeClass}">${escapeHtml(change)}</td>
      <td class="numeric">${formatPercent(row.percent_saved)}</td>
      <td class="mode-cell" title="${escapeHtml(mode)}">${escapeHtml(mode)}</td>
      <td class="row-actions">
        <button data-action="reviewed" data-path="${escapeHtml(row.path)}">Done</button>
        <button data-action="queue_recheck" data-path="${escapeHtml(row.path)}">Queue</button>
      </td>
    `;
    tbody.appendChild(tr);
  });
}

async function loadSummary() {
  const response = await fetch("/api/summary");
  if (!response.ok) throw new Error("Failed to load summary");
  renderSummary(await response.json());
}

async function loadProgress() {
  const response = await fetch("/api/progress");
  if (!response.ok) return;
  renderProgress(await response.json());
}

async function loadReview(kind = state.reviewKind) {
  state.reviewKind = kind;
  $("reviewGrewBtn").classList.toggle("active", kind === "grew");
  $("reviewFailedBtn").classList.toggle("active", kind === "failed");
  const response = await fetch(`/api/review?kind=${encodeURIComponent(kind)}&limit=80`);
  if (!response.ok) return;
  const data = await response.json();
  renderReview(data.rows || []);
}

async function reviewAction(path, action) {
  const response = await fetch("/api/review/action", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ path, action }),
  });
  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    alert(err.detail || "Action could not be completed");
    return;
  }
  await Promise.all([loadSummary(), loadReview()]);
}

function numberOrNull(id) {
  const value = $(id).value;
  return value === "" ? null : Number(value);
}

function scanPayload() {
  return {
    dry_run: $("dryRun").checked,
    backup: $("backup").checked,
    recheck_all: $("recheckAll").checked,
    verbose: $("verbose").checked,
    no_convert: $("noConvert").checked,
    no_flatten: $("noFlatten").checked,
    delete_empty: $("deleteEmpty").checked,
    smart_distance: $("smartDistance").checked,
    effort: Number($("effort").value),
    threads: Number($("threads").value),
    distance: numberOrNull("distance"),
    smart_mb: Number($("smartMb").value),
    smart_mp: Number($("smartMp").value),
  };
}

async function startScan() {
  const response = await fetch("/api/scan", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(scanPayload()),
  });
  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    alert(err.detail || "Scan could not start");
  }
  await Promise.all([loadSummary(), loadProgress()]);
}

async function stopScan() {
  await fetch("/api/stop", { method: "POST" });
  await Promise.all([loadSummary(), loadProgress()]);
}

async function loadSchedule() {
  const response = await fetch("/api/settings/schedule");
  if (!response.ok) return;
  const data = await response.json();
  $("scheduleEnabled").checked = Boolean(data.enabled);
  $("scheduleHour").value = data.hour ?? 4;
  $("scheduleMinute").value = data.minute ?? 0;
  setText("scheduleStatus", data.enabled ? `Daily at ${String(data.hour).padStart(2, "0")}:${String(data.minute).padStart(2, "0")}` : "Paused");
}

async function saveSchedule() {
  const payload = {
    enabled: $("scheduleEnabled").checked,
    hour: Number($("scheduleHour").value),
    minute: Number($("scheduleMinute").value),
  };
  const response = await fetch("/api/settings/schedule", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!response.ok) {
    const err = await response.json().catch(() => ({}));
    alert(err.detail || "Schedule could not be saved");
    return;
  }
  await loadSchedule();
}

function wireEvents() {
  $("scanBtn").addEventListener("click", startScan);
  $("stopBtn").addEventListener("click", stopScan);
  $("refreshBtn").addEventListener("click", () => {
    loadSummary();
    loadProgress();
    loadReview();
  });
  $("saveScheduleBtn").addEventListener("click", saveSchedule);
  document.querySelectorAll("[data-review-kind]").forEach((button) => {
    button.addEventListener("click", () => loadReview(button.dataset.reviewKind));
  });
  $("reviewRows").addEventListener("click", (event) => {
    const button = event.target.closest("button[data-action]");
    if (!button) return;
    reviewAction(button.dataset.path, button.dataset.action);
  });
}

async function boot() {
  wireEvents();
  await Promise.all([loadSchedule(), loadSummary(), loadProgress(), loadReview()]);
  state.summaryTimer = setInterval(loadSummary, 5000);
  state.progressTimer = setInterval(loadProgress, 2000);
}

boot().catch((error) => {
  setText("runState", error.message);
});
