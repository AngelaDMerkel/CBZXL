import type { Json } from "../types";

const GB = 1024 ** 3;
const TOKEN_KEY = "cbzxl-api-token";

export function token() {
  return localStorage.getItem(TOKEN_KEY) || "";
}

export function setToken(value: string) {
  if (value) {
    localStorage.setItem(TOKEN_KEY, value);
  } else {
    localStorage.removeItem(TOKEN_KEY);
  }
}

async function request<T = Json>(path: string, options: RequestInit = {}): Promise<T> {
  const headers = new Headers(options.headers);
  const credential = token();
  if (credential) headers.set("Authorization", `Bearer ${credential}`);
  if (typeof options.body === "string" && !headers.has("Content-Type")) headers.set("Content-Type", "application/json");
  const response = await fetch(`/api/v2${path}`, { ...options, headers });
  if (response.status === 401) throw new Error("Authentication required");
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.message || payload.detail || `Request failed (${response.status})`);
  }
  return response.status === 204 ? ({} as T) : response.json();
}

function optionsFromPolicy(policy: Json = {}): Json {
  const strategy = policy.strategy || "lossless";
  const scope = policy.scope || "unprocessed";
  return {
    effort: Number(policy.effort ?? 10),
    threads: Number(policy.threads ?? 4),
    backup: Boolean(policy.backup),
    smart_distance: strategy === "smart",
    distance: ["smart", "lossy"].includes(strategy) ? Number(policy.distance ?? .5) : null,
    smart_mb: Number(policy.smart_mb ?? 10),
    smart_mp: Number(policy.smart_mp ?? 5),
    no_convert: ["recheck", "audit"].includes(strategy),
    integrity_audit: strategy === "audit",
    recheck_all: strategy === "audit" || scope === "library",
    failed_only: strategy !== "audit" && scope === "failed",
  };
}

function policyFromOptions(options: Json = {}): Json {
  const strategy = options.integrity_audit ? "audit" : options.no_convert ? "recheck" : options.smart_distance ? "smart" : options.distance !== null && options.distance !== undefined ? "lossy" : "lossless";
  const scope = options.failed_only ? "failed" : options.recheck_all ? "library" : "unprocessed";
  return {
    strategy,
    scope,
    effort: Number(options.effort ?? 10),
    threads: Number(options.threads ?? 4),
    distance: ["smart", "lossy"].includes(strategy) ? Number(options.distance ?? .5) : null,
    smart_mb: Number(options.smart_mb ?? 10),
    smart_mp: Number(options.smart_mp ?? 5),
    backup: Boolean(options.backup),
  };
}

function mapProfile(row: Json): Json {
  return { ...row, is_default: Boolean(row.is_default), options: optionsFromPolicy(row.policy) };
}

function elapsed(row: Json): number {
  const start = Date.parse(row.started_at || row.queued_at || "");
  const end = Date.parse(row.finished_at || new Date().toISOString());
  return Number.isFinite(start) && Number.isFinite(end) ? Math.max(0, (end - start) / 1000) : 0;
}

function mapJob(row: Json): Json {
  const completed = Number(row.processed || 0) + Number(row.skipped || 0) + Number(row.failed || 0) + Number(row.deleted || 0);
  const total = Number(row.archives_found || 0);
  const percent = total ? completed / total * 100 : 0;
  return { ...row, completed, percent, smooth_percent: percent, elapsed_seconds: elapsed(row), net_gb: Number(row.net_bytes_saved || 0) / GB };
}

function scheduleDescription(row: Json): string {
  const values = row.cadence_options || {};
  if (row.cadence === "interval") return `Every ${values.value || 1} ${values.unit || "hours"}`;
  const time = `${String(values.hour ?? 0).padStart(2, "0")}:${String(values.minute ?? 0).padStart(2, "0")}`;
  if (row.cadence === "weekly") return `Weekly · day ${Number(values.weekday || 0) + 1} · ${time}`;
  if (row.cadence === "monthly") return `Monthly · day ${values.day ?? 1} · ${time}`;
  return `Daily · ${time}`;
}

function mapSchedule(row: Json): Json {
  const values = row.cadence_options || {};
  return {
    ...row,
    enabled: Boolean(row.enabled),
    profile_id: row.policy_id,
    interval_value: values.value ?? null,
    interval_unit: values.unit ?? null,
    weekday: values.weekday ?? null,
    day_of_month: values.day ?? null,
    hour: values.hour ?? 0,
    minute: values.minute ?? 0,
    description: scheduleDescription(row),
  };
}

function schedulePayload(payload: Json): Json {
  const cadence = payload.cadence || "daily";
  const timing = cadence === "weekly"
    ? { weekday: Number(payload.weekday ?? 0), hour: Number(payload.hour || 0), minute: Number(payload.minute || 0) }
    : cadence === "monthly"
      ? { day: Number(payload.day_of_month ?? 1), hour: Number(payload.hour || 0), minute: Number(payload.minute || 0) }
      : cadence === "interval"
        ? { value: Number(payload.interval_value ?? 24), unit: payload.interval_unit || "hours" }
        : { hour: Number(payload.hour || 0), minute: Number(payload.minute || 0) };
  return { name: payload.name, enabled: Boolean(payload.enabled), library_id: payload.library_id, policy_id: payload.profile_id, cadence, ...timing };
}

function outcomeName(value: string): string {
  if (["optimized", "converted"].includes(value)) return "optimized";
  if (["already_current", "skipped"].includes(value)) return "already_current";
  if (["retained", "unchanged", "unsupported"].includes(value)) return "retained";
  return value === "deleted" ? "deleted" : "failed";
}

function issueRemedies(issue: Json): string[] {
  if (issue.kind === "crc_mismatch") return ["rebuild", "preserve", "replace", "delete", "dismiss"];
  if (issue.kind === "filename_collision") return ["keep_primary", "keep_related", "dismiss"];
  if (issue.kind === "archive_corrupt") return ["salvage", "salvage_partial", "compatibility", "dismiss"];
  if (issue.kind === "archive_grew") return ["lossless", "smart_lossy", "restore_original", "repack", "dismiss"];
  if (issue.entry_path) return ["recover", "preserve", "replace", "delete", "dismiss"];
  return ["compatibility", "dismiss"];
}

function mapIssue(issue: Json): Json {
  return {
    ...issue,
    remedies: issue.remedies || issueRemedies(issue),
    retry_failed: Boolean(issue.retry_failed),
    preview_url: issue.entry_path ? `/api/v2/issues/${issue.id}/preview` : null,
    related_preview_url: issue.related_entry_path ? `/api/v2/issues/${issue.id}/preview?related=true` : null,
    diagnostics_url: ["crc_mismatch", "archive_corrupt"].includes(issue.kind)
      ? `/api/issues/${issue.id}/diagnostics`
      : null,
    latest_at: issue.updated_at,
  };
}

async function bootstrap(): Promise<Json> {
  const value = await request<Json>("/bootstrap");
  return {
    ...value,
    database_status: value.database_status || "ok",
    database_error: value.database_error || null,
    conversion_runtime_status: value.conversion_runtime,
    libraries: (value.libraries || []).map((row: Json) => ({ ...row, root_path: row.root })),
    profiles: (value.policies || []).map(mapProfile),
    schedules: (value.schedules || []).map(mapSchedule),
  };
}

async function dashboard(): Promise<Json> {
  const value = await request<Json>("/dashboard");
  const totals = value.totals || {};
  return {
    totals: {
      ...totals,
      net_gb: Number(totals.net_bytes || 0) / GB,
      page_count: Number(totals.converted_pages || 0),
      optimized_count: Number(totals.optimized_archives || 0),
    },
    review: { total: Number(value.attention?.archives || 0) },
    active_jobs: (value.active_jobs || []).map(mapJob),
    recent_jobs: (value.recent_jobs || []).map(mapJob),
  };
}

async function statistics(): Promise<Json> {
  const value = await request<Json>("/statistics");
  if (value.health && value.filetypes && value.processing) return value;
  const library = value.library || {};
  const outcomes = value.outcomes || [];
  const attempted = outcomes.reduce((sum: number, row: Json) => sum + Number(row.archives || 0), 0);
  const optimized = outcomes.filter((row: Json) => row.outcome === "optimized").reduce((sum: number, row: Json) => sum + Number(row.archives || 0), 0);
  const fileRows = (value.formats || []).map((row: Json) => ({ label: row.format, file_count: row.files, storage_bytes: row.compressed_bytes }));
  return {
    health: {
      library_coverage_percent: Number(library.archives || 0) ? Number(library.converted_pages || 0) / Math.max(1, Number(library.pages || 0)) * 100 : 0,
      average_percent_saved: Number(library.average_percent_saved || 0),
      retained_pages: outcomes.reduce((sum: number, row: Json) => sum + Number(row.retained_pages || 0), 0),
      success_percent: attempted ? optimized / attempted * 100 : 0,
    },
    library: {
      ...library,
      optimized_archives: optimized,
      source_bytes: library.baseline_bytes,
      jxl_coverage_percent: Number(library.pages || 0) ? Number(library.converted_pages || 0) / Number(library.pages) * 100 : 0,
    },
    run_trend: [],
    filetypes: {
      rows: fileRows,
      total_files: fileRows.reduce((sum: number, row: Json) => sum + Number(row.file_count || 0), 0),
      total_storage_bytes: fileRows.reduce((sum: number, row: Json) => sum + Number(row.storage_bytes || 0), 0),
      inventoried_archives: library.archives,
      archive_count: library.archives,
    },
    formats: fileRows.map((row: Json) => ({ ...row, net_bytes: 0, average_percent_saved: 0 })),
    outcomes: outcomes.map((row: Json) => ({ label: row.outcome, count: row.archives })),
    savings_distribution: [],
    savings_summary: { median: library.average_percent_saved, average: library.average_percent_saved, middle_50: [0, 0] },
    processing: {
      median_seconds: Number(library.average_duration_ms || 0) / 1000,
      p90_seconds: Number(library.average_duration_ms || 0) / 1000,
      distribution: [],
      policies: (value.policies || []).map((row: Json) => ({ name: row.policy, median_seconds: Number(row.average_duration_ms || 0) / 1000, average_percent_saved: row.average_percent_saved, success_percent: Number(row.attempted || 0) ? Number(row.optimized || 0) / Number(row.attempted) * 100 : 0 })),
    },
    attention: (value.issues || []).map((row: Json) => ({ id: `${row.kind}:${row.stage}`, label: `${row.kind} · ${row.stage}`, count: row.issues })),
    effort: { completed_runs: 0, archives_attempted: attempted, pages_examined: library.pages, encoder_hours: 0 },
  };
}

async function liveJob(id: string): Promise<Json> {
  const value = await request<Json>(`/jobs/${id}`);
  const latest = value.events?.[0] || {};
  let data: Json = {};
  try { data = JSON.parse(latest.data_json || "{}"); } catch { data = {}; }
  const mapped = mapJob(value.job);
  return { ...value, job: { ...mapped, ...data, smooth_percent: data.progress_percent ?? mapped.percent, current_stage: latest.event_type, current_archive: data.archive } };
}

async function jobHistory(id: string, query = ""): Promise<Json> {
  const value = await request<Json>(`/jobs/${id}/history?limit=500${query ? `&${query}` : ""}`);
  return { ...value, job: mapJob(value.job) };
}

export async function api<T = Json>(path: string, options: RequestInit = {}): Promise<T> {
  const method = (options.method || "GET").toUpperCase();
  if (path === "/api/bootstrap") return bootstrap() as Promise<T>;
  if (path === "/api/dashboard") return dashboard() as Promise<T>;
  if (path === "/api/statistics") return statistics() as Promise<T>;

  const live = path.match(/^\/api\/jobs\/([^/]+)\/live$/);
  if (live) return liveJob(live[1]) as Promise<T>;
  const history = path.match(/^\/api\/jobs\/([^/]+)\/history(?:\?(.*))?$/);
  if (history) return jobHistory(history[1], history[2] || "") as Promise<T>;

  if (path === "/api/jobs" && method === "POST") {
    const payload = JSON.parse(String(options.body || "{}"));
    return request<T>("/jobs", { ...options, body: JSON.stringify({ library_id: payload.library_id, policy_id: payload.profile_id, scope: payload.scope, policy: policyFromOptions(payload.options) }) });
  }
  if (path.startsWith("/api/jobs")) {
    const value = await request<Json>(path.slice(4), options);
    if (Array.isArray(value.rows)) value.rows = value.rows.map(mapJob);
    return value as T;
  }

  if (path === "/api/issues/summary") {
    const value = await request<Json>("/issues/summary");
    const human = (text: string) => text.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
    const groups = (value.groups || []).map((row: Json) => ({ id: `${row.kind}:${row.stage}`, label: `${human(row.kind)} · ${human(row.stage)}`, title: `${human(row.kind)} · ${human(row.stage)}`, count: row.issues, archive_count: row.archives, issues: row.issues, archives: row.archives, scope: row.stage, message: `${row.issues} unresolved issues`, latest_at: null }));
    return { groups, rows: groups, total: Number(value.total?.issues || 0) } as T;
  }
  if (path.startsWith("/api/issues?") && method === "GET") {
    const query = new URLSearchParams(path.split("?")[1]);
    const group = query.get("group");
    const before = query.get("before_id") || query.get("after_id");
    const value = await request<Json>(`/issues?limit=${query.get("limit") || 100}${group ? `&group=${encodeURIComponent(group)}` : ""}${before ? `&before_id=${encodeURIComponent(before)}` : ""}`);
    value.rows = (value.rows || []).map(mapIssue);
    return value as T;
  }
  const issueAction = path.match(/^\/api\/issues\/(\d+)\/action$/);
  if (issueAction) {
    const payload = JSON.parse(String(options.body || "{}"));
    const endpoint = payload.action === "dismiss"
      ? `/issues/${issueAction[1]}/dismiss`
      : payload.action === "retry"
        ? `/issues/${issueAction[1]}/retry`
        : `/issues/${issueAction[1]}/action`;
    return request<T>(endpoint, options);
  }
  if (path.startsWith("/api/issues/")) return request<T>(path.slice(4), options);

  const profile = path.match(/^\/api\/profiles(?:\/([^/]+))?$/);
  if (profile) {
    const payload = options.body ? JSON.parse(String(options.body)) : null;
    return request<T>(`/policies${profile[1] ? `/${profile[1]}` : ""}`, payload ? { ...options, body: JSON.stringify({ name: payload.name, is_default: payload.is_default, policy: policyFromOptions(payload.options) }) } : options);
  }

  if (path === "/api/schedules" && method === "GET") {
    const value = await request<Json>("/schedules");
    value.rows = (value.rows || []).map(mapSchedule);
    return value as T;
  }
  if (path.startsWith("/api/schedules")) {
    const payload = options.body ? schedulePayload(JSON.parse(String(options.body))) : null;
    return request<T>(path.slice(4), payload ? { ...options, body: JSON.stringify(payload) } : options);
  }

  return request<T>(path.startsWith("/api") ? path.slice(4) : path, options);
}
