import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import App, { liveStage } from "./App";

const bootstrap = {
  version: "5.2.0",
  authentication: false,
  timezone: "America/New_York",
  conversion_runtime: "ok",
  capabilities: { cpu_limit: 8 },
  libraries: [
    { id: "one", name: "Primary", root: "/data", enabled: true },
    { id: "two", name: "Secondary", root: "/secondary", enabled: true },
  ],
  policies: [{
    id: "balanced",
    name: "Balanced lossless",
    is_default: true,
    policy: { strategy: "lossless", scope: "unprocessed", effort: 8, threads: 4, backup: false, smart_mb: 10, smart_mp: 5 },
  }],
  schedules: [],
};

const dashboard = {
  totals: { archives: 10, optimized_archives: 5, pages: 100, converted_pages: 50, net_bytes: 1024, failed_archives: 0 },
  attention: { archives: 0, issues: 0 },
  active_jobs: [],
  recent_jobs: [],
};

function json(value: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(value), {
    status,
    headers: { "Content-Type": "application/json" },
  }));
}

afterEach(() => {
  cleanup();
  localStorage.clear();
  location.hash = "";
});

beforeEach(() => {
  vi.stubGlobal("scrollTo", vi.fn());
});

describe("CBZXL application workflows", () => {
  it("maps volatile worker events to stable user-facing stages", () => {
    expect(liveStage("archive_started")).toBe("Converting");
    expect(liveStage("page_completed")).toBe("Converting");
    expect(liveStage("reconciling")).toBe("Preparing Queue");
  });

  it("keeps multiple libraries and complete schedule controls accessible", async () => {
    vi.stubGlobal("fetch", vi.fn((input: RequestInfo | URL) => {
      const path = String(input);
      if (path.endsWith("/api/v2/bootstrap")) return json(bootstrap);
      if (path.includes("/api/v2/dashboard")) return json(dashboard);
      if (path.endsWith("/api/v2/schedules")) return json({ rows: [] });
      throw new Error(`unexpected request: ${path}`);
    }));
    render(<App />);
    await screen.findByRole("button", { name: "Begin Manual Conversion" });
    fireEvent.click(screen.getByRole("button", { name: "Begin Manual Conversion" }));
    expect(screen.getByRole("combobox", { name: "Library" })).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: "Settings" }));
    await screen.findByRole("heading", { name: "Libraries" });
    fireEvent.click(screen.getByRole("button", { name: "New schedule" }));
    const cadence = screen.getByRole("combobox", { name: "Cadence" });
    fireEvent.change(cadence, { target: { value: "interval" } });
    expect(screen.getByRole("option", { name: "Interval" })).toBeTruthy();
    expect(screen.getByRole("spinbutton", { name: "Every" })).toBeTruthy();
  });

  it("uses an in-app authentication form instead of a browser prompt", async () => {
    vi.stubGlobal("fetch", vi.fn(() => json({ error: "unauthorized" }, 401)));
    render(<App />);
    await waitFor(() => expect(screen.getByRole("heading", { name: "Connect to CBZXL" })).toBeTruthy());
    expect(screen.getByLabelText("API token")).toBeTruthy();
  });

  it("routes the primary Attention retry through the contextual retry endpoint", async () => {
    const fetchMock = vi.fn((input: RequestInfo | URL, options?: RequestInit) => {
      const path = String(input);
      if (path.endsWith("/api/v2/bootstrap")) return json(bootstrap);
      if (path.includes("/api/v2/dashboard")) return json({ ...dashboard, attention: { archives: 1, issues: 1 } });
      if (path.endsWith("/api/v2/issues/summary")) return json({ groups: [{ kind: "archive_grew", stage: "verifying", issues: 1, archives: 1 }], total: { issues: 1, archives: 1 } });
      if (path.includes("/api/v2/issues?")) return json({ rows: [{ id: 1, archive_id: 1, library_id: "one", path: "Book.cbz", kind: "archive_grew", stage: "verifying", status: "open", message: "Larger", backup_available: false, remedies: ["repack", "dismiss"], updated_at: "2026-08-23T00:00:00Z" }] });
      if (path.endsWith("/api/v2/issues/1/retry") && options?.method === "POST") return json({ id: "retry", status: "queued" });
      throw new Error(`unexpected request: ${path}`);
    });
    vi.stubGlobal("fetch", fetchMock);
    render(<App />);
    await screen.findByRole("button", { name: "Begin Manual Conversion" });
    fireEvent.click(screen.getByRole("button", { name: /Attention/ }));
    fireEvent.click(await screen.findByRole("button", { name: "Review" }));
    fireEvent.click(await screen.findByRole("button", { name: "Repack" }));
    await waitFor(() => expect(fetchMock.mock.calls.some(([input]) => String(input).endsWith("/api/v2/issues/1/retry"))).toBe(true));
  });
});
