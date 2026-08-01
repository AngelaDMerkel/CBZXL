import type { Json } from "../types";

export async function api<T = Json>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
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
    } catch {
      // The status code remains useful when an upstream proxy returns HTML.
    }
    throw new Error(message);
  }
  return response.status === 204 ? ({} as T) : response.json();
}

