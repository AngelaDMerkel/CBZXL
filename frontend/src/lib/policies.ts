import type { Json } from "../types";

export function policyStrategy(options: Json = {}) {
  if (options.integrity_audit) return "audit";
  if (options.no_convert) return "recheck";
  if (options.smart_distance) return "smart";
  if (options.distance !== null && options.distance !== undefined) return "lossy";
  return "lossless";
}

export function policyScope(options: Json = {}) {
  if (options.failed_only) return "failed";
  if (options.recheck_all) return "library";
  return "unprocessed";
}

export function policySummary(profile: Json) {
  const options = profile.options || {};
  const strategy = policyStrategy(options);
  const effort = Number(options.effort ?? 10);
  const threads = Number(options.threads ?? 4);
  if (strategy === "audit") {
    return `Full CRC check + full JPEG XL decode · ${threads} threads`;
  }
  if (strategy === "recheck") return `Recheck only · ${threads} threads`;
  if (strategy === "smart") {
    return `Lossless first · d${Number(options.distance ?? 0.5)} fallback above ${Number(options.smart_mb ?? 10)} MB or ${Number(options.smart_mp ?? 5)} MP · effort ${effort}`;
  }
  if (strategy === "lossy") {
    return `Lossy d${Number(options.distance)} · effort ${effort} · ${threads} threads`;
  }
  return `Lossless · effort ${effort} · ${threads} threads`;
}

