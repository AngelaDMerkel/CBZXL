export function formatBytes(value?: number | null) {
  const bytes = Number(value || 0);
  const absolute = Math.abs(bytes);
  if (absolute >= 1024 ** 3) {
    return `${(bytes / 1024 ** 3).toFixed(absolute >= 10 * 1024 ** 3 ? 1 : 2)} GB`;
  }
  if (absolute >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
  if (absolute >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${bytes.toLocaleString()} B`;
}

export function formatGb(value?: number | null) {
  return formatBytes(Number(value || 0) * 1024 ** 3);
}

export function compactNumber(value?: number | null) {
  const number = Number(value || 0);
  return new Intl.NumberFormat(undefined, {
    notation: number >= 100_000 ? "compact" : "standard",
    maximumFractionDigits: 2,
  }).format(number);
}

export function cpuOptions(limitValue?: number | null) {
  const limit = Math.max(1, Math.floor(Number(limitValue || 1)));
  const standard = [1, 2, 4, 6, 8, 12, 16, 24, 32].filter((value) => value <= limit);
  return [...new Set([...standard, limit])].sort((left, right) => left - right);
}

export function dateTime(value?: string | null) {
  if (!value) return "Not yet";
  return new Date(value).toLocaleString(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

export function duration(secondsValue?: number | null) {
  const seconds = Math.max(0, Number(secondsValue || 0));
  if (!seconds) return "Calculating";
  if (seconds < 60) return `${Math.round(seconds)} sec`;
  if (seconds < 3600) {
    return `${Math.floor(seconds / 60)}m ${Math.round(seconds % 60)}s`;
  }
  return `${Math.floor(seconds / 3600)}h ${Math.round((seconds % 3600) / 60)}m`;
}

export function archiveName(path = "") {
  return path.split(/[\\/]/).pop() || path;
}

export function label(value = "") {
  return value.replaceAll("_", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

