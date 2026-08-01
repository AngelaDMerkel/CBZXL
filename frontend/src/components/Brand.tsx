import type { ToastValue } from "../types";

export function Logo() {
  return <svg viewBox="0 0 28 28" role="img" aria-label="Manga page being compressed">
    <rect x="9" y="3" width="10" height="22" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1.5" />
    <rect x="11" y="5.5" width="6" height="6" rx=".5" fill="none" stroke="currentColor" strokeWidth="1.1" />
    <path d="M11 14h2.4v3.6H11zm3.6 0H17v3.6h-2.4zM11 21h6M2 14h5m-2.5-2.5L7 14l-2.5 2.5M26 14h-5m2.5-2.5L21 14l2.5 2.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
  </svg>;
}

export function Toast({ value }: { value: ToastValue }) {
  return value
    ? <div className={`toast ${value.kind}`} role="status">{value.message}</div>
    : null;
}

