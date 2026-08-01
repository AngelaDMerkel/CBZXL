export type Json = Record<string, any>;
export type View = "overview" | "attention" | "history" | "settings";
export type ToastValue = { kind: "success" | "error"; message: string } | null;

