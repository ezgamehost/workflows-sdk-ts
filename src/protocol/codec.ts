import { utf8 } from "./bytes.ts"
import type { RetryPolicy } from "./types.ts"

export const MAX_OUTPUT_BYTES = 1_048_576

// encodeValue passes a value through JSON exactly as the wire will, so a step's live result and
// its replayed result are the same value (design, "Return values are JSON").
export function encodeValue(v: unknown): unknown {
  if (v === undefined) return null
  const text = JSON.stringify(v, (_key, value: unknown) => {
    if (typeof value === "function" || typeof value === "symbol" || typeof value === "bigint")
      throw new TypeError(`A ${typeof value} can't be a step result or workflow output; return JSON values`)
    return value
  })
  return text === undefined ? null : (JSON.parse(text) as unknown)
}

export function jsonByteLength(v: unknown): number {
  return utf8(JSON.stringify(v ?? null)).length
}

const units: Record<string, number> = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000 }

// parseDuration: milliseconds, or a string such as 500ms, 30s, 5m, 2h or 3d.
export function parseDuration(d: string | number): number {
  if (typeof d === "number") {
    if (!Number.isFinite(d) || d < 0) throw new TypeError("A duration is a non-negative number of milliseconds")
    return Math.floor(d)
  }
  const m = typeof d === "string" ? /^(\d+)(ms|s|m|h|d)$/.exec(d.trim()) : null
  if (!m) throw new TypeError(`"${String(d)}" isn't a duration: use 500ms, 30s, 5m, 2h or 3d`)
  return Number(m[1]) * (units[m[2]!] as number)
}

// backoff is the delay before attempt+1: min(maxMs, floor(initialMs * multiplier^(attempt-1))).
// Attempts count from 1 (internal/protocol RetryPolicy.Backoff).
export function backoff(p: RetryPolicy, attempt: number): number {
  const ms = Math.floor(p.initialMs * p.multiplier ** (Math.max(1, attempt) - 1))
  if (!Number.isFinite(ms) || ms > p.maxMs) return p.maxMs
  return Math.max(0, ms)
}
