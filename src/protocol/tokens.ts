import { fromBase64url } from "./bytes.ts"

const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/

// shardOf reads the shard (0–1023) from a v7 or v8 run ID: rand_a holds 0b00 and the 10-bit
// shard. It returns undefined for anything else (internal/protocol ShardOf).
export function shardOf(runId: string): number | undefined {
  if (typeof runId !== "string" || !uuid.test(runId)) return undefined
  const hex = runId.replace(/-/g, "")
  const b6 = parseInt(hex.slice(12, 14), 16)
  const b7 = parseInt(hex.slice(14, 16), 16)
  const b8 = parseInt(hex.slice(16, 18), 16)
  const version = b6 >> 4
  if ((version !== 7 && version !== 8) || (b8 & 0xc0) !== 0x80 || (b6 & 0x0c) !== 0) return undefined
  return ((b6 & 0x03) << 8) | b7
}

// isInvocationToken: "<shard 3 hex>.<run UUID>.<invocation ID>.<43 base64url secret>", the shard
// being the run ID's (internal/protocol ParseInvocationToken). SDKs pass tokens through; they
// check the shape only to fail fast.
export function isInvocationToken(s: string): boolean {
  if (typeof s !== "string") return false
  const parts = s.split(".")
  if (parts.length !== 4) return false
  const [shard, runId, invocationId, secret] = parts as [string, string, string, string]
  if (!/^[0-9a-f]{3}$/.test(shard) || !/^inv_[a-z2-7]{26}$/.test(invocationId)) return false
  const runShard = shardOf(runId)
  if (runShard === undefined || runShard !== parseInt(shard, 16)) return false
  try { return fromBase64url(secret).length === 32 } catch { return false }
}
