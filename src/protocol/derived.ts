import { toHex, utf8 } from "./bytes.ts"

// Derived run IDs (plan D2-8, protocol/vectors/derived-ids.json): a UUIDv8 from the first 16
// bytes of SHA-256(domain + "\0" + a + "\0" + b), with version 8, the RFC 4122 variant, and
// rand_a carrying the shard: big-endian uint16 of sum[16:18] mod 1024 (byte 6's low two bits and
// byte 7), as shardOf reads it.
export async function derivedRunId(domain: string, a: string, b: string): Promise<string> {
  const sum = new Uint8Array(await crypto.subtle.digest("SHA-256", utf8(`${domain}\u0000${a}\u0000${b}`)))
  const shard = ((sum[16]! << 8) | sum[17]!) % 1024
  const h = sum.slice(0, 16)
  h[6] = 0x80 | ((shard >> 8) & 0x03)
  h[7] = shard & 0xff
  h[8] = 0x80 | (h[8]! & 0x3f)
  const x = toHex(h)
  return `${x.slice(0, 8)}-${x.slice(8, 12)}-${x.slice(12, 16)}-${x.slice(16, 20)}-${x.slice(20)}`
}

// rfc3339Nano formats a time like Go's time.RFC3339Nano in UTC: fractional seconds without
// trailing zeros, and none at all for a whole second. A string keeps its sub-millisecond digits.
export function rfc3339Nano(t: string | Date): string {
  let ms: number
  let frac = ""
  if (typeof t === "string") {
    const m = /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/i.exec(t)
    if (!m) throw new TypeError(`Not an RFC 3339 time: "${t}"`)
    ms = Date.parse(`${m[1]}${m[3]}`)
    frac = m[2] ?? ""
  } else {
    ms = t.getTime()
    frac = String(((ms % 1000) + 1000) % 1000).padStart(3, "0")
    ms -= ((ms % 1000) + 1000) % 1000
  }
  if (Number.isNaN(ms)) throw new TypeError("Not a time")
  const whole = new Date(ms).toISOString().slice(0, 19)
  frac = frac.replace(/0+$/, "")
  return `${whole}${frac ? `.${frac}` : ""}Z`
}

export const childRunId = (parentRunId: string, stepId: string) => derivedRunId("workflows.child", parentRunId, stepId)
export const scheduledRunId = (scheduleId: string, scheduledTime: string | Date) => derivedRunId("workflows.schedule", scheduleId, rfc3339Nano(scheduledTime))
export const triggeredRunId = (scheduleId: string, idempotencyKey: string) => derivedRunId("workflows.trigger", scheduleId, idempotencyKey)
