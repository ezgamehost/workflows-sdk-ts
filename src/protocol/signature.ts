import { concat, fromBase64url, fromHex, toHex, utf8 } from "./bytes.ts"

export const SIGNATURE_HEADER = "EZGH-Signature"
export const SECRET_PREFIX = "whsec_"
export const MAX_SIGNATURE_AGE_SECONDS = 300

export class SignatureError extends Error {
  constructor(readonly reason: "expired" | "invalid", message: string) {
    super(message)
    this.name = "SignatureError"
  }
}

const malformedSecret = "A signing secret is whsec_ and 43 base64url characters, as the API returned it"

// decodeSecret: "whsec_" + 43 base64url characters → the 32 HMAC key bytes (D22).
export function decodeSecret(secret: string): Uint8Array<ArrayBuffer> {
  const s = typeof secret === "string" ? secret.trim() : ""
  if (!s.startsWith(SECRET_PREFIX) || s.length !== SECRET_PREFIX.length + 43) throw new TypeError(malformedSecret)
  let b: Uint8Array<ArrayBuffer>
  try { b = fromBase64url(s.slice(SECRET_PREFIX.length)) } catch { throw new TypeError(malformedSecret) }
  if (b.length !== 32) throw new TypeError(malformedSecret)
  return b
}

const key = (secret: Uint8Array, usage: KeyUsage) =>
  crypto.subtle.importKey("raw", new Uint8Array(secret), { name: "HMAC", hash: "SHA-256" }, false, [usage])

// sign: EZGH-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256(secret, "<t>." + body)>, one v1 per secret.
export async function sign(secrets: Uint8Array[], t: Date, body: Uint8Array): Promise<string> {
  const ts = Math.floor(t.getTime() / 1000)
  const payload = concat(utf8(`${ts}.`), body)
  const parts = [`t=${ts}`]
  for (const s of secrets) parts.push(`v1=${toHex(new Uint8Array(await crypto.subtle.sign("HMAC", await key(s, "sign"), payload)))}`)
  return parts.join(",")
}

// verify throws SignatureError("invalid") for a malformed header or no matching signature, and
// SignatureError("expired") when t is more than 300 s from now either way. crypto.subtle.verify
// compares in constant time.
export async function verify(header: string, secrets: Uint8Array[], body: Uint8Array, now: Date): Promise<void> {
  let ts: number | undefined
  const sigs: Uint8Array<ArrayBuffer>[] = []
  for (const raw of header.split(",")) {
    const part = raw.trim()
    const i = part.indexOf("=")
    if (i < 0) throw new SignatureError("invalid", "The EZGH-Signature header is malformed")
    const k = part.slice(0, i)
    const v = part.slice(i + 1)
    if (k === "t") {
      if (!/^\d{1,12}$/.test(v)) throw new SignatureError("invalid", "The EZGH-Signature timestamp is malformed")
      ts = Number(v)
    } else if (k === "v1" && /^[0-9a-fA-F]{64}$/.test(v)) sigs.push(fromHex(v.toLowerCase()))
  }
  if (ts === undefined || sigs.length === 0) throw new SignatureError("invalid", "The EZGH-Signature header is missing or malformed")
  if (Math.abs(now.getTime() / 1000 - ts) > MAX_SIGNATURE_AGE_SECONDS)
    throw new SignatureError("expired", "The EZGH-Signature timestamp is more than 5 minutes from now")
  const payload = concat(utf8(`${ts}.`), body)
  for (const s of secrets) {
    const k = await key(s, "verify")
    for (const sig of sigs) if (await crypto.subtle.verify("HMAC", k, sig, payload)) return
  }
  throw new SignatureError("invalid", "The EZGH-Signature doesn't match any signing secret")
}
