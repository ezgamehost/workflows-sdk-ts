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

const key = (secret: Uint8Array) =>
  crypto.subtle.importKey("raw", new Uint8Array(secret), { name: "HMAC", hash: "SHA-256" }, false, ["sign"])
const mac = async (secret: Uint8Array, payload: Uint8Array<ArrayBuffer>) =>
  new Uint8Array(await crypto.subtle.sign("HMAC", await key(secret), payload))

// equal compares in constant time for equal lengths.
function equal(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false
  let d = 0
  for (let i = 0; i < a.length; i++) d |= a[i]! ^ b[i]!
  return d === 0
}

// sign: EZGH-Signature: t=<unix seconds>,v1=<hex HMAC-SHA256(secret, "<t>." + body)>, one v1 per secret.
export async function sign(secrets: Uint8Array[], t: Date, body: Uint8Array): Promise<string> {
  const ts = Math.floor(t.getTime() / 1000)
  const payload = concat(utf8(`${ts}.`), body)
  const parts = [`t=${ts}`]
  for (const s of secrets) parts.push(`v1=${toHex(await mac(s, payload))}`)
  return parts.join(",")
}

export const MAX_SIGNATURES = 8

export interface ParsedSignature { t: number; signatures: Uint8Array[] }

// parseSignature checks the header's shape and timestamp without the body, so a request with a
// malformed or stale signature is refused before its body is read. It throws
// SignatureError("invalid") for a malformed header (or more than 8 v1 values) and
// SignatureError("expired") when t is more than 300 s from now either way.
export function parseSignature(header: string, now: Date): ParsedSignature {
  if (typeof header !== "string" || header.length > 1024) throw new SignatureError("invalid", "The EZGH-Signature header is missing or malformed")
  let t: number | undefined
  const signatures: Uint8Array[] = []
  for (const raw of header.split(",")) {
    const part = raw.trim()
    const i = part.indexOf("=")
    if (i < 0) throw new SignatureError("invalid", "The EZGH-Signature header is malformed")
    const k = part.slice(0, i)
    const v = part.slice(i + 1)
    if (k === "t") {
      if (!/^\d{1,12}$/.test(v)) throw new SignatureError("invalid", "The EZGH-Signature timestamp is malformed")
      t = Number(v)
    } else if (k === "v1" && /^[0-9a-fA-F]{64}$/.test(v)) {
      if (signatures.length === MAX_SIGNATURES) throw new SignatureError("invalid", `The EZGH-Signature header has more than ${MAX_SIGNATURES} signatures`)
      signatures.push(fromHex(v.toLowerCase()))
    }
  }
  if (t === undefined || signatures.length === 0) throw new SignatureError("invalid", "The EZGH-Signature header is missing or malformed")
  if (Math.abs(now.getTime() / 1000 - t) > MAX_SIGNATURE_AGE_SECONDS)
    throw new SignatureError("expired", "The EZGH-Signature timestamp is more than 5 minutes from now")
  return { t, signatures }
}

// verifyParsed checks a parsed signature against the body: one HMAC per secret, each candidate
// compared in constant time.
export async function verifyParsed(sig: ParsedSignature, secrets: Uint8Array[], body: Uint8Array): Promise<void> {
  const payload = concat(utf8(`${sig.t}.`), body)
  let ok = false
  for (const s of secrets) {
    const want = await mac(s, payload)
    for (const got of sig.signatures) if (equal(want, got)) ok = true
  }
  if (!ok) throw new SignatureError("invalid", "The EZGH-Signature doesn't match any signing secret")
}

// verify = parseSignature then verifyParsed.
export async function verify(header: string, secrets: Uint8Array[], body: Uint8Array, now: Date): Promise<void> {
  await verifyParsed(parseSignature(header, now), secrets, body)
}
