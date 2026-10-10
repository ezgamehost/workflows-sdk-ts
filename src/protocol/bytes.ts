export const utf8 = (s: string): Uint8Array<ArrayBuffer> => new TextEncoder().encode(s) as Uint8Array<ArrayBuffer>

export function toHex(b: Uint8Array): string {
  let s = ""
  for (const x of b) s += x.toString(16).padStart(2, "0")
  return s
}

export function fromHex(h: string): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(h.length / 2)
  for (let i = 0; i < out.length; i++) out[i] = parseInt(h.slice(2 * i, 2 * i + 2), 16)
  return out
}

export function toBase64url(b: Uint8Array): string {
  let bin = ""
  for (const x of b) bin += String.fromCharCode(x)
  return btoa(bin).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "")
}

// fromBase64url decodes unpadded base64url strictly: only the canonical encoding of the bytes is
// accepted (Go's base64.RawURLEncoding.Strict()), so one secret has exactly one text form.
export function fromBase64url(s: string): Uint8Array<ArrayBuffer> {
  if (!/^[A-Za-z0-9_-]*$/.test(s) || s.length % 4 === 1) throw new TypeError("not base64url")
  const bin = atob(s.replace(/-/g, "+").replace(/_/g, "/") + "=".repeat((4 - (s.length % 4)) % 4))
  const out = new Uint8Array(bin.length)
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i)
  if (toBase64url(out) !== s) throw new TypeError("not canonical base64url")
  return out
}

export function concat(a: Uint8Array, b: Uint8Array): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(a.length + b.length)
  out.set(a, 0)
  out.set(b, a.length)
  return out
}
