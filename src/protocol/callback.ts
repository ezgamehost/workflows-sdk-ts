import { fromBase64url, toBase64url, toHex } from "./bytes.ts"

// Callback tokens (design-phase-2, "Callbacks"): "<shard 3 lowercase hex>.<43 base64url>", 256
// random bits. The shard is the run's, so workflows-api routes a completion without a lookup; the
// engine stores SHA-256 of the secret (internal/protocol CallbackToken).
const TOKEN = /^([0-9a-f]{3})\.([A-Za-z0-9_-]{43})$/

export function mintCallbackToken(shard: number, random: (n: number) => Uint8Array = (n) => crypto.getRandomValues(new Uint8Array(n))): string {
  if (!Number.isInteger(shard) || shard < 0 || shard > 1023) throw new TypeError("A callback token's shard is 0–1023")
  const secret = random(32)
  if (secret.length !== 32) throw new TypeError("A callback token's secret is 32 bytes")
  return `${shard.toString(16).padStart(3, "0")}.${toBase64url(secret)}`
}

// parseCallbackToken parses the wire form strictly (lowercase hex, canonical base64url).
export function parseCallbackToken(token: string): { shard: number; secret: Uint8Array } {
  const m = typeof token === "string" ? TOKEN.exec(token) : null
  if (!m) throw new TypeError("Not a callback token")
  const shard = parseInt(m[1]!, 16)
  if (shard > 1023) throw new TypeError("Not a callback token: the shard is out of range")
  const secret = fromBase64url(m[2]!)
  if (secret.length !== 32) throw new TypeError("Not a callback token: the secret isn't 32 bytes")
  return { shard, secret }
}

// callbackTokenHash: lowercase hex SHA-256 of the token's secret (what the engine stores).
export async function callbackTokenHash(token: string): Promise<string> {
  const { secret } = parseCallbackToken(token)
  return toHex(new Uint8Array(await crypto.subtle.digest("SHA-256", secret as Uint8Array<ArrayBuffer>)))
}
