import { describe, expect, test } from "bun:test"
import pkg from "../package.json"
import {
  decodeSecret, encodeValue, isInvocationToken, jsonByteLength, parseDuration, sign, SignatureError, stepId, verify,
} from "../src/protocol/index.ts"
import { SDK_NAME, SDK_VERSION } from "../src/version.ts"

const secret = decodeSecret("whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")
const body = new TextEncoder().encode('{"kind":"sync"}')

describe("signatures", () => {
  test("round trip, with two secrets during a rotation", async () => {
    const other = decodeSecret("whsec_Hx4dHBsaGRgXFhUUExIREA8ODQwLCgkIBwYFBAMCAQA")
    const t = new Date(1_760_000_000_000)
    const header = await sign([secret, other], t, body)
    expect(header).toMatch(/^t=1760000000,v1=[0-9a-f]{64},v1=[0-9a-f]{64}$/)
    await verify(header, [other], body, new Date(1_760_000_060_000))
  })
  test("older than five minutes is expired", async () => {
    const header = await sign([secret], new Date(1_760_000_000_000), body)
    const err = await verify(header, [secret], body, new Date(1_760_000_301_000)).catch((e) => e)
    expect(err).toBeInstanceOf(SignatureError)
    expect((err as SignatureError).reason).toBe("expired")
  })
  test("a changed body is invalid", async () => {
    const header = await sign([secret], new Date(1_760_000_000_000), body)
    const err = await verify(header, [secret], new TextEncoder().encode("{}"), new Date(1_760_000_000_000)).catch((e) => e)
    expect((err as SignatureError).reason).toBe("invalid")
  })
  test("garbage headers are invalid, not exceptions of another kind", async () => {
    for (const h of ["", "t=x,v1=00", "v1=" + "0".repeat(64), "t=1760000000", "t=1760000000,garbage,v1=" + "0".repeat(64)]) {
      const err = await verify(h, [secret], body, new Date(1_760_000_000_000)).catch((e) => e)
      expect(err).toBeInstanceOf(SignatureError)
      expect((err as SignatureError).reason).toBe("invalid")
    }
  })
  test("a secret that isn't whsec_ and 32 bytes is refused", () => {
    expect(() => decodeSecret("abc")).toThrow(TypeError)
    expect(() => decodeSecret("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8")).toThrow(TypeError) // no whsec_
    expect(() => decodeSecret("whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh9")).toThrow(TypeError) // non-canonical last character
    expect(() => decodeSecret("whsec_AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh+")).toThrow(TypeError) // not base64url
  })
})

describe("values", () => {
  test("live values are what a replay will see", () => {
    expect(encodeValue(new Date(0))).toBe("1970-01-01T00:00:00.000Z")
    expect(encodeValue(undefined)).toBeNull()
    expect(encodeValue({ a: undefined, b: [undefined, 1], c: new Date(0) })).toEqual({ b: [null, 1], c: "1970-01-01T00:00:00.000Z" })
    expect(encodeValue(new Map([["k", 1]]))).toEqual({})
  })
  test("functions, symbols and bigints are refused", () => {
    expect(() => encodeValue(() => 1)).toThrow(TypeError)
    expect(() => encodeValue({ f: Symbol("s") })).toThrow(TypeError)
    expect(() => encodeValue(1n)).toThrow(TypeError)
  })
  test("byte length is UTF-8 of the JSON", () => {
    expect(jsonByteLength("é")).toBe(4)
    expect(jsonByteLength("x".repeat(1_048_574))).toBe(1_048_576)
  })
  test("durations", () => {
    expect(parseDuration("500ms")).toBe(500)
    expect(parseDuration("2s")).toBe(2000)
    expect(parseDuration("5m")).toBe(300_000)
    expect(parseDuration("1h")).toBe(3_600_000)
    expect(parseDuration("3d")).toBe(259_200_000)
    expect(parseDuration(1500)).toBe(1500)
    expect(() => parseDuration("soon")).toThrow(TypeError)
    expect(() => parseDuration(-1)).toThrow(TypeError)
  })
})

describe("step IDs and tokens", () => {
  test("step IDs are 32 hex characters and depend on the occurrence", async () => {
    const a = await stepId("send", 0)
    expect(a).toMatch(/^[0-9a-f]{32}$/)
    expect(await stepId("send", 1)).not.toBe(a)
  })
  test("invocation token shape", () => {
    expect(isInvocationToken("03f.0199a3d0-2b3c-703f-9f50-6b7c8d9e0f1a.inv_abcdefghijklmnopqrstuvwxyz." + "A".repeat(43))).toBe(true)
    expect(isInvocationToken("03f.0199a3d0-2b3c-7d4e-9f50-6b7c8d9e0f1a.inv_abcdefghijklmnopqrstuvwxyz." + "A".repeat(43))).toBe(false) // rand_a's top bits set
    expect(isInvocationToken("ezgh_" + "A".repeat(40))).toBe(false)
  })
})

test("SDK_VERSION matches package.json", () => expect(SDK_VERSION).toBe(pkg.version))
test("SDK_NAME matches package.json", () => expect(SDK_NAME).toBe(pkg.name))
