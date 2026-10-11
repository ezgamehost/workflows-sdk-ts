// Type-level checks (tsc --noEmit runs them): Jsonify and the open unions.
import { expect, test } from "bun:test"
import type { App, Jsonify, Run } from "../src/index.ts"

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false
const assert = <T extends true>(_: T) => {}

test("Jsonify", () => {
  assert<Equal<Jsonify<{ a?: Date }>, { a?: string }>>(true)
  assert<Equal<Jsonify<{ a: number | undefined; b: string }>, { a?: number; b: string }>>(true)
  assert<Equal<Jsonify<(Date | undefined)[]>, (string | null)[]>>(true)
  assert<Equal<Jsonify<undefined>, null>>(true)
  assert<Equal<Jsonify<{ n: { d: Date } }>, { n: { d: string } }>>(true)
  expect(true).toBe(true)
})

test("RunStatus and App.kind stay open to values a later API adds", () => {
  const s: Run["status"] = "paused"
  const k: App["kind"] = "scheduled"
  expect([s, k]).toEqual(["paused", "scheduled"])
})

test("step.invoke types its input as the child workflow's", () => {
  const { workflow } = require("../src/index.ts") as typeof import("../src/index.ts")
  const child = workflow("child", async ({ input }: { input: { n: number } }) => input.n * 10)
  workflow("parent", async ({ step }) => {
    const ok: number = await step.invoke("a", { workflow: child, input: { n: 3 } })
    // @ts-expect-error input isn't the child's
    await step.invoke("b", { workflow: child, input: { n: "three" } })
    // @ts-expect-error a missing field
    await step.invoke("c", { workflow: child, input: {} })
    const byName: unknown = await step.invoke("d", { workflow: "child", input: "anything" })
    return [ok, byName]
  })
  expect(true).toBe(true)
})
