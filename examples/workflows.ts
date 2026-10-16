import { workflow } from "@ezghcloud/workflows"

export const onboarding = workflow("onboarding", async ({ input, step }) => {
  const user = await step.run("load-user", () => ({ id: (input as { userId: string }).userId, email: "ada@example.test" }))
  await step.run("welcome-email", () => `sent to ${user.email}`)
  await step.sleep("cooldown", "3d")
  return step.run("follow-up", () => "done")
})
