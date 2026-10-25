// The plugin wiring against a fake OpenCode context, with a fake bridge
// command as AGENT_COURIER_BIN.
import { afterAll, expect, spyOn, test } from "bun:test"
import { join } from "node:path"
import plugin from "../index"
import * as module from "../index"
import { until } from "./fake"

process.env.AGENT_COURIER_BIN = join(import.meta.dir, "fake-bridge.ts")
delete process.env.AGENT_COURIER_NAME
delete process.env.AGENT_COURIER_TOPICS

const logs: string[] = []
spyOn(console, "error").mockImplementation((...args: unknown[]) => {
  logs.push(args.join(" "))
})

// A fake event stream that the test feeds.
const events: { type: string; data?: unknown }[] = []
let wake = () => {}
async function* subscribe(options?: { signal?: AbortSignal }) {
  while (!options?.signal?.aborted) {
    const ev = events.shift()
    if (ev) yield ev
    else await new Promise<void>((r) => (wake = r))
  }
}
function emit(type: string, sessionID: string) {
  events.push({ type, data: { sessionID } })
  wake()
}

const tools: any[] = []
const namespaces: any[] = []
const hooks: Record<string, (e: any) => Promise<void>> = {}
const synthetic: any[] = []
const disposed: string[] = []
const ctx: any = {
  session: {
    get: async ({ sessionID }: { sessionID: string }) => ({
      parentID: sessionID === "ses_child" ? "ses_top" : undefined,
      location: { directory: "/work" },
    }),
    synthetic: async (input: unknown) => {
      synthetic.push(input)
      return {}
    },
    hook: async (name: string, cb: any) => {
      hooks[name] = cb
      return { dispose: async () => void disposed.push(`hook ${name}`) }
    },
  },
  tool: {
    transform: async (cb: any) => {
      cb({ namespace: (ns: unknown) => namespaces.push(ns), add: (t: unknown) => tools.push(t) })
      return { dispose: async () => void disposed.push("tools") }
    },
  },
  event: { subscribe },
}

const cleanup = (await plugin.setup(ctx)) as () => Promise<void>
let cleaned = false
afterAll(async () => {
  if (!cleaned) await cleanup()
  wake()
})

const started = () => logs.filter((l) => l === "agent-courier: bridge: fake bridge started").length
const exits = () => logs.filter((l) => l.includes("bridge exited")).length

const tool = (name: string) => tools.find((t) => t.name === name)
const call = (name: string, sessionID: string, input: Record<string, unknown> = {}) =>
  tool(name).execute(input, { sessionID, signal: new AbortController().signal })

test("the module exports only the plugin definition", () => {
  expect(Object.keys(module)).toEqual(["default"])
  expect(plugin.id).toBe("agent-courier")
  expect(typeof plugin.setup).toBe("function")
})

test("the four tools are in the courier namespace", () => {
  expect(namespaces.map((n) => n.name)).toEqual(["courier"])
  expect(tools.map((t) => [t.name, t.options.namespace])).toEqual([
    ["send", "courier"],
    ["pending", "courier"],
    ["ack", "courier"],
    ["agents", "courier"],
  ])
})

test("execution start registers through the bridge; the context gets the protocol; the push goes to synthetic", async () => {
  emit("session.execution.started", "ses_top")
  await until(() => synthetic.length === 1)
  expect(synthetic[0]).toEqual({
    sessionID: "ses_top",
    text: "[courier] note from claude-fake",
    description: "courier: note from claude-fake",
    metadata: { id: "01ID", topic: "o_fake", sender: "claude-fake", kind: "note", to: "opencode-fake" },
    delivery: "steer",
  })
  const event = { sessionID: "ses_top", system: [{ type: "text", text: "base" }] }
  await hooks.context!(event)
  await hooks.context!(event)
  expect(event.system).toEqual([
    { type: "text", text: "base" },
    { type: "text", text: "FAKE PROTOCOL" },
  ])
  expect(logs).toContain("agent-courier: bridge: fake bridge started")
})

test("a tool returns the daemon's text through the bridge", async () => {
  expect(await call("pending", "ses_top")).toEqual({ content: "fake pending for no params" })
})

test("a bridge that exits fails the call, and the next call starts a new bridge", async () => {
  await expect(call("send", "ses_top", { topic: "o_fake", kind: "note", summary: "bye" })).rejects.toThrow(
    "the courier daemon is not reachable",
  )
  await until(() => logs.some((l) => l.includes("bridge exited (code 3)")))
  expect(await call("pending", "ses_top")).toEqual({ content: "fake pending for no params" })
  expect(logs.filter((l) => l === "agent-courier: bridge: fake bridge started").length).toBe(2)
})

test("a child session gets no protocol and no tools", async () => {
  const event = { sessionID: "ses_child", system: [] }
  await hooks.context!(event)
  expect(event.system).toEqual([])
  await expect(call("pending", "ses_child")).rejects.toThrow("top-level session")
})

test("the context hook of a session with no execution starts no bridge", async () => {
  const before = started()
  const event = { sessionID: "ses_quiet", system: [] }
  for (let i = 0; i < 3; i++) await hooks.context!(event)
  expect(event.system).toEqual([])
  await new Promise((r) => setTimeout(r, 100))
  expect(started()).toBe(before)
})

test.each(["session.deleted", "session.archived"])("%s ends the bridge", async (type) => {
  expect(await call("pending", "ses_top")).toEqual({ content: "fake pending for no params" })
  const before = exits()
  emit(type, "ses_top")
  await until(() => exits() === before + 1)
  expect(logs.at(-1)).toContain("bridge exited (code 0)")
})

test("cleanup ends the bridges, disposes the registrations, and nothing starts a bridge after it", async () => {
  expect(await call("pending", "ses_top")).toEqual({ content: "fake pending for no params" })
  const before = exits()
  const bridges = started()
  cleaned = true
  await cleanup()
  await until(() => exits() === before + 1)
  expect(disposed).toEqual(["tools", "hook context"])
  await expect(call("pending", "ses_top")).rejects.toThrow(/^the courier daemon is not reachable$/)
  const event = { sessionID: "ses_top", system: [] }
  await hooks.context!(event)
  expect(event.system).toEqual([])
  await new Promise((r) => setTimeout(r, 100))
  expect(started()).toBe(bridges)
})
