import { describe, expect, test } from "bun:test"
import { Connection, MAX_LINE, UNREACHABLE, type Deliver } from "../src/connection"
import { Courier, NOT_TOP_LEVEL, VERSION, type Host } from "../src/courier"
import pkg from "../package.json" with { type: "json" }
import { FakeDaemon, deferred, tick, until } from "./fake"

const TOP = "ses_top"
const CHILD = "ses_child"

function setup(over: Partial<Host> = {}) {
  const daemon = new FakeDaemon()
  const pushes: { sessionID: string; d: Deliver }[] = []
  const logs: string[] = []
  const courier = new Courier({
    spawn: daemon.spawn,
    sessionInfo: async (id) => ({ parentID: id === CHILD ? TOP : undefined, directory: `/work/${id}` }),
    push: async (sessionID, d) => {
      pushes.push({ sessionID, d })
    },
    log: (m) => logs.push(m),
    ...over,
  })
  return { daemon, courier, pushes, logs }
}

describe("tools", () => {
  test("register once with the plugin version and the session directory", async () => {
    const { daemon, courier } = setup({ name: "asked", topics: ["a", "b"] })
    await courier.call(TOP, "pending")
    await courier.call(TOP, "agents")
    expect(daemon.connections.length).toBe(1)
    expect(daemon.requests.map((r) => r.method)).toEqual(["register", "pending", "agents"])
    expect(daemon.requests[0]?.params).toEqual({
      harness: "opencode",
      version: pkg.version,
      cwd: `/work/${TOP}`,
      name: "asked",
      topics: ["a", "b"],
    })
    expect(VERSION).toBe(pkg.version)
  })

  test("each tool returns the daemon's text", async () => {
    const { daemon, courier } = setup()
    expect(await courier.call(TOP, "send", { topic: "o_repo", kind: "ask", summary: "which branch?", to: "claude-repo" })).toBe(
      "Sent 01ID to claude-repo.",
    )
    expect(await courier.call(TOP, "pending")).toBe("1 pending message: ...")
    expect(await courier.call(TOP, "ack", { id: "01ID" })).toBe("Acknowledged 01ID.")
    expect(await courier.call(TOP, "agents")).toBe("o_repo: opencode-repo (live)")
    expect(daemon.requests.slice(1)).toEqual([
      { jsonrpc: "2.0", id: 2, method: "send", params: { topic: "o_repo", kind: "ask", summary: "which branch?", to: "claude-repo" } },
      { jsonrpc: "2.0", id: 3, method: "pending" },
      { jsonrpc: "2.0", id: 4, method: "ack", params: { id: "01ID" } },
      { jsonrpc: "2.0", id: 5, method: "agents" },
    ] as never)
  })

  test("a daemon error throws the daemon's message unchanged", async () => {
    const { daemon, courier } = setup()
    daemon.errors.set("send", { code: -32010, message: "claude-x is not an agent known on topic o_repo" })
    await expect(courier.call(TOP, "send", { topic: "o_repo", kind: "note", summary: "hi", to: "claude-x" })).rejects.toThrow(
      /^claude-x is not an agent known on topic o_repo$/,
    )
  })

  test("a request over the line cap is refused before it is written", async () => {
    const { daemon, courier } = setup()
    await courier.call(TOP, "pending")
    await expect(courier.call(TOP, "send", { topic: "t", kind: "note", summary: "x".repeat(MAX_LINE) })).rejects.toThrow(
      `the request has more than ${MAX_LINE} bytes`,
    )
    expect(daemon.requests.map((r) => r.method)).toEqual(["register", "pending"])
  })
})

describe("dead bridge", () => {
  test("a waiting call fails and the next call starts a new bridge that registers again", async () => {
    const { daemon, courier } = setup()
    await courier.call(TOP, "pending")
    daemon.hold.add("send")
    const waiting = courier.call(TOP, "send", { topic: "o_repo", kind: "note", summary: "hi" })
    await until(() => daemon.requests.length === 2)
    daemon.last.die()
    await expect(waiting).rejects.toThrow(UNREACHABLE)
    await tick()
    daemon.hold.clear()
    expect(await courier.call(TOP, "pending")).toBe("1 pending message: ...")
    expect(daemon.connections.length).toBe(2)
    // The second registration asks for the name and topics the daemon assigned.
    expect(daemon.requests.filter((r) => r.method === "register").map((r) => r.params)).toEqual([
      { harness: "opencode", version: VERSION, cwd: `/work/${TOP}` },
      { harness: "opencode", version: VERSION, cwd: `/work/${TOP}`, name: "opencode-repo", topics: ["o_repo"] },
    ])
    // The send was not sent again.
    expect(daemon.requests.filter((r) => r.method === "send").length).toBe(1)
  })

  test("a bridge that exits before register answers gives the unreachable error", async () => {
    const { daemon, courier } = setup()
    daemon.hold.add("register")
    const call = courier.call(TOP, "pending")
    await until(() => daemon.connections.length === 1)
    daemon.last.die()
    await expect(call).rejects.toThrow(new RegExp(`^${UNREACHABLE}$`))
  })

  test.each([
    [-32004, "the daemon runs 0.0.1 and the client 0.0.0"],
    [-32602, "the name opencode-x is taken"],
  ])("a refused registration (%d) stops registration from events until a tool call", async (code, msg) => {
    const { daemon, courier } = setup()
    daemon.errors.set("register", { code, message: msg })
    await expect(courier.call(TOP, "pending")).rejects.toThrow(new RegExp(`^${msg}$`))
    await until(() => daemon.last.closed)
    courier.started(TOP)
    courier.started(TOP)
    await tick()
    expect(await courier.protocol(TOP)).toBeUndefined()
    expect(daemon.connections.length).toBe(1)
    daemon.errors.clear()
    expect(await courier.call(TOP, "pending")).toBe("1 pending message: ...")
    expect(daemon.connections.length).toBe(2)
  })

  test("an invalid registration gives the unreachable error, not the raw error, and releases no push", async () => {
    const { daemon, courier, logs, pushes } = setup()
    daemon.results.set("register", { name: "opencode-repo" })
    daemon.hold.add("register")
    const call = courier.call(TOP, "pending")
    await until(() => daemon.requests.length === 1)
    daemon.last.deliver({ text: "held", notice: "", attrs: {} })
    daemon.last.respond()
    await expect(call).rejects.toThrow(new RegExp(`^${UNREACHABLE}$`))
    expect(logs).toContain(`register ${TOP}: the courier daemon sent an invalid registration`)
    await tick()
    expect(pushes).toEqual([])
  })
})

describe("retry", () => {
  test("the context hook never starts a bridge; one execution starts at most one", async () => {
    const { daemon, courier, logs } = setup()
    daemon.dead = true
    for (let i = 0; i < 5; i++) expect(await courier.protocol(TOP)).toBeUndefined()
    expect(daemon.connections.length).toBe(0)

    courier.started(TOP)
    await until(() => logs.includes(`register ${TOP}: ${UNREACHABLE}`))
    for (let i = 0; i < 3; i++) await expect(courier.call(TOP, "pending")).rejects.toThrow(new RegExp(`^${UNREACHABLE}$`))
    for (let i = 0; i < 5; i++) expect(await courier.protocol(TOP)).toBeUndefined()
    expect(daemon.connections.length).toBe(1)
  })

  test("with no backoff left, a failed registration still waits for the next execution", async () => {
    const { daemon, courier, logs } = setup({ retryMs: 0 })
    daemon.dead = true
    courier.started(TOP)
    await until(() => logs.length === 1)
    await new Promise((r) => setTimeout(r, 5))
    for (let i = 0; i < 3; i++) await expect(courier.call(TOP, "pending")).rejects.toThrow(new RegExp(`^${UNREACHABLE}$`))
    expect(daemon.connections.length).toBe(1)
    courier.started(TOP)
    await until(() => logs.length === 2)
    expect(daemon.connections.length).toBe(2)
  })

  test("a failed registration retries at a later execution after the backoff", async () => {
    const { daemon, courier, logs } = setup({ retryMs: 80 })
    daemon.dead = true
    courier.started(TOP)
    await until(() => logs.length === 1)
    // A new execution inside the backoff does not retry.
    courier.started(TOP)
    await tick()
    expect(daemon.connections.length).toBe(1)
    await new Promise((r) => setTimeout(r, 100))
    daemon.dead = false
    expect(await courier.call(TOP, "pending")).toBe("1 pending message: ...")
    expect(daemon.connections.length).toBe(2)
  })

  test("a daemon error at register backs off as a transport error does", async () => {
    const { daemon, courier, logs } = setup()
    daemon.errors.set("register", { code: -32603, message: "the store failed" })
    courier.started(TOP)
    await until(() => logs.includes(`register ${TOP}: the store failed`))
    await expect(courier.call(TOP, "pending")).rejects.toThrow(new RegExp(`^${UNREACHABLE}$`))
    courier.started(TOP)
    await tick()
    expect(daemon.connections.length).toBe(1)
  })
})

describe("cleanup", () => {
  test("after dispose no entry point starts a bridge", async () => {
    const { daemon, courier } = setup()
    await courier.call(TOP, "pending")
    courier.dispose()
    expect(daemon.last.closed).toBe(true)
    await expect(courier.call(TOP, "pending")).rejects.toThrow(new RegExp(`^${UNREACHABLE}$`))
    courier.started(TOP)
    courier.started("ses_new")
    expect(await courier.protocol(TOP)).toBeUndefined()
    await tick()
    expect(daemon.connections.length).toBe(1)
  })

  test("a lookup that returns after dispose starts no bridge", async () => {
    const info = deferred<{ directory: string }>()
    const { daemon, courier } = setup({ sessionInfo: () => info.promise })
    courier.started(TOP)
    const call = courier.call(TOP, "pending")
    courier.dispose()
    info.resolve({ directory: "/work" })
    await expect(call).rejects.toThrow(new RegExp(`^${UNREACHABLE}$`))
    await tick()
    expect(daemon.connections.length).toBe(0)
  })
})

describe("sessions", () => {
  test("execution start registers, and the context gets the protocol text", async () => {
    const { daemon, courier } = setup()
    courier.started(TOP)
    await until(() => daemon.registered === 1)
    expect(await courier.protocol(TOP)).toBe("PROTOCOL for opencode-repo")
    expect(daemon.connections.length).toBe(1)
  })

  test("a child session never registers", async () => {
    const { daemon, courier } = setup()
    courier.started(CHILD)
    expect(await courier.protocol(CHILD)).toBeUndefined()
    await expect(courier.call(CHILD, "pending")).rejects.toThrow(NOT_TOP_LEVEL)
    await tick()
    expect(daemon.connections.length).toBe(0)
  })

  test("two sessions are two connections, and each push lands in its own session", async () => {
    const { daemon, courier, pushes } = setup()
    await courier.call(TOP, "pending")
    await courier.call("ses_other", "pending")
    expect(daemon.connections.length).toBe(2)
    daemon.connections[1]!.deliver({ text: "to other", notice: "", attrs: {} })
    daemon.connections[0]!.deliver({ text: "to top", notice: "", attrs: {} })
    await until(() => pushes.length === 2)
    expect(pushes.map((p) => [p.sessionID, p.d.text])).toEqual([
      ["ses_other", "to other"],
      [TOP, "to top"],
    ])
  })

  test("a session deleted during its first lookup is not added", async () => {
    const info = deferred<{ directory: string }>()
    const { daemon, courier } = setup({ sessionInfo: () => info.promise })
    courier.started(TOP)
    const call = courier.call(TOP, "pending")
    courier.close(TOP)
    info.resolve({ directory: "/work" })
    await expect(call).rejects.toThrow(new RegExp(`^${UNREACHABLE}$`))
    await tick()
    expect(daemon.connections.length).toBe(0)
    expect(await courier.protocol(TOP)).toBeUndefined()
  })

  test("a session closed during register drops the connection and its pushes", async () => {
    const { daemon, courier, pushes } = setup()
    daemon.hold.add("register")
    courier.started(TOP)
    await until(() => daemon.requests.length === 1)
    daemon.last.deliver({ text: "held", notice: "", attrs: {} })
    courier.close(TOP)
    expect(daemon.last.closed).toBe(true)
    await tick()
    expect(pushes).toEqual([])
    expect(await courier.protocol(TOP)).toBeUndefined()
  })

  test("deleted, archived and cleanup close the connection", async () => {
    const { daemon, courier } = setup()
    await courier.call(TOP, "pending")
    await courier.call("ses_b", "pending")
    courier.close(TOP)
    expect(daemon.connections[0]?.closed).toBe(true)
    expect(daemon.connections[1]?.closed).toBe(false)
    courier.dispose()
    expect(daemon.connections[1]?.closed).toBe(true)
  })

  test("a session with no execution for the idle time closes its connection", async () => {
    const { daemon, courier } = setup({ idleMs: 30 })
    await courier.call(TOP, "pending")
    await until(() => daemon.last.closed, 1000)
    await courier.call(TOP, "pending")
    expect(daemon.connections.length).toBe(2)
  })
})

describe("push", () => {
  test("the push carries only the daemon's text, notice and attrs", async () => {
    const { daemon, courier, pushes } = setup()
    await courier.call(TOP, "pending")
    daemon.last.deliver({
      text: "[courier] handoff from claude-repo\n| fix the build",
      notice: "courier: handoff from claude-repo",
      attrs: { id: "01ID", topic: "o_repo", sender: "claude-repo", kind: "handoff", to: "opencode-repo", issue: 7 },
      summary: "raw summary that must not reach the session",
      sender: "raw sender",
    })
    await until(() => pushes.length === 1)
    expect(pushes[0]).toEqual({
      sessionID: TOP,
      d: {
        text: "[courier] handoff from claude-repo\n| fix the build",
        notice: "courier: handoff from claude-repo",
        attrs: { id: "01ID", topic: "o_repo", sender: "claude-repo", kind: "handoff", to: "opencode-repo" },
      },
    })
  })

  test("a deliver without text is dropped", async () => {
    const { daemon, courier, pushes } = setup()
    await courier.call(TOP, "pending")
    daemon.last.deliver({ notice: "n", attrs: {}, summary: "raw" })
    daemon.last.deliver({ text: 42, attrs: {} })
    await tick()
    expect(pushes).toEqual([])
  })

  test("a deliver before the register response waits for it, and pushes keep their order", async () => {
    const { daemon, courier, pushes } = setup()
    daemon.hold.add("register")
    courier.started(TOP)
    await until(() => daemon.connections.length === 1 && daemon.requests.length === 1)
    daemon.last.deliver({ text: "first", notice: "", attrs: {} })
    daemon.last.deliver({ text: "second", notice: "", attrs: {} })
    await tick()
    expect(pushes).toEqual([])
    daemon.last.respond()
    await until(() => pushes.length === 2)
    expect(pushes.map((p) => p.d.text)).toEqual(["first", "second"])
    expect(await courier.protocol(TOP)).toBe("PROTOCOL for opencode-repo")
  })

  test("a session closed after the register response but before it is handled drops its pushes", async () => {
    const { daemon, courier, pushes } = setup()
    daemon.hold.add("register")
    courier.started(TOP)
    await until(() => daemon.requests.length === 1)
    daemon.last.deliver({ text: "held", notice: "", attrs: {} })
    daemon.last.respond()
    courier.close(TOP)
    await tick()
    await tick()
    expect(pushes).toEqual([])
    expect(await courier.protocol(TOP)).toBeUndefined()
  })

  test("a slow push holds the next one back", async () => {
    const first = deferred()
    const order: string[] = []
    const { daemon, courier } = setup({
      push: async (_, d) => {
        order.push(`start ${d.text}`)
        if (d.text === "first") await first.promise
        order.push(`end ${d.text}`)
      },
    })
    await courier.call(TOP, "pending")
    daemon.last.deliver({ text: "first", notice: "", attrs: {} })
    daemon.last.deliver({ text: "second", notice: "", attrs: {} })
    await until(() => order.length === 1)
    for (let i = 0; i < 5; i++) await tick()
    expect(order).toEqual(["start first"])
    first.resolve()
    await until(() => order.length === 4)
    expect(order).toEqual(["start first", "end first", "start second", "end second"])
  })

  test("after a rename the context gets the new protocol before the next push", async () => {
    const seen: (string | undefined)[] = []
    let courier!: Courier
    const daemon = new FakeDaemon()
    courier = new Courier({
      spawn: daemon.spawn,
      sessionInfo: async () => ({ directory: "/work" }),
      push: async () => {
        seen.push(await courier.protocol(TOP))
      },
      log: () => {},
    })
    await courier.call(TOP, "pending")
    expect(await courier.protocol(TOP)).toBe("PROTOCOL for opencode-repo")
    daemon.last.die()
    daemon.rename = "opencode-repo-2"
    daemon.hold.add("register")
    courier.started(TOP)
    await until(() => daemon.connections.length === 2 && daemon.requests.length === 3)
    daemon.last.deliver({ text: "after the rename", notice: "", attrs: {} })
    await tick()
    expect(seen).toEqual([])
    daemon.last.respond()
    await until(() => seen.length === 1)
    expect(seen).toEqual(["PROTOCOL for opencode-repo-2"])
  })

  test("a tool result is passed on whole", async () => {
    const { daemon, courier } = setup()
    const text = "p".repeat(60_000) + " end"
    daemon.results.set("pending", { text, total: 1 })
    expect(await courier.call(TOP, "pending")).toBe(text)
  })

  test("a line that is not JSON breaks the connection", async () => {
    const { daemon, courier } = setup()
    await courier.call(TOP, "pending")
    daemon.last.write("not json")
    expect(daemon.last.closed).toBe(true)
  })
})

test("Connection rejects calls after close", async () => {
  const daemon = new FakeDaemon()
  const c = new Connection(daemon.spawn, () => {}, () => {})
  c.close()
  await expect(c.call("pending")).rejects.toThrow(UNREACHABLE)
})
