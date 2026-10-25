// A fake daemon behind an injected transport: it answers the wire protocol
// in memory and records every request.
import type { Spawn, Transport, TransportHandlers } from "../src/connection"

export interface Request {
  id: number
  method: string
  params?: Record<string, unknown>
}

export class FakeDaemon {
  // Every request of every connection, in order.
  requests: Request[] = []
  connections: FakeConnection[] = []
  // A method with an error answers it instead of the result.
  errors = new Map<string, { code: number; message: string }>()
  // A method in hold gets no response until respond is called.
  hold = new Set<string>()
  // A method with a result here answers it instead of the usual result.
  results = new Map<string, unknown>()
  // The name register assigns instead of the one asked for.
  rename?: string
  // A dead daemon: each bridge exits right after it starts.
  dead = false
  registered = 0

  spawn: Spawn = (h) => {
    const c = new FakeConnection(this, h)
    this.connections.push(c)
    if (this.dead) queueMicrotask(() => c.die())
    return c
  }

  get last(): FakeConnection {
    const c = this.connections.at(-1)
    if (!c) throw new Error("no connection")
    return c
  }

  result(method: string, params: Record<string, unknown> | undefined): unknown {
    if (this.results.has(method)) return this.results.get(method)
    switch (method) {
      case "register": {
        this.registered++
        const name = this.rename ?? (typeof params?.name === "string" ? params.name : "opencode-repo")
        const topics = Array.isArray(params?.topics) ? params.topics : ["o_repo"]
        return { session: `S${this.registered}`, name, topics, protocol: `PROTOCOL for ${name}`, version: params?.version }
      }
      case "send":
        return { id: "01ID", recipients: ["claude-repo"], skipped: [], text: "Sent 01ID to claude-repo." }
      case "pending":
        return { text: "1 pending message: ...", total: 1 }
      case "ack":
        return { text: `Acknowledged ${String(params?.id)}.` }
      case "agents":
        return { agents: [], total: 0, text: "o_repo: opencode-repo (live)" }
    }
    return undefined
  }
}

export class FakeConnection implements Transport {
  closed = false
  private held: Request[] = []

  constructor(
    private readonly daemon: FakeDaemon,
    private readonly h: TransportHandlers,
  ) {}

  send(line: string): boolean {
    if (this.closed) return false
    const req = JSON.parse(line) as Request
    this.daemon.requests.push(req)
    if (this.daemon.hold.has(req.method)) this.held.push(req)
    else queueMicrotask(() => this.answer(req))
    return true
  }

  // respond answers the held requests.
  respond(): void {
    const held = this.held
    this.held = []
    for (const r of held) this.answer(r)
  }

  // write sends a raw line from the daemon.
  write(msg: unknown): void {
    if (!this.closed) this.h.line(typeof msg === "string" ? msg : JSON.stringify(msg))
  }

  deliver(params: unknown): void {
    this.write({ jsonrpc: "2.0", method: "deliver", params })
  }

  // die is a bridge that exits.
  die(): void {
    if (this.closed) return
    this.closed = true
    this.h.closed()
  }

  close(): void {
    this.die()
  }

  private answer(req: Request): void {
    const err = this.daemon.errors.get(req.method)
    if (err) this.write({ jsonrpc: "2.0", id: req.id, error: err })
    else this.write({ jsonrpc: "2.0", id: req.id, result: this.daemon.result(req.method, req.params) })
  }
}

export const tick = () => new Promise((r) => setTimeout(r, 0))

export async function until(cond: () => boolean, ms = 2000): Promise<void> {
  const end = Date.now() + ms
  while (!cond()) {
    if (Date.now() > end) throw new Error("timed out")
    await new Promise((r) => setTimeout(r, 5))
  }
}

export function deferred<T = void>() {
  let resolve!: (v: T) => void
  const promise = new Promise<T>((r) => (resolve = r))
  return { promise, resolve }
}
