// One daemon connection, which is one courier session: JSON-RPC 2.0, one
// object per line, over the stdio of an `agent-courier bridge` child.
// design.md "Socket and wire protocol" is the protocol.
import { spawn } from "node:child_process"
import { createInterface } from "node:readline"

export const UNREACHABLE = "the courier daemon is not reachable"

// daemon.MaxLine: the most bytes of a line without its newline. The bridge
// stops at a longer line in either direction.
export const MAX_LINE = 65536

export interface TransportHandlers {
  line(line: string): void
  closed(): void
}

export interface Transport {
  // send writes one line and returns false when the transport is closed.
  send(line: string): boolean
  close(): void
}

export type Spawn = (handlers: TransportHandlers) => Transport

// Deliver is the params of a deliver notification. Every field comes from
// the daemon: text is message.Render, notice is message.Notice, attrs are
// the validated message attributes.
export interface Deliver {
  text: string
  notice: string
  attrs: Record<string, string>
}

export class DaemonError extends Error {
  constructor(
    readonly code: number,
    message: string,
  ) {
    super(message)
  }
}

interface Waiting {
  resolve(result: unknown): void
  reject(err: Error): void
}

export class Connection {
  closed = false
  private readonly transport: Transport
  private next = 0
  private readonly waiting = new Map<number, Waiting>()
  // Deliver notifications wait here until release, so the register response
  // is handled before the first push of a connection.
  private held: Deliver[] | undefined = []

  constructor(
    spawnTransport: Spawn,
    private readonly onDeliver: (d: Deliver) => void,
    private readonly onClose: () => void,
  ) {
    this.transport = spawnTransport({ line: (l) => this.handle(l), closed: () => this.shut() })
  }

  // call sends a request and resolves with its result. A daemon error is a
  // DaemonError with the daemon's message. A broken connection is UNREACHABLE,
  // also for a call that was waiting: the call is never sent again.
  call(method: string, params?: Record<string, unknown>, signal?: AbortSignal): Promise<unknown> {
    if (this.closed) return Promise.reject(new Error(UNREACHABLE))
    const id = ++this.next
    const line = JSON.stringify({ jsonrpc: "2.0", id, method, ...(params ? { params } : {}) })
    if (Buffer.byteLength(line) > MAX_LINE) {
      return Promise.reject(new Error(`the request has more than ${MAX_LINE} bytes`))
    }
    return new Promise((resolve, reject) => {
      const abort = () => {
        this.waiting.delete(id)
        reject(signal?.reason instanceof Error ? signal.reason : new Error("the call was aborted"))
      }
      if (signal?.aborted) return abort()
      signal?.addEventListener("abort", abort, { once: true })
      const done = () => signal?.removeEventListener("abort", abort)
      this.waiting.set(id, {
        resolve: (r) => (done(), resolve(r)),
        reject: (e) => (done(), reject(e)),
      })
      if (!this.transport.send(line)) {
        this.waiting.delete(id)
        done()
        reject(new Error(UNREACHABLE))
      }
    })
  }

  // release starts handing deliver notifications to onDeliver.
  release(): void {
    const held = this.held ?? []
    this.held = undefined
    for (const d of held) this.onDeliver(d)
  }

  close(): void {
    this.transport.close()
    this.shut()
  }

  private shut(): void {
    if (this.closed) return
    this.closed = true
    for (const w of this.waiting.values()) w.reject(new Error(UNREACHABLE))
    this.waiting.clear()
    this.onClose()
  }

  // handle reads one line from the daemon. A line that is not a JSON-RPC
  // object breaks the connection, as in the Go client.
  private handle(line: string): void {
    if (line.trim() === "") return
    let msg: unknown
    try {
      msg = JSON.parse(line)
    } catch {
      return this.close()
    }
    if (!isObject(msg)) return this.close()
    if (typeof msg.method === "string") {
      if (msg.method !== "deliver") return
      const d = parseDeliver(msg.params)
      if (!d) return
      if (this.held) this.held.push(d)
      else this.onDeliver(d)
      return
    }
    if (typeof msg.id !== "number") return
    const w = this.waiting.get(msg.id)
    if (!w) return
    this.waiting.delete(msg.id)
    if (msg.error !== undefined) {
      const e = isObject(msg.error) ? msg.error : {}
      const code = typeof e.code === "number" ? e.code : 0
      w.reject(new DaemonError(code, typeof e.message === "string" ? e.message : "the courier daemon sent an invalid error"))
    } else {
      w.resolve(msg.result)
    }
  }
}

// parseDeliver keeps the three fields of a deliver and nothing else: text
// and notice as strings, attrs as an object of strings.
export function parseDeliver(params: unknown): Deliver | undefined {
  if (!isObject(params) || typeof params.text !== "string" || params.text === "") return undefined
  const attrs: Record<string, string> = {}
  if (isObject(params.attrs)) {
    for (const [k, v] of Object.entries(params.attrs)) if (typeof v === "string") attrs[k] = v
  }
  return { text: params.text, notice: typeof params.notice === "string" ? params.notice : "", attrs }
}

export function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v)
}

// bridgeSpawner starts `<bin> bridge` for each connection. The bridge runs
// the Go client's Dial: the socket directory and peer uid checks and the
// autostart. Its stderr goes to log.
export function bridgeSpawner(bin: string, log: (msg: string) => void): Spawn {
  return (h) => {
    const child = spawn(bin, ["bridge"], { stdio: ["pipe", "pipe", "pipe"] })
    let closed = false
    const close = () => {
      if (closed) return
      closed = true
      h.closed()
    }
    child.on("error", (err) => {
      log(`bridge: ${err.message}`)
      close()
    })
    child.on("close", (code, signal) => {
      log(`bridge exited (${signal ?? `code ${code}`})`)
      close()
    })
    // A write after the bridge exited fails with EPIPE; the close handler
    // reports the broken connection.
    child.stdin.on("error", () => {})
    createInterface({ input: child.stdout, crlfDelay: Infinity }).on("line", (l) => h.line(l))
    createInterface({ input: child.stderr, crlfDelay: Infinity }).on("line", (l) => log(`bridge: ${l}`))
    return {
      send(line) {
        if (closed || !child.stdin.writable) return false
        child.stdin.write(line + "\n")
        return true
      },
      close() {
        // Closed stdin ends the bridge.
        child.stdin.end()
      },
    }
  }
}
