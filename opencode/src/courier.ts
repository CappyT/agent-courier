// The courier sessions of one plugin instance: one daemon connection per
// top-level OpenCode session. design.md "OpenCode client".
import pkg from "../package.json" with { type: "json" }
import { Connection, DaemonError, UNREACHABLE, isObject, type Deliver, type Spawn } from "./connection"

export const VERSION: string = pkg.version

// A session with no execution for this long frees its agent name.
export const IDLE_MS = 30 * 60 * 1000
// The context hook waits this long for a registration in flight, as the
// Claude Code client waits for the daemon before it answers initialize.
export const CONTEXT_WAIT_MS = 5000
const REGISTER_TIMEOUT_MS = 10_000
// After a failed registration the next attempt waits for a new execution and
// for this time, doubled at each failure up to RETRY_MAX_MS.
export const RETRY_MS = 1000
const RETRY_MAX_MS = 60_000

export const NOT_TOP_LEVEL = "the courier tools work only in a top-level session, not in a subagent session"

export interface SessionInfo {
  parentID?: string
  directory: string
}

export interface Host {
  spawn: Spawn
  sessionInfo(sessionID: string): Promise<SessionInfo>
  // push gives a deliver to the session. It passes only the daemon's fields.
  push(sessionID: string, d: Deliver): Promise<unknown>
  log(msg: string): void
  // AGENT_COURIER_NAME and AGENT_COURIER_TOPICS.
  name?: string
  topics?: string[]
  idleMs?: number
  contextWaitMs?: number
  retryMs?: number
}

interface Session {
  directory: string
  conn?: Connection
  connecting?: Promise<Connection>
  // The connection whose register is in flight, so close can end it.
  registering?: Connection
  // What the daemon assigned at the last registration; a new connection asks
  // for it again.
  name?: string
  topics?: string[]
  protocol?: string
  // The message of a -32004 or -32602 from register. Only a tool call tries
  // again, because each attempt cancels the idle exit of the old daemon.
  refused?: string
  // Executions started so far; any other failed registration blocks a new
  // attempt until a later execution and retryAt.
  execution: number
  failures: number
  failedIn?: number
  retryAt?: number
  timer?: ReturnType<typeof setTimeout>
  pushes: Promise<unknown>
}

export class Courier {
  private readonly sessions = new Map<string, Session>()
  private readonly children = new Set<string>()
  // The first session.get of a session. close drops the entry, so a lookup
  // that returns after the session closed adds nothing.
  private readonly lookups = new Map<string, Promise<SessionInfo>>()
  private disposed = false

  constructor(private readonly host: Host) {}

  // started handles session.execution.started: it registers the session.
  started(sessionID: string): void {
    const s = this.sessions.get(sessionID)
    if (s) s.execution++
    this.connection(sessionID, false).catch((e) => this.host.log(`register ${sessionID}: ${message(e)}`))
  }

  // protocol is the protocol text for the session's context. It never starts
  // a connection: it waits a short time for a registration in flight, then
  // gives the text of the last registration. Undefined for a child session
  // or before the first registration.
  async protocol(sessionID: string): Promise<string | undefined> {
    const connecting = this.sessions.get(sessionID)?.connecting
    if (connecting) await within(connecting, this.host.contextWaitMs ?? CONTEXT_WAIT_MS).catch(() => {})
    return this.sessions.get(sessionID)?.protocol
  }

  // call runs a tool: it sends method to the daemon and returns the text of
  // the result. A daemon error throws the daemon's message.
  async call(sessionID: string, method: string, params?: Record<string, unknown>, signal?: AbortSignal): Promise<string> {
    const conn = await this.connection(sessionID, true)
    if (!conn) throw new Error(NOT_TOP_LEVEL)
    const result = await conn.call(method, params, signal)
    if (!isObject(result) || typeof result.text !== "string") throw new Error("the courier daemon sent an invalid result")
    return result.text
  }

  // close ends the session's connection and forgets the session, at
  // session.deleted and session.archived.
  close(sessionID: string): void {
    const s = this.sessions.get(sessionID)
    this.sessions.delete(sessionID)
    this.children.delete(sessionID)
    this.lookups.delete(sessionID)
    if (!s) return
    clearTimeout(s.timer)
    s.registering?.close()
    s.conn?.close()
  }

  // dispose closes every connection at plugin cleanup. After it, no entry
  // point starts a bridge.
  dispose(): void {
    this.disposed = true
    this.lookups.clear()
    for (const id of [...this.sessions.keys()]) this.close(id)
  }

  // connection is the live connection of a top-level session: it starts a
  // bridge and registers when none is live. Undefined for a child session,
  // and for a refused registration unless a tool asks. A tool gets
  // UNREACHABLE when the session closed or a retry is not due.
  private async connection(sessionID: string, fromTool: boolean): Promise<Connection | undefined> {
    const none = (): undefined => {
      if (fromTool) throw new Error(UNREACHABLE)
      return undefined
    }
    if (this.disposed) return none()
    if (this.children.has(sessionID)) return undefined
    const s = this.sessions.get(sessionID) ?? (await this.lookup(sessionID))
    if (s === "child") return undefined
    if (!s) return none()
    this.touch(sessionID, s)
    if (s.conn && !s.conn.closed) return s.conn
    if (s.connecting) return s.connecting
    if (s.refused !== undefined && !fromTool) return undefined
    if (s.retryAt !== undefined && (s.failedIn === s.execution || Date.now() < s.retryAt)) return none()
    const session = s
    session.connecting = this.register(sessionID, session).finally(() => {
      session.connecting = undefined
    })
    return session.connecting
  }

  // lookup finds out whether a session is top-level and adds it. Undefined
  // when the session closed or the plugin was disposed meanwhile: both drop
  // the lookup.
  private async lookup(sessionID: string): Promise<Session | "child" | undefined> {
    let p = this.lookups.get(sessionID)
    if (!p) {
      p = this.host.sessionInfo(sessionID)
      this.lookups.set(sessionID, p)
    }
    let info: SessionInfo
    try {
      info = await p
    } catch (e) {
      if (this.lookups.get(sessionID) === p) this.lookups.delete(sessionID)
      throw e
    }
    if (this.children.has(sessionID)) return "child"
    const known = this.sessions.get(sessionID)
    if (known) return known
    if (this.lookups.get(sessionID) !== p) return undefined
    this.lookups.delete(sessionID)
    if (info.parentID) {
      this.children.add(sessionID)
      return "child"
    }
    const s: Session = { directory: info.directory, pushes: Promise.resolve(), execution: 0, failures: 0 }
    this.sessions.set(sessionID, s)
    return s
  }

  private async register(sessionID: string, s: Session): Promise<Connection> {
    const conn: Connection = new Connection(
      this.host.spawn,
      (d) => this.deliver(sessionID, s, d),
      () => {
        if (s.conn === conn) {
          s.conn = undefined
          this.host.log(`session ${sessionID}: the connection to the courier daemon closed`)
        }
      },
    )
    s.registering = conn
    const name = s.name ?? this.host.name
    const topics = s.topics ?? this.host.topics
    try {
      const reg = await within(
        conn.call("register", {
          harness: "opencode",
          version: VERSION,
          cwd: s.directory,
          ...(name ? { name } : {}),
          ...(topics?.length ? { topics } : {}),
        }),
        REGISTER_TIMEOUT_MS,
      )
      if (!isObject(reg) || typeof reg.name !== "string" || typeof reg.protocol !== "string" || !isStrings(reg.topics)) {
        throw new Error("the courier daemon sent an invalid registration")
      }
      if (this.sessions.get(sessionID) !== s) throw new Error(UNREACHABLE)
      s.name = reg.name
      s.topics = reg.topics
      s.protocol = reg.protocol
      s.refused = undefined
      s.failures = 0
      s.failedIn = s.retryAt = undefined
      s.conn = conn
      conn.release()
      return conn
    } catch (e) {
      conn.close()
      const refused = e instanceof DaemonError && (e.code === -32004 || e.code === -32602)
      if (refused) s.refused = e.message
      else {
        s.failures++
        s.failedIn = s.execution
        s.retryAt = Date.now() + Math.min((this.host.retryMs ?? RETRY_MS) * 2 ** (s.failures - 1), RETRY_MAX_MS)
      }
      if (e instanceof DaemonError) throw e
      if (message(e) !== UNREACHABLE) this.host.log(`register ${sessionID}: ${message(e)}`)
      throw new Error(UNREACHABLE)
    } finally {
      if (s.registering === conn) s.registering = undefined
    }
  }

  // deliver pushes in the order the daemon sent, across connections.
  private deliver(sessionID: string, s: Session, d: Deliver): void {
    s.pushes = s.pushes
      .then(() => this.host.push(sessionID, d))
      .catch((e) => this.host.log(`push to ${sessionID}: ${message(e)}`))
  }

  private touch(sessionID: string, s: Session): void {
    clearTimeout(s.timer)
    s.timer = setTimeout(() => {
      if (s.conn) this.host.log(`session ${sessionID}: no execution for ${this.host.idleMs ?? IDLE_MS} ms, closing`)
      s.conn?.close()
    }, this.host.idleMs ?? IDLE_MS)
    s.timer.unref?.()
  }
}

function within<T>(p: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(UNREACHABLE)), ms)
  })
  return Promise.race([p, timeout]).finally(() => clearTimeout(timer))
}

function isStrings(v: unknown): v is string[] {
  return Array.isArray(v) && v.every((x) => typeof x === "string")
}

function message(e: unknown): string {
  return e instanceof Error ? e.message : String(e)
}
