// agent-courier for OpenCode: the courier tools and the push of inbound
// messages into the live session. design.md "OpenCode client".
import { bridgeSpawner, type Deliver } from "./src/connection"
import { Courier } from "./src/courier"

// The parts of the OpenCode 2.1.4 plugin context (@opencode/plugin) that the
// plugin uses. Declared here, so the plugin has no runtime dependency.
interface Registration {
  dispose(): Promise<void>
}

interface ToolContext {
  readonly sessionID: string
  readonly signal: AbortSignal
}

interface ToolInfo {
  name: string
  description: string
  input: Record<string, unknown>
  options?: { namespace?: string }
  execute(input: Record<string, unknown>, context: ToolContext): Promise<{ content: string }>
}

interface ToolEditor {
  namespace(ns: { name: string; description: string }): void
  add(tool: ToolInfo): void
}

interface SystemPart {
  type: "text"
  text: string
}

export interface PluginContext {
  readonly session: {
    get(input: { sessionID: string }): Promise<{ parentID?: string; location: { directory: string } }>
    synthetic(input: {
      sessionID: string
      text: string
      description?: string
      metadata?: Record<string, string>
      delivery?: "steer" | "queue"
    }): Promise<unknown>
    hook(name: "context", callback: (event: { sessionID: string; system: SystemPart[] }) => Promise<void> | void): Promise<Registration>
  }
  readonly tool: {
    transform(callback: (editor: ToolEditor) => void): Promise<Registration>
  }
  readonly event: {
    subscribe(options?: { signal?: AbortSignal }): AsyncIterable<{ type: string; data?: unknown }>
  }
}

const NAMESPACE = "courier"

const KINDS = ["handoff", "done", "blocked", "ask", "reply", "note"]

const noInput = { type: "object", properties: {}, additionalProperties: false }

interface CourierTool {
  name: string
  description: string
  input: Record<string, unknown>
  // params picks the members the daemon takes from the tool input.
  params?(input: Record<string, unknown>): Record<string, unknown>
}

// The same names, arguments and descriptions as the Claude Code tools in
// internal/mcpserver/tools.go.
const tools: CourierTool[] = [
  {
    name: "send",
    description:
      "Send a message to the agents on a topic, or to one of them. Update the shared state first " +
      "(issue, pull request, document), then send a message that points at it. The sender is always your agent name.",
    input: {
      type: "object",
      properties: {
        topic: {
          type: "string",
          description: "A topic your agent is known on, as agents lists them, for example cappyt_agent-courier.",
        },
        kind: {
          type: "string",
          enum: KINDS,
          description:
            "handoff: work passes to someone. done: work someone waits on is finished. blocked: you wait on someone. " +
            "ask: a question for another agent. reply: the answer to an ask. note: information only.",
        },
        summary: {
          type: "string",
          description:
            "1 to 1000 characters: what happened or what you ask, and what the receiver does next. Name the issue, branch or pull request.",
        },
        to: {
          type: "string",
          description: "The agent name of the receiver, as agents lists it. Leave it out to reach every agent on the topic.",
        },
        issue: { type: "integer", description: "The issue or pull request number the message is about." },
        link: {
          type: "string",
          description: "An https link to the issue, comment, pull request or document, at most 512 characters.",
        },
        re: { type: "string", description: "For kind reply only: the id of the ask it answers." },
      },
      required: ["topic", "kind", "summary"],
      additionalProperties: false,
    },
    params: (input) => pick(input, ["topic", "kind", "summary", "to", "issue", "link", "re"]),
  },
  {
    name: "pending",
    description: "List the messages that wait for your ack, direct ones first, oldest first in each group. Call it when a task ends.",
    input: noInput,
  },
  {
    name: "ack",
    description: "Acknowledge a message you have handled. Until then it stays pending.",
    input: {
      type: "object",
      properties: { id: { type: "string", description: "The id of the message, from its header line." } },
      required: ["id"],
      additionalProperties: false,
    },
    params: (input) => pick(input, ["id"]),
  },
  {
    name: "agents",
    description: "List the agents known on your topics, your own agent included, and whether each one is live now.",
    input: noInput,
  },
]

function pick(input: Record<string, unknown>, keys: string[]): Record<string, unknown> {
  const out: Record<string, unknown> = {}
  for (const k of keys) if (input[k] !== undefined && input[k] !== null) out[k] = input[k]
  return out
}

function log(msg: string): void {
  console.error(`agent-courier: ${msg}`)
}

export default {
  id: "agent-courier",
  setup: async (ctx: PluginContext) => {
    const env = process.env
    const topics = (env.AGENT_COURIER_TOPICS ?? "")
      .split(",")
      .map((t) => t.trim())
      .filter((t) => t !== "")
    const courier = new Courier({
      spawn: bridgeSpawner(env.AGENT_COURIER_BIN || "agent-courier", log),
      sessionInfo: async (sessionID) => {
        const info = await ctx.session.get({ sessionID })
        return { parentID: info.parentID, directory: info.location.directory }
      },
      // The session text is the daemon's rendered text, the notice its
      // one-line notice, the metadata its validated attributes. Nothing else.
      push: (sessionID: string, d: Deliver) =>
        ctx.session.synthetic({ sessionID, text: d.text, description: d.notice, metadata: d.attrs, delivery: "steer" }),
      log,
      name: env.AGENT_COURIER_NAME?.trim() || undefined,
      topics,
    })

    const registrations: Registration[] = []
    registrations.push(await ctx.tool.transform((editor) => {
      editor.namespace({ name: NAMESPACE, description: "agent-courier: messages between coding agents" })
      for (const t of tools) {
        editor.add({
          name: t.name,
          description: t.description,
          input: t.input,
          options: { namespace: NAMESPACE },
          execute: async (input, context) => ({
            content: await courier.call(context.sessionID, t.name, t.params?.(input ?? {}), context.signal),
          }),
        })
      }
    }))

    // The context hook runs before each model call, so the protocol text is
    // in the context before the model sees the first push.
    registrations.push(await ctx.session.hook("context", async (event) => {
      const protocol = await courier.protocol(event.sessionID)
      if (protocol && !event.system.some((p) => p.text === protocol)) event.system.push({ type: "text", text: protocol })
    }))

    const abort = new AbortController()
    void (async () => {
      while (!abort.signal.aborted) {
        try {
          for await (const ev of ctx.event.subscribe({ signal: abort.signal })) {
            const data = ev.data as { sessionID?: unknown } | undefined
            const id = typeof data?.sessionID === "string" ? data.sessionID : undefined
            if (!id) continue
            if (ev.type === "session.execution.started") courier.started(id)
            else if (ev.type === "session.deleted" || ev.type === "session.archived") courier.close(id)
          }
        } catch (e) {
          if (!abort.signal.aborted) log(`event stream: ${e instanceof Error ? e.message : String(e)}`)
        }
        if (!abort.signal.aborted) await new Promise((r) => setTimeout(r, 1000))
      }
    })()

    return async () => {
      abort.abort()
      courier.dispose()
      for (const r of await Promise.allSettled(registrations.map(async (r) => r.dispose()))) {
        if (r.status === "rejected") log(`cleanup: ${r.reason instanceof Error ? r.reason.message : String(r.reason)}`)
      }
    }
  },
}
