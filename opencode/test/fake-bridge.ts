#!/usr/bin/env bun
// A fake `agent-courier bridge` for the plugin test: it answers register and
// pending, pushes one deliver after register, and exits on a send.
import { createInterface } from "node:readline"

if (process.argv[2] !== "bridge") {
  console.error(`fake bridge: unexpected arguments ${JSON.stringify(process.argv.slice(2))}`)
  process.exit(2)
}
console.error("fake bridge started")
const out = (msg: unknown) => process.stdout.write(JSON.stringify(msg) + "\n")
createInterface({ input: process.stdin }).on("line", (line) => {
  const req = JSON.parse(line)
  switch (req.method) {
    case "register":
      out({ jsonrpc: "2.0", id: req.id, result: { session: "S", name: "opencode-fake", topics: ["o_fake"], protocol: "FAKE PROTOCOL", version: req.params.version } })
      out({
        jsonrpc: "2.0",
        method: "deliver",
        params: {
          text: "[courier] note from claude-fake",
          notice: "courier: note from claude-fake",
          attrs: { id: "01ID", topic: "o_fake", sender: "claude-fake", kind: "note", to: "opencode-fake" },
          summary: "raw summary",
        },
      })
      break
    case "pending":
      out({ jsonrpc: "2.0", id: req.id, result: { text: `fake pending for ${req.params === undefined ? "no params" : "params"}`, total: 0 } })
      break
    default:
      console.error("fake bridge exits")
      process.exit(3)
  }
})
