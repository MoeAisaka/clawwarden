import crypto from "node:crypto";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

const HERE = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_SCRIPT = path.resolve(HERE, "../../../scripts/control_plane.py");

function clip(value, limit) {
  const text = String(value ?? "").replace(/\s+/g, " ").trim();
  return text.slice(0, Math.max(0, Number(limit) || 0));
}

function hashText(value) {
  return crypto.createHash("sha256").update(String(value ?? "")).digest("hex").slice(0, 20);
}

export function lifecycleEventId(kind, ctx = {}, event = {}) {
  const runId = event.runId ?? ctx.runId;
  if (runId) {
    if (kind === "tool_error") {
      return `tool-error:${runId}:${event.toolCallId ?? ctx.toolCallId ?? hashText(event.toolName)}`;
    }
    return `${kind}:${runId}`;
  }
  const seed = JSON.stringify({
    kind,
    sessionKey: ctx.sessionKey,
    sessionId: ctx.sessionId,
    prompt: event.prompt,
    at: Math.floor(Date.now() / 1000),
  });
  return `${kind}:${hashText(seed)}`;
}

export function shouldSkipPrompt(prompt, ctx = {}) {
  const text = String(prompt ?? "");
  if (/^\s*\[CLAWWARDEN_ALERT\]/i.test(text)) return true;
  if (/Clawwarden 自动巡检|scripts\/clawwarden\.py patrol/i.test(text)) return true;
  if (/^\s*\[OpenClaw heartbeat poll\]\s*$/i.test(text)) return true;
  if (String(ctx.trigger ?? "").toLowerCase() === "heartbeat") return true;
  return false;
}

export function recoveryMetadata(prompt) {
  const text = String(prompt ?? "");
  const currentRequestIndex = text.lastIndexOf("Current user request:");
  const currentRequest = currentRequestIndex >= 0 ? text.slice(currentRequestIndex) : text;
  const matches = [
    ...currentRequest.matchAll(
      /\[CLAWWARDEN_RECOVERY\s+run=([^\s\]]+)\s+task=([^\s\]]+)\s+event=([^\s\]]+)\]/gi,
    ),
  ];
  const match = matches.at(-1);
  if (!match) return null;
  return { recoveryOfRunId: match[1], recoveryTaskId: match[2], recoveryEventId: match[3] };
}

export function isGatewayContinuation(prompt) {
  const text = String(prompt ?? "");
  const currentRequestIndex = text.lastIndexOf("Current user request:");
  const currentRequest = currentRequestIndex >= 0 ? text.slice(currentRequestIndex) : text;
  return /previous turn was interrupted by a gateway restart/i.test(currentRequest);
}

function textFromContent(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content
    .map((item) => {
      if (typeof item === "string") return item;
      if (!item || typeof item !== "object") return "";
      return item.text ?? item.content ?? "";
    })
    .filter(Boolean)
    .join("\n");
}

export function lastAssistantSummary(messages, limit = 6000) {
  if (!Array.isArray(messages)) return "";
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index];
    if (!message || typeof message !== "object") continue;
    const role = String(message.role ?? message.type ?? "").toLowerCase();
    if (!role.includes("assistant")) continue;
    const text = textFromContent(message.content ?? message.text ?? message.message);
    if (text) return clip(text, limit);
  }
  return "";
}

export function buildTurnStartPayload(config, event, ctx, recovery = null) {
  const rawPrompt = String(event.prompt ?? "");
  return {
    runId: event.runId ?? ctx.runId,
    sessionKey: ctx.sessionKey,
    sessionId: ctx.sessionId,
    agentId: ctx.agentId,
    trigger: ctx.trigger,
    channelId: ctx.channelId,
    prompt: config.capturePrompt === true ? clip(rawPrompt, config.maxPromptChars) : "",
    promptHash: hashText(rawPrompt),
    gatewayContinuation: isGatewayContinuation(rawPrompt),
    ...(recovery ?? {}),
  };
}

export function buildTurnFinishPayload(config, event, ctx) {
  const rawSummary = lastAssistantSummary(event.messages, config.maxSummaryChars);
  return {
    runId: event.runId ?? ctx.runId,
    sessionKey: ctx.sessionKey,
    sessionId: ctx.sessionId,
    success: event.success !== false,
    error: clip(event.error, 2000),
    durationMs: event.durationMs,
    summary: config.captureSummary === true ? rawSummary : "",
    summaryHash: hashText(rawSummary),
  };
}

export function buildEnqueueArgs(config, eventId, eventType, payload) {
  return [
    config.scriptPath || DEFAULT_SCRIPT,
    "enqueue",
    "--event-id",
    eventId,
    "--event-type",
    eventType,
    "--payload",
    JSON.stringify(payload),
  ];
}

function enqueue(config, logger, eventId, eventType, payload) {
  const python = config.pythonPath || process.env.CLAWWARDEN_PYTHON || "python3";
  const args = buildEnqueueArgs(config, eventId, eventType, payload);
  const result = spawnSync(python, args, {
    encoding: "utf8",
    timeout: Math.max(250, Math.min(5000, Number(config.enqueueTimeoutMs ?? 2000))),
    env: {
      ...process.env,
      PATH: process.env.PATH || "/usr/local/bin:/usr/bin:/bin",
    },
    maxBuffer: 1024 * 1024,
  });
  if (result.error || result.status !== 0) {
    logger.warn?.(
      `clawwarden-runtime: enqueue failed type=${eventType} id=${eventId}: ${result.error ?? result.stderr ?? result.status}`,
    );
    return false;
  }
  return true;
}

export default {
  id: "clawwarden-runtime",
  name: "Clawwarden Runtime",
  description: "Durable lifecycle bridge for unattended Clawwarden processing",

  register(api) {
    const config = {
      enabled: true,
      pythonPath: process.env.CLAWWARDEN_PYTHON || "python3",
      scriptPath: DEFAULT_SCRIPT,
      enqueueTimeoutMs: 2000,
      capturePrompt: false,
      captureSummary: false,
      maxPromptChars: 8000,
      maxSummaryChars: 6000,
      ...(api.pluginConfig ?? {}),
    };
    if (config.enabled === false) {
      api.logger.info?.("clawwarden-runtime: disabled by config");
      return;
    }

    api.on("before_model_resolve", (event, ctx) => {
      const recovery = recoveryMetadata(event.prompt);
      if (!recovery && shouldSkipPrompt(event.prompt, ctx)) return;
      const eventId = lifecycleEventId("turn_start", ctx, event);
      enqueue(config, api.logger, eventId, "turn_start", buildTurnStartPayload(config, event, ctx, recovery));
    });

    api.on("agent_end", (event, ctx) => {
      const runId = event.runId ?? ctx.runId;
      if (!runId) return;
      const eventId = lifecycleEventId("turn_finish", ctx, event);
      enqueue(config, api.logger, eventId, "turn_finish", buildTurnFinishPayload(config, event, ctx));
    });

    api.on("after_tool_call", (event, ctx) => {
      if (!event.error) return;
      const eventId = lifecycleEventId("tool_error", ctx, event);
      enqueue(config, api.logger, eventId, "tool_error", {
        runId: event.runId ?? ctx.runId,
        sessionKey: ctx.sessionKey,
        toolName: event.toolName,
        toolCallId: event.toolCallId ?? ctx.toolCallId,
        error: clip(event.error, 2000),
        durationMs: event.durationMs,
      });
    });

    api.on("gateway_start", (event) => {
      const eventId = `gateway_start:${Date.now()}:${process.pid}`;
      enqueue(config, api.logger, eventId, "gateway_start", {
        pid: process.pid,
        event: event ?? {},
      });
    });

    api.on("gateway_stop", (event) => {
      const eventId = `gateway_stop:${Date.now()}:${process.pid}`;
      enqueue(config, api.logger, eventId, "gateway_stop", {
        pid: process.pid,
        event: event ?? {},
      });
    });

    api.logger.info?.(`clawwarden-runtime: lifecycle bridge active (${config.scriptPath})`);
  },
};
