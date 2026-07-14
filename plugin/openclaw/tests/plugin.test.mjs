import test from "node:test";
import assert from "node:assert/strict";
import {
  buildTurnFinishPayload,
  buildTurnStartPayload,
  buildEnqueueArgs,
  lastAssistantSummary,
  lifecycleEventId,
  isGatewayContinuation,
  recoveryMetadata,
  shouldSkipPrompt,
} from "../src/index.js";

test("stable lifecycle ids use run id", () => {
  assert.equal(lifecycleEventId("turn_start", { runId: "run-1" }), "turn_start:run-1");
  assert.equal(
    lifecycleEventId("tool_error", { runId: "run-1" }, { toolCallId: "tool-2" }),
    "tool-error:run-1:tool-2",
  );
});

test("recursive Clawwarden alerts and patrol prompts are skipped", () => {
  assert.equal(shouldSkipPrompt("[CLAWWARDEN_ALERT] warning"), true);
  assert.equal(shouldSkipPrompt("run scripts/clawwarden.py patrol"), true);
  assert.equal(shouldSkipPrompt("用户真实任务"), false);
  assert.equal(shouldSkipPrompt("anything", { trigger: "heartbeat" }), true);
});

test("assistant summary is bounded and ignores user tail", () => {
  const messages = [
    { role: "assistant", content: [{ type: "text", text: "done" }] },
    { role: "user", content: "later" },
  ];
  assert.equal(lastAssistantSummary(messages, 20), "done");
});

test("enqueue args preserve payload as JSON", () => {
  const args = buildEnqueueArgs({ scriptPath: "/tmp/clawwarden.py" }, "evt", "turn_start", { a: 1 });
  assert.equal(args[0], "/tmp/clawwarden.py");
  assert.equal(args[3], "evt");
  assert.equal(args[6], "--payload");
  assert.equal(JSON.parse(args[7]).a, 1);
});

test("raw prompts and summaries are not persisted by default", () => {
  const config = { capturePrompt: false, captureSummary: false, maxPromptChars: 8000, maxSummaryChars: 6000 };
  const start = buildTurnStartPayload(config, { prompt: "private prompt", runId: "run-1" }, { sessionKey: "agent:main:main" });
  const finish = buildTurnFinishPayload(
    config,
    { runId: "run-1", messages: [{ role: "assistant", content: "private summary" }] },
    { sessionKey: "agent:main:main" },
  );
  assert.equal(start.prompt, "");
  assert.match(start.promptHash, /^[a-f0-9]{20}$/);
  assert.equal(finish.summary, "");
  assert.match(finish.summaryHash, /^[a-f0-9]{20}$/);
});

test("content capture requires explicit opt-in", () => {
  const config = { capturePrompt: true, captureSummary: true, maxPromptChars: 7, maxSummaryChars: 7 };
  const start = buildTurnStartPayload(config, { prompt: "private prompt" }, {});
  const finish = buildTurnFinishPayload(config, { messages: [{ role: "assistant", content: "private summary" }] }, {});
  assert.equal(start.prompt, "private");
  assert.equal(finish.summary, "private");
});

test("recovery marker preserves task lineage", () => {
  assert.deepEqual(
    recoveryMetadata("[CLAWWARDEN_RECOVERY run=run-1 task=task-1 event=resume:run-1:1] continue"),
    {
      recoveryOfRunId: "run-1",
      recoveryTaskId: "task-1",
      recoveryEventId: "resume:run-1:1",
    },
  );
  assert.equal(recoveryMetadata("normal task"), null);
});

test("heartbeat filtering remains distinguishable from recovery metadata", () => {
  const prompt = "[CLAWWARDEN_RECOVERY run=run-1 task=task-1 event=resume:run-1:1] continue";
  assert.equal(shouldSkipPrompt(prompt, { trigger: "heartbeat" }), true);
  assert.equal(recoveryMetadata(prompt)?.recoveryOfRunId, "run-1");
});

test("recovery marker is extracted from compiled current request, not history", () => {
  const prompt = `conversation_context: [CLAWWARDEN_RECOVERY run=old task=old-task event=old-event]\nCurrent user request:\nSystem: [time] [CLAWWARDEN_RECOVERY run=new task=new-task event=new-event] continue`;
  assert.deepEqual(recoveryMetadata(prompt), {
    recoveryOfRunId: "new",
    recoveryTaskId: "new-task",
    recoveryEventId: "new-event",
  });
  assert.equal(
    recoveryMetadata("conversation_context: [CLAWWARDEN_RECOVERY run=old task=old-task event=old-event]\nCurrent user request:\nnormal"),
    null,
  );
});

test("gateway continuation is detected only in the current request", () => {
  assert.equal(
    isGatewayContinuation("history: previous turn was interrupted by a gateway restart\nCurrent user request:\nnormal"),
    false,
  );
  assert.equal(
    isGatewayContinuation("Current user request:\n[System] Your previous turn was interrupted by a gateway restart while waiting"),
    true,
  );
});
