const MAX_INPUT = 50000;

const templates = {
  edgeCases: input => [
    "Identify the boundary conditions for the task.",
    "Test empty, null, missing, malformed, duplicated, and oversized inputs.",
    "Test unexpected types and invalid state transitions.",
    "Test dependency timeout, partial failure, retry, and recovery behavior.",
    "Test concurrency, ordering, idempotency, and duplicate delivery where applicable.",
    "Identify security-sensitive inputs and unsafe assumptions.",
    "",
    "TASK:",
    input
  ].join("\n"),

  codeReview: input => [
    "Review the following implementation for correctness, edge cases, security, error handling, performance, maintainability, and testability.",
    "Return concrete findings with severity and a practical fix for each.",
    "Do not claim a test was run unless evidence is provided.",
    "",
    "CODE:",
    input
  ].join("\n"),

  testPlan: input => [
    "Create a focused technical test plan.",
    "Include happy paths, boundaries, invalid inputs, concurrency, retries/failures, security, and regression coverage.",
    "For each test, state the setup, action, and expected result.",
    "",
    "IMPLEMENTATION:",
    input
  ].join("\n"),

  reasoning: input => [
    "Analyze this technical problem systematically.",
    "Return: assumptions, requirements, constraints, edge cases, failure modes, proposed solution, and verification strategy.",
    "Separate facts from assumptions and do not invent external evidence.",
    "",
    "PROBLEM:",
    input
  ].join("\n"),

  formatter: input => [
    "Format the following technical answer.",
    "Answer directly. Remove repetition. Preserve important caveats.",
    "Use concise technical language. Do not invent tests, sources, or results.",
    "",
    "DRAFT:",
    input
  ].join("\n")
};

function normalizeType(type) {
  const value = String(type || "").trim();
  if (!Object.hasOwn(templates, value)) throw new Error("unknown evaluation template");
  return value;
}

function clampInput(input) {
  const value = String(input || "").trim();
  if (!value) throw new Error("input is required");
  if (value.length > MAX_INPUT) throw new Error("input exceeds 50,000 characters");
  return value;
}

async function callModel(prompt, {
  apiKey = process.env.AI_API_KEY || process.env.OPENAI_API_KEY || "",
  baseUrl = process.env.AI_BASE_URL || process.env.OPENAI_BASE_URL || "https://api.openai.com/v1",
  model = process.env.AI_MODEL || process.env.OPENAI_MODEL || "gpt-4.1-mini"
} = {}) {
  if (!apiKey) return null;

  const root = String(baseUrl).replace(/\/$/, "");
  const response = await fetch(root + "/chat/completions", {
    method: "POST",
    headers: {
      "content-type": "application/json",
      authorization: "Bearer " + apiKey
    },
    body: JSON.stringify({
      model,
      temperature: 0.2,
      messages: [
        {
          role: "system",
          content: "You are a technical evaluation drafting assistant. Produce accurate drafts for human review. Never claim that work, tests, tool calls, or external submissions occurred unless explicitly provided as evidence."
        },
        {role: "user", content: prompt}
      ]
    }),
    signal: AbortSignal.timeout(45000)
  });

  if (!response.ok) {
    const detail = await response.text().catch(() => "");
    throw new Error("AI provider HTTP " + response.status + (detail ? ": " + detail.slice(0, 300) : ""));
  }

  const data = await response.json();
  const content = data?.choices?.[0]?.message?.content;
  if (typeof content !== "string" || !content.trim()) throw new Error("AI provider returned no text");
  return content.trim();
}

export class AiEvalHelper {
  async generate({type, input, useModel=true} = {}) {
    const template = normalizeType(type);
    const clean = clampInput(input);
    const prompt = templates[template](clean);

    if (useModel) {
      const generated = await callModel(prompt);
      if (generated) {
        return {
          mode: "ai",
          type: template,
          prompt,
          result: generated
        };
      }
    }

    return {
      mode: "template",
      type: template,
      prompt,
      result: prompt
    };
  }

  templates() {
    return Object.keys(templates);
  }
}
