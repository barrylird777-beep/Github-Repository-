import test from "node:test";
import assert from "node:assert/strict";
import os from "node:os";
import path from "node:path";
import fs from "node:fs/promises";
import { PaperOpportunityEngine, evaluateBackrun, estimatePaymentRoutes, LIVE_TRANSACTION_SUBMISSION_ENABLED } from "../src/paper/paper-engine.mjs";

async function tempDir() { return fs.mkdtemp(path.join(os.tmpdir(), "opportunity-paper-")); }

test("live transaction submission is a hard-disabled paper-trading constant", () => {
  assert.equal(LIVE_TRANSACTION_SUBMISSION_ENABLED, false);
});

test("backrun passes only when all costs are known and net profit clears the threshold", () => {
  const pass = evaluateBackrun({ notionalUsd: 1000, grossProfitUsd: 12, transactionFeeUsd: 2, protocolFeeUsd: 1, slippageBps: 10, marketImpactBps: 5, minNetProfitUsd: 1 });
  assert.equal(pass.result, "PASS");
  assert.equal(pass.netProfitUsd, 7);
  assert.equal(pass.executable, false);
  assert.equal(pass.liveTransactionsEnabled, false);

  const reject = evaluateBackrun({ notionalUsd: 1000, grossProfitUsd: 3, transactionFeeUsd: 2, protocolFeeUsd: 1, slippageBps: 10, marketImpactBps: 5 });
  assert.equal(reject.result, "REJECT");
  assert.equal(reject.netProfitUsd, -2);
  assert.equal(reject.executable, false);
});

test("backrun rejects missing cost inputs instead of assuming zero fees", () => {
  const result = evaluateBackrun({ notionalUsd: 100, grossProfitUsd: 5 });
  assert.equal(result.result, "REJECT");
  assert.ok(result.missing.includes("transactionFeeUsd"));
  assert.ok(result.missing.includes("slippageUsd"));
  assert.ok(result.missing.includes("marketImpactUsd"));
});

test("payment routing compares Stripe to configured alternatives without sending a payment", () => {
  const result = estimatePaymentRoutes({ amountUsd: 25 }, {
    STRIPE_FEE_BPS: "290",
    STRIPE_FIXED_FEE_USD: "0.30",
    USDC_NETWORK_FEE_USD: "0.08",
    BANK_TRANSFER_FEE_BPS: "0",
    BANK_TRANSFER_FIXED_FEE_USD: "0.10"
  });
  assert.equal(result.paperOnly, true);
  assert.equal(result.liveTransactionsEnabled, false);
  assert.equal(result.routes.find(x => x.route === "stripe_card").feeUsd, 1.025);
  assert.equal(result.routes.find(x => x.route === "usdc_direct").feeUsd, 0.08);
  assert.equal(result.recommendation, "usdc_direct");
});

test("liquidation feed logs passes and rejections with costs, health and paper-only guardrails", async () => {
  const dir = await tempDir();
  try {
    const rows = [
      { account: "unhealthy-good", healthFactor: 0.95, notionalUsd: 1000, estimatedLiquidationProfitUsd: 5, estimatedGasUsd: 0.5, slippageBps: 10, marketImpactBps: 5 },
      { account: "healthy", healthFactor: 1.2, notionalUsd: 1000, estimatedLiquidationProfitUsd: 5, estimatedGasUsd: 0.5, slippageBps: 10, marketImpactBps: 5 },
      { account: "missing-costs", healthFactor: 0.8, notionalUsd: 1000, estimatedLiquidationProfitUsd: 5 }
    ];
    const engine = new PaperOpportunityEngine({
      walDir: dir,
      env: { KAMINO_LIQUIDATION_FEED_URL: "https://feed.invalid/kamino", JUPITER_LEND_FEED_URL: "", PAPER_WAL_SEGMENT_BYTES: "64000" },
      fetchImpl: async () => ({ ok: true, json: async () => ({ positions: rows }) })
    });
    await engine.init();
    const result = await engine.scanLiquidations();
    assert.equal(result.scanned, 3);
    assert.equal(result.candidates, 1);
    assert.equal(result.rejected, 2);
    const records = await engine.recent(20);
    const scans = records.filter(x => x.eventType === "liquidation-scan");
    assert.equal(scans.length, 3);
    assert.ok(scans.every(x => x.liveTransactionsEnabled === false && x.executable === false));
    assert.ok(scans.some(x => x.account === "unhealthy-good" && x.result === "PASS" && x.estimatedNetProfitUsd === 3));
    assert.ok(scans.some(x => x.account === "healthy" && x.result === "REJECT"));
    assert.ok(records.some(x => x.eventType === "liquidation-feed-check" && x.protocol === "jupiter-lend" && x.result === "REJECT"));
    await engine.close();
  } finally { await fs.rm(dir, { recursive: true, force: true }); }
});

test("payment and backrun evaluations are written to the local JSONL Ring-WAL", async () => {
  const dir = await tempDir();
  try {
    const engine = new PaperOpportunityEngine({ walDir: dir, env: { USDC_NETWORK_FEE_USD: "0.05" } });
    await engine.init();
    await engine.evaluatePayment({ amountUsd: 25 });
    await engine.evaluateBackrun({ notionalUsd: 100, grossProfitUsd: 2, transactionFeeUsd: 0.1, slippageBps: 2, marketImpactBps: 2 });
    const records = await engine.recent(10);
    assert.ok(records.some(x => x.eventType === "payment-fee-routing-evaluation"));
    assert.ok(records.some(x => x.eventType === "trade-backrun-validation" && x.result === "PASS"));
    const files = await fs.readdir(dir);
    assert.ok(files.some(name => name.startsWith("paper-ring-wal.") && name.endsWith(".jsonl")));
    await engine.close();
  } finally { await fs.rm(dir, { recursive: true, force: true }); }
});
