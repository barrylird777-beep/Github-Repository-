import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";

export const PAPER_TRADING_ONLY = true;
export const LIVE_TRANSACTION_SUBMISSION_ENABLED = false;

const finite = (value) => value !== null && value !== undefined && value !== "" && Number.isFinite(Number(value));
const nonNegative = (value, fallback = 0) => finite(value) && Number(value) >= 0 ? Number(value) : fallback;
const sha256 = (value) => crypto.createHash("sha256").update(value).digest("hex");
const asArray = (data) => Array.isArray(data) ? data : Array.isArray(data?.positions) ? data.positions : Array.isArray(data?.items) ? data.items : Array.isArray(data?.data) ? data.data : [];

function assessHealth(position) {
  const hf = position.healthFactor ?? position.health_factor ?? position.healthRatio ?? position.health_ratio;
  if (finite(hf)) return { known: true, healthFactor: Number(hf), unhealthy: Number(hf) <= 1 };
  const ltv = position.ltv ?? position.loanToValue ?? position.loan_to_value;
  const threshold = position.liquidationThreshold ?? position.liquidation_threshold ?? position.liquidationLtv ?? position.liquidation_ltv;
  if (finite(ltv) && finite(threshold) && Number(threshold) > 0) {
    return { known: true, healthFactor: Number(threshold) / Math.max(Number(ltv), 1e-9), unhealthy: Number(ltv) >= Number(threshold) };
  }
  return { known: false, healthFactor: null, unhealthy: false };
}

export class PaperRingWal {
  constructor({ dir = "./data", maxBytes = 1_000_000, segments = 4 } = {}) {
    this.dir = path.resolve(dir);
    this.maxBytes = Math.max(64_000, Number(maxBytes) || 1_000_000);
    this.segments = Math.max(2, Math.min(8, Math.floor(Number(segments) || 4)));
    this.slot = 0;
    this.sequence = 0;
    this.previousHash = "GENESIS";
    this.chain = Promise.resolve();
  }
  file(slot = this.slot) { return path.join(this.dir, `paper-ring-wal.${slot}.jsonl`); }
  async init() {
    await fs.mkdir(this.dir, { recursive: true, mode: 0o700 });
    const candidates = [];
    for (let slot = 0; slot < this.segments; slot++) {
      try {
        const text = await fs.readFile(this.file(slot), "utf8");
        const last = text.trim().split("\n").filter(Boolean).at(-1);
        if (last) {
          const row = JSON.parse(last);
          candidates.push({ slot, sequence: Number(row.sequence) || 0, hash: String(row.hash || "GENESIS") });
        }
      } catch (error) { if (error.code !== "ENOENT") throw error; }
    }
    candidates.sort((a, b) => b.sequence - a.sequence);
    if (candidates.length) {
      this.slot = candidates[0].slot;
      this.sequence = candidates[0].sequence;
      this.previousHash = candidates[0].hash;
    }
    return this;
  }
  async append(event) {
    const operation = async () => {
      const body = { sequence: this.sequence + 1, observedAt: new Date().toISOString(), previousHash: this.previousHash, ...event };
      const hash = sha256(JSON.stringify(body));
      const record = { ...body, hash };
      let size = 0;
      try { size = (await fs.stat(this.file())).size; } catch (error) { if (error.code !== "ENOENT") throw error; }
      const line = JSON.stringify(record) + "\n";
      if (size > 0 && size + Buffer.byteLength(line) > this.maxBytes) {
        this.slot = (this.slot + 1) % this.segments;
        await fs.writeFile(this.file(), "", { mode: 0o600 });
      }
      await fs.appendFile(this.file(), line, { encoding: "utf8", mode: 0o600 });
      this.sequence = record.sequence;
      this.previousHash = record.hash;
      return record;
    };
    const pending = this.chain.then(operation, operation);
    this.chain = pending.then(() => undefined, () => undefined);
    return pending;
  }
  async readRecent(limit = 100) {
    await this.chain;
    const records = [];
    for (let slot = 0; slot < this.segments; slot++) {
      try {
        const text = await fs.readFile(this.file(slot), "utf8");
        for (const line of text.split("\n")) {
          if (!line.trim()) continue;
          try { records.push(JSON.parse(line)); } catch { /* Ignore an incomplete trailing record only. */ }
        }
      } catch (error) { if (error.code !== "ENOENT") throw error; }
    }
    return records.sort((a, b) => Number(b.sequence) - Number(a.sequence)).slice(0, Math.max(1, Math.min(1000, Number(limit) || 100)));
  }
}

export function estimatePaymentRoutes(input = {}, env = process.env) {
  const amountUsd = Number(input.amountUsd ?? input.amount_usd ?? 25);
  if (!Number.isFinite(amountUsd) || amountUsd <= 0) throw new TypeError("amountUsd must be a positive number");
  const stripeBps = nonNegative(env.STRIPE_FEE_BPS, 290);
  const stripeFixed = nonNegative(env.STRIPE_FIXED_FEE_USD, 0.30);
  const usdcNetworkFee = finite(env.USDC_NETWORK_FEE_USD) ? nonNegative(env.USDC_NETWORK_FEE_USD) : null;
  const bankBps = finite(env.BANK_TRANSFER_FEE_BPS) ? nonNegative(env.BANK_TRANSFER_FEE_BPS) : null;
  const bankFixed = finite(env.BANK_TRANSFER_FIXED_FEE_USD) ? nonNegative(env.BANK_TRANSFER_FIXED_FEE_USD) : null;
  const routes = [
    { route: "stripe_card", feeUsd: amountUsd * stripeBps / 10000 + stripeFixed, availability: "estimate", note: "Configurable estimate; confirm your actual Stripe pricing." },
    { route: "usdc_direct", feeUsd: usdcNetworkFee, availability: usdcNetworkFee === null ? "fee_not_configured" : "estimate", note: "Network/wallet costs are not assumed to be zero; no transfer is submitted." },
    { route: "bank_transfer", feeUsd: bankBps === null || bankFixed === null ? null : amountUsd * bankBps / 10000 + bankFixed, availability: bankBps === null || bankFixed === null ? "fee_not_configured" : "estimate", note: "Optional bank-transfer estimate; configure actual provider fees." }
  ].map(route => ({ ...route, netUsd: route.feeUsd === null ? null : Number((amountUsd - route.feeUsd).toFixed(6)) }));
  const stripe = routes.find(route => route.route === "stripe_card");
  for (const route of routes) route.savingsVsStripeUsd = route.feeUsd === null ? null : Number((stripe.feeUsd - route.feeUsd).toFixed(6));
  return { kind: "payment-fee-comparison", amountUsd, currency: "USD", paperOnly: true, liveTransactionsEnabled: false, routes, recommendation: routes.filter(route => route.feeUsd !== null).sort((a, b) => a.feeUsd - b.feeUsd)[0]?.route || "no-comparable-route" };
}

export function evaluateBackrun(input = {}) {
  const notionalUsd = Number(input.notionalUsd ?? input.notional_usd);
  const grossProfitUsd = finite(input.grossProfitUsd ?? input.gross_profit_usd)
    ? Number(input.grossProfitUsd ?? input.gross_profit_usd)
    : finite(input.grossEdgeBps ?? input.gross_edge_bps) && finite(notionalUsd)
      ? notionalUsd * Number(input.grossEdgeBps ?? input.gross_edge_bps) / 10000
      : NaN;
  const costs = {
    transactionFeeUsd: Number(input.transactionFeeUsd ?? input.estimatedGasUsd ?? input.estimated_gas_usd),
    protocolFeeUsd: nonNegative(input.protocolFeeUsd ?? input.protocol_fee_usd),
    slippageUsd: finite(input.slippageUsd ?? input.estimatedSlippageUsd)
      ? nonNegative(input.slippageUsd ?? input.estimatedSlippageUsd)
      : finite(input.slippageBps) && finite(notionalUsd) ? notionalUsd * nonNegative(input.slippageBps) / 10000 : NaN,
    marketImpactUsd: finite(input.marketImpactUsd)
      ? nonNegative(input.marketImpactUsd)
      : finite(input.marketImpactBps) && finite(notionalUsd) ? notionalUsd * nonNegative(input.marketImpactBps) / 10000 : NaN
  };
  const missing = [];
  if (!finite(notionalUsd) || notionalUsd <= 0) missing.push("notionalUsd");
  if (!finite(grossProfitUsd) || grossProfitUsd < 0) missing.push("grossProfitUsd");
  for (const [key, value] of Object.entries(costs)) if (!Number.isFinite(value) || value < 0) missing.push(key);
  const totalCostsUsd = Object.values(costs).every(Number.isFinite) ? Object.values(costs).reduce((sum, value) => sum + value, 0) : null;
  const netProfitUsd = totalCostsUsd === null || !Number.isFinite(grossProfitUsd) ? null : Number((grossProfitUsd - totalCostsUsd).toFixed(8));
  const minNetProfitUsd = nonNegative(input.minNetProfitUsd ?? input.min_net_profit_usd);
  const accepted = missing.length === 0 && netProfitUsd > minNetProfitUsd;
  return {
    kind: "trade-backrun-validation", protocol: String(input.protocol || "generic"), market: String(input.market || "unspecified"),
    notionalUsd: finite(notionalUsd) ? notionalUsd : null, grossProfitUsd: finite(grossProfitUsd) ? grossProfitUsd : null,
    costs, totalCostsUsd, netProfitUsd, minNetProfitUsd, result: accepted ? "PASS" : "REJECT",
    reason: missing.length ? "missing-or-invalid-cost-inputs" : accepted ? "positive-net-after-costs" : "net-profit-does-not-clear-threshold",
    missing, paperOnly: true, liveTransactionsEnabled: false, executable: false
  };
}

export class PaperOpportunityEngine {
  constructor({ walDir = process.env.WAL_DIR || "./data", fetchImpl = globalThis.fetch, env = process.env } = {}) {
    this.env = env;
    this.fetchImpl = fetchImpl;
    this.wal = new PaperRingWal({ dir: walDir, maxBytes: nonNegative(env.PAPER_WAL_SEGMENT_BYTES, 1_000_000), segments: nonNegative(env.PAPER_WAL_SEGMENTS, 4) });
    this.lastScan = { status: "not-run", feedsConfigured: false, scanned: 0, candidates: 0, rejected: 0, at: null, errors: [] };
    this.started = false;
  }
  get feedConfigs() {
    return [
      { protocol: "kamino", url: String(this.env.KAMINO_LIQUIDATION_FEED_URL || "").trim() },
      { protocol: "jupiter-lend", url: String(this.env.JUPITER_LEND_FEED_URL || "").trim() }
    ];
  }
  async init() { await this.wal.init(); this.started = true; return this; }
  status() {
    const feeds = this.feedConfigs;
    return { enabled: true, mode: "paper-trading", liveTransactionSubmissionEnabled: false, liveTransactionsEnabled: false,
      wal: { type: "local-jsonl-ring-wal", directory: this.wal.dir, segmentCount: this.wal.segments, maxBytesPerSegment: this.wal.maxBytes, latestSequence: this.wal.sequence },
      liquidationFeeds: feeds.map(feed => ({ protocol: feed.protocol, configured: Boolean(feed.url) })),
      lastLiquidationScan: this.lastScan };
  }
  async record(event) { return this.wal.append({ paperOnly: true, liveTransactionsEnabled: false, ...event }); }
  async evaluateBackrun(input) {
    const result = evaluateBackrun(input);
    await this.record({ eventType: "trade-backrun-validation", ...result });
    return result;
  }
  async evaluatePayment(input) {
    const result = estimatePaymentRoutes(input, this.env);
    await this.record({ eventType: "payment-fee-routing-evaluation", ...result });
    return result;
  }
  async scanLiquidations() {
    const feeds = this.feedConfigs;
    const errors = [];
    let scanned = 0, candidates = 0, rejected = 0;
    for (const feed of feeds) {
      if (!feed.url) {
        await this.record({ eventType: "liquidation-feed-check", protocol: feed.protocol, result: "REJECT", reason: "feed-url-not-configured", liveTransactionsEnabled: false });
        errors.push({ protocol: feed.protocol, error: "feed URL not configured" });
        continue;
      }
      try {
        const response = await this.fetchImpl(feed.url, { headers: { accept: "application/json" }, cache: "no-store", signal: AbortSignal.timeout(10000) });
        if (!response.ok) throw new Error("feed HTTP " + response.status);
        const payload = await response.json();
        const positions = asArray(payload);
        if (!positions.length) {
          await this.record({ eventType: "liquidation-feed-check", protocol: feed.protocol, result: "REJECT", reason: "feed-contained-no-position-rows", liveTransactionsEnabled: false });
        }
        for (const position of positions) {
          scanned++;
          const health = assessHealth(position);
          const account = String(position.account || position.owner || position.wallet || position.positionAddress || position.position_id || "unknown").slice(0, 180);
          const notionalUsd = finite(position.notionalUsd ?? position.notional_usd ?? position.debtValueUsd) ? Number(position.notionalUsd ?? position.notional_usd ?? position.debtValueUsd) : null;
          const gross = finite(position.estimatedLiquidationProfitUsd ?? position.estimatedBonusUsd ?? position.expectedBonusUsd)
            ? Number(position.estimatedLiquidationProfitUsd ?? position.estimatedBonusUsd ?? position.expectedBonusUsd) : null;
          const gasFeeUsd = finite(position.estimatedGasUsd ?? this.env.LIQUIDATION_GAS_USD) ? nonNegative(position.estimatedGasUsd ?? this.env.LIQUIDATION_GAS_USD) : null;
          const slippageBps = finite(position.slippageBps ?? this.env.LIQUIDATION_SLIPPAGE_BPS) ? nonNegative(position.slippageBps ?? this.env.LIQUIDATION_SLIPPAGE_BPS) : null;
          const impactBps = finite(position.marketImpactBps ?? this.env.LIQUIDATION_MARKET_IMPACT_BPS) ? nonNegative(position.marketImpactBps ?? this.env.LIQUIDATION_MARKET_IMPACT_BPS) : null;
          const missing = [];
          if (!health.known) missing.push("health-factor-or-ltv-threshold");
          if (health.known && !health.unhealthy) missing.push("position-not-unhealthy");
          if (gross === null) missing.push("estimated-liquidation-profit");
          if (gasFeeUsd === null) missing.push("estimated-gas-fee");
          if (slippageBps === null || notionalUsd === null) missing.push("notional-and-slippage-estimate");
          if (impactBps === null || notionalUsd === null) missing.push("market-impact-estimate");
          const costs = gasFeeUsd !== null && notionalUsd !== null && slippageBps !== null && impactBps !== null
            ? gasFeeUsd + notionalUsd * (slippageBps + impactBps) / 10000 : null;
          const net = costs !== null && gross !== null ? Number((gross - costs).toFixed(8)) : null;
          const accepted = missing.length === 0 && net > nonNegative(this.env.MIN_LIQUIDATION_NET_USD, 0);
          const result = { eventType: "liquidation-scan", protocol: feed.protocol, account, healthFactor: health.healthFactor,
            notionalUsd, estimatedGrossProfitUsd: gross, estimatedGasUsd: gasFeeUsd, slippageBps, marketImpactBps,
            estimatedTotalCostsUsd: costs, estimatedNetProfitUsd: net, result: accepted ? "PASS" : "REJECT",
            reason: accepted ? "unhealthy-position-net-positive-after-costs" : missing.length ? "insufficient-or-unhealthy-data" : "net-profit-does-not-clear-threshold",
            missing, executable: false, paperOnly: true, liveTransactionsEnabled: false };
          await this.record(result);
          if (accepted) candidates++; else rejected++;
        }
      } catch (error) {
        const message = String(error?.message || error).slice(0, 300);
        errors.push({ protocol: feed.protocol, error: message });
        await this.record({ eventType: "liquidation-feed-check", protocol: feed.protocol, result: "REJECT", reason: "feed-fetch-or-parse-failed", error: message, liveTransactionsEnabled: false });
      }
    }
    this.lastScan = { status: errors.length === feeds.length ? "degraded" : "ok", feedsConfigured: feeds.some(feed => Boolean(feed.url)), scanned, candidates, rejected, at: new Date().toISOString(), errors };
    return this.lastScan;
  }
  async recent(limit = 100) { return this.wal.readRecent(limit); }
  async close() { await this.wal.chain; this.started = false; }
}
