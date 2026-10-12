export class SovereignApiStore {
  constructor({
    baseUrl = process.env.APEX_OPPORTUNITY_ENGINE_API_URL || "",
    token = process.env.APEX_OPPORTUNITY_ENGINE_TOKEN || "",
    timeoutMs = 15000,
    fetchImpl = globalThis.fetch
  } = {}) {
    this.baseUrl = String(baseUrl || "").replace(/\/+$/, "");
    this.token = String(token || "");
    this.timeoutMs = Math.max(1000, Number(timeoutMs) || 15000);
    this.fetchImpl = fetchImpl;
  }

  get configured() {
    return Boolean(this.baseUrl && this.token && typeof this.fetchImpl === "function");
  }

  async request(action, payload = {}) {
    if (!this.configured) {
      throw new Error("Sovereign Pure-WAL store is not configured; set APEX_OPPORTUNITY_ENGINE_API_URL and APEX_OPPORTUNITY_ENGINE_TOKEN");
    }
    const response = await this.fetchImpl(
      this.baseUrl + "/api/internal/opportunity-engine/store/" + action,
      {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "x-apex-opportunity-token": this.token
        },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(this.timeoutMs)
      }
    );
    let result;
    try {
      result = await response.json();
    } catch {
      throw new Error("Sovereign Pure-WAL store returned invalid JSON (" + response.status + ")");
    }
    if (!response.ok || result?.success !== true) {
      const detail = String(result?.error || response.statusText || "request failed").slice(0, 300);
      throw new Error("Sovereign Pure-WAL store " + action + " failed (" + response.status + "): " + detail);
    }
    return result;
  }

  async get(collection, id) {
    const result = await this.request("get", { collection, id });
    return result.record ?? null;
  }

  async query(collection, {
    filters = {},
    sortField = "",
    descending = false,
    limit = 100
  } = {}) {
    const result = await this.request("query", {
      collection,
      filters,
      sortField,
      descending,
      limit: Math.max(1, Math.min(500, Math.floor(Number(limit) || 100)))
    });
    return Array.isArray(result.records) ? result.records : [];
  }

  async put(collection, id, record) {
    const result = await this.request("put", { collection, id, record });
    return result.record;
  }

  async bulk(items) {
    if (!Array.isArray(items) || items.length < 1 || items.length > 100) {
      throw new TypeError("Sovereign Pure-WAL bulk writes require 1–100 records");
    }
    return this.request("bulk", { items });
  }

  async health() {
    if (!this.configured) {
      return { configured: false, ok: false, error: "Sovereign store token or API URL is missing" };
    }
    try {
      await this.get("koin_kob_engine_state", "scanner");
      return { configured: true, ok: true };
    } catch (error) {
      return { configured: true, ok: false, error: String(error?.message || error).slice(0, 300) };
    }
  }
}
