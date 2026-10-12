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

  async request(url, { method = "GET", body, authenticate = false } = {}) {
    if (!this.baseUrl || typeof this.fetchImpl !== "function") {
      throw new Error("KashKorner Pure-WAL API URL is not configured");
    }
    if (authenticate && !this.token) {
      throw new Error("APEX_OPPORTUNITY_ENGINE_TOKEN is required for durable opportunity writes");
    }
    const headers = { accept: "application/json", "cache-control": "no-store" };
    if (body !== undefined) headers["content-type"] = "application/json";
    if (authenticate) headers["x-apex-opportunity-token"] = this.token;
    const response = await this.fetchImpl(this.baseUrl + url, {
      method,
      headers,
      ...(body !== undefined ? { body: JSON.stringify(body) } : {}),
      signal: AbortSignal.timeout(this.timeoutMs)
    });
    let result;
    try {
      result = await response.json();
    } catch {
      throw new Error("KashKorner Pure-WAL API returned invalid JSON (" + response.status + ")");
    }
    if (!response.ok || result?.success !== true) {
      const detail = String(result?.error || response.statusText || "request failed").slice(0, 300);
      throw new Error("KashKorner Pure-WAL API failed (" + response.status + "): " + detail);
    }
    return result;
  }

  async saveOpportunity(opportunity) {
    if (!this.configured) {
      throw new Error("KashKorner Pure-WAL API credentials are not configured");
    }
    return this.request("/api/kash-korner/opportunities", {
      method: "POST",
      body: { opportunity },
      authenticate: true
    });
  }

  async listOpportunities(limit = 100) {
    const bounded = Math.max(1, Math.min(500, Math.floor(Number(limit) || 100)));
    const result = await this.request("/api/kash-korner/opportunities?limit=" + bounded);
    return Array.isArray(result.items) ? result.items : [];
  }

  async health() {
    if (!this.configured) {
      return { configured: false, ok: false, error: "Opportunity API URL or token is missing" };
    }
    try {
      await this.listOpportunities(1);
      return { configured: true, ok: true };
    } catch (error) {
      return { configured: true, ok: false, error: String(error?.message || error).slice(0, 300) };
    }
  }
}
