const SAFE_METHODS = new Set(["GET", "HEAD", "OPTIONS"]);

export function createZeroTrustHttpPolicy({
  allowedOrigins = [],
  production = process.env.NODE_ENV === "production"
} = {}) {
  const allowlist = new Set(allowedOrigins.map(value => String(value || "").trim()).filter(Boolean));
  return function applyZeroTrustHttpPolicy(req, res) {
    const requestId = cryptoRandomId();
    res.setHeader("x-request-id", requestId);
    res.setHeader("x-content-type-options", "nosniff");
    res.setHeader("x-frame-options", "DENY");
    res.setHeader("referrer-policy", "no-referrer");
    res.setHeader("x-dns-prefetch-control", "off");
    res.setHeader("permissions-policy", "camera=(self), microphone=(self), geolocation=()");
    res.setHeader("cache-control", "no-store");
    if (production) res.setHeader("strict-transport-security", "max-age=15552000; includeSubDomains");

    const origin = String(req.headers?.origin || "").trim();
    if (!origin) return { allowed: true, requestId };
    let parsed;
    try { parsed = new URL(origin); } catch { return { allowed: false, requestId }; }
    if (!["https:", "http:"].includes(parsed.protocol) || parsed.username || parsed.password) {
      return { allowed: false, requestId };
    }
    const requestHost = String(req.headers?.host || "").toLowerCase();
    const sameHost = requestHost && parsed.host.toLowerCase() === requestHost;
    const allowed = sameHost || allowlist.has(origin);
    if (!allowed && production) return { allowed: false, requestId };
    if (allowed) {
      res.setHeader("access-control-allow-origin", origin);
      res.setHeader("access-control-allow-methods", "GET, HEAD, POST, OPTIONS");
      res.setHeader("access-control-allow-headers", "Authorization, Content-Type");
      res.setHeader("vary", "Origin");
    }
    if (production && String(req.headers?.["sec-fetch-site"] || "").toLowerCase() === "cross-site" && !allowed) {
      return { allowed: false, requestId };
    }
    return { allowed: true, requestId };
  };
}

function cryptoRandomId() {
  return globalThis.crypto?.randomUUID?.() || requireRandomId();
}
function requireRandomId() {
  // Imported lazily through Node's built-in crypto without exposing IDs as secrets.
  return randomUUIDFallback();
}
import { randomUUID as randomUUIDFallback } from "node:crypto";
