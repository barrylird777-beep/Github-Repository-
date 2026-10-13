const RETRYABLE_STATUS = new Set([408, 425, 429, 500, 502, 503, 504]);

function retryAfterMs(response, now = Date.now()) {
  const value = response?.headers?.get?.("retry-after");
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.min(30_000, seconds * 1000);
  const date = Date.parse(value);
  return Number.isFinite(date) ? Math.max(0, Math.min(30_000, date - now)) : null;
}

const wait = ms => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Retries transient upstream failures with bounded exponential backoff.
 * Reuse only for operations that are safe to repeat (GET or idempotent writes).
 */
export async function fetchWithRetry(url, options = {}, config = {}) {
  const attempts = Math.max(1, Math.min(6, Number(config.attempts) || 4));
  const baseDelayMs = Math.max(0, Number(config.baseDelayMs ?? 250));
  const maxDelayMs = Math.max(baseDelayMs, Number(config.maxDelayMs ?? 4000));
  const fetchImpl = config.fetchImpl ?? fetch;
  const sleep = config.sleep ?? wait;
  let lastError;

  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      const response = await fetchImpl(url, options);
      if (!RETRYABLE_STATUS.has(response.status) || attempt === attempts - 1) return response;
      const exponential = Math.min(maxDelayMs, baseDelayMs * (2 ** attempt));
      const delay = retryAfterMs(response) ?? exponential;
      await sleep(delay);
    } catch (error) {
      lastError = error;
      if (error?.name === "AbortError" || attempt === attempts - 1) throw error;
      const delay = Math.min(maxDelayMs, baseDelayMs * (2 ** attempt));
      await sleep(delay);
    }
  }
  throw lastError ?? new Error("Upstream request failed after retries");
}
