import { timingSafeEqual } from "node:crypto";

export function authorizeScanRequest(
  req,
  token = process.env.APEX_ZERO_TRUST_TOKEN ||
    process.env.APEX_OPPORTUNITY_ENGINE_TOKEN ||
    process.env.APEX_OPPORTUNITY_ENGINE_SYNC_TOKEN ||
    process.env.OPPORTUNITY_SCAN_TOKEN
) {
  const expected = String(token || "").trim();
  const header = String(req?.headers?.authorization || "");
  const match = /^Bearer\s+(.+)$/i.exec(header);
  const provided = String(match?.[1] || "").trim();
  if (!expected || !provided) return false;
  const expectedBytes = Buffer.from(expected);
  const providedBytes = Buffer.from(provided);
  return expectedBytes.length === providedBytes.length && timingSafeEqual(expectedBytes, providedBytes);
}
