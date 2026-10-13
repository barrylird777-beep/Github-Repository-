import test from "node:test";
import assert from "node:assert/strict";
import { unavailableOpportunityFeed, opportunityStorageName } from "../src/api/opportunity-store-policy.mjs";

test("opportunity feed fails closed when sovereign API storage is not configured", () => {
  const feed = unavailableOpportunityFeed({ configured: false, status: "not-configured", error: null });
  assert.deepEqual(feed.items, []);
  assert.equal(feed.storage, "unavailable");
  assert.equal(feed.source, "none");
  assert.match(feed.warning, /not configured/i);
  assert.equal(opportunityStorageName({ status: "not-configured" }), "unavailable");
});

test("opportunity feed never exposes memory fallback when sovereign storage degrades", () => {
  const feed = unavailableOpportunityFeed({
    configured: true, status: "degraded", error: "remote API timeout"
  });
  assert.deepEqual(feed.items, []);
  assert.equal(feed.storage, "unavailable");
  assert.equal(feed.source, "none");
  assert.equal(feed.warning, "remote API timeout");
  assert.equal(opportunityStorageName({ status: "degraded" }), "unavailable");
});

test("connected opportunity storage is explicitly identified as remote CAS", () => {
  assert.equal(opportunityStorageName({ status: "connected" }), "sovereign-remote-cas");
});
