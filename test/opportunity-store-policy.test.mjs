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


import { isBlockedAddress, resolveSafeUrl } from "../src/extract/instant-extract.mjs";

test("extractor blocks private, loopback, link-local, metadata and reserved IPv4", () => {
  for (const address of ["127.0.0.1","10.0.0.1","172.16.0.1","192.168.1.1","169.254.169.254","100.64.0.1","198.18.0.1","224.0.0.1"]) {
    assert.equal(isBlockedAddress(address), true, address);
  }
});

test("extractor blocks local and mapped-private IPv6 destinations", () => {
  for (const address of ["::1","fc00::1","fd12::1","fe80::1","::ffff:127.0.0.1","::ffff:192.168.1.2","2001:db8::1","2002:7f00:1::1"]) {
    assert.equal(isBlockedAddress(address), true, address);
  }
  assert.equal(isBlockedAddress("2606:4700:4700::1111"), false);
});

test("extractor rejects unsafe URL schemes and embedded credentials", async () => {
  await assert.rejects(resolveSafeUrl("file:///etc/passwd"), /invalid URL/);
  await assert.rejects(resolveSafeUrl("https://user:pass@example.com"), /invalid URL/);
});

test("extractor rejects mixed public and private DNS answers", async () => {
  const lookup = async () => [{ address: "8.8.8.8", family: 4 }, { address: "127.0.0.1", family: 4 }];
  await assert.rejects(resolveSafeUrl("https://example.com", { lookup }), /private or non-public destination blocked/);
});

test("extractor fails closed on DNS errors and retains vetted lookup results", async () => {
  const lookup = async () => [{ address: "8.8.8.8", family: 4 }];
  const resolved = await resolveSafeUrl("https://example.com/path", { lookup });
  assert.equal(resolved.url.hostname, "example.com");
  assert.deepEqual(resolved.addresses, [{ address: "8.8.8.8", family: 4 }]);
  await assert.rejects(resolveSafeUrl("https://example.com", { lookup: async () => { throw new Error("DNS failure"); } }), /destination DNS lookup failed/);
});
