import test from "node:test";
import assert from "node:assert/strict";
import { isBlockedAddress, resolveSafeUrl } from "../src/extract/instant-extract.mjs";

test("extractor blocks private, loopback, link-local, carrier-grade NAT, and reserved IPv4", () => {
  for (const address of [
    "0.0.0.0", "10.1.2.3", "100.64.0.1", "127.0.0.1", "169.254.169.254",
    "172.16.0.1", "172.31.255.254", "192.168.1.1", "198.18.0.1",
    "192.0.2.10", "198.51.100.10", "203.0.113.10", "224.0.0.1", "240.0.0.1"
  ]) assert.equal(isBlockedAddress(address), true, address);
});

test("extractor blocks non-public IPv6 and IPv4-mapped private addresses", () => {
  for (const address of [
    "::", "::1", "fc00::1", "fd12::1", "fe80::1", "ff02::1",
    "::ffff:127.0.0.1", "::ffff:7f00:1", "::ffff:192.168.1.1",
    "2001:db8::1", "2001:0:1::1", "2002::1"
  ]) assert.equal(isBlockedAddress(address), true, address);
});

test("extractor permits representative public IPv4 and IPv6 addresses", () => {
  assert.equal(isBlockedAddress("93.184.216.34"), false);
  assert.equal(isBlockedAddress("8.8.8.8"), false);
  assert.equal(isBlockedAddress("2001:4860:4860::8888"), false);
  assert.equal(isBlockedAddress("2606:4700:4700::1111"), false);
});

test("URL validation rejects a private DNS result and mixed public/private answers", async () => {
  const privateLookup = async () => [{ address: "127.0.0.1", family: 4 }];
  const mixedLookup = async () => [
    { address: "93.184.216.34", family: 4 },
    { address: "169.254.169.254", family: 4 }
  ];
  await assert.rejects(resolveSafeUrl("https://example.com", { lookup: privateLookup }), /private or non-public destination blocked/);
  await assert.rejects(resolveSafeUrl("https://example.com", { lookup: mixedLookup }), /private or non-public destination blocked/);
});

test("URL validation rejects credentials, non-web protocols, and DNS failures", async () => {
  await assert.rejects(resolveSafeUrl("https://user:pass@example.com", { lookup: async () => [{ address: "93.184.216.34", family: 4 }] }), /invalid URL/);
  await assert.rejects(resolveSafeUrl("file:///etc/passwd"), /invalid URL/);
  await assert.rejects(resolveSafeUrl("https://example.com", { lookup: async () => { throw new Error("DNS failure"); } }), /destination DNS lookup failed/);
});

test("URL validation returns vetted addresses for the pinned network request", async () => {
  const lookup = async hostname => {
    assert.equal(hostname, "example.com");
    return [{ address: "93.184.216.34", family: 4 }];
  };
  const result = await resolveSafeUrl("https://example.com/path", { lookup });
  assert.equal(result.url.hostname, "example.com");
  assert.deepEqual(result.addresses, [{ address: "93.184.216.34", family: 4 }]);
});
