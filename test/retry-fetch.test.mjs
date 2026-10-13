import test from "node:test";
import assert from "node:assert/strict";
import {fetchWithRetry} from "../src/network/retry-fetch.mjs";

test("retries transient 502 responses with exponential backoff", async () => {
  const statuses = [502, 502, 200];
  const delays = [];
  let calls = 0;
  const response = await fetchWithRetry("https://upstream.invalid", {}, {
    fetchImpl: async () => ({status: statuses[calls++], headers: {get: () => null}}),
    sleep: async ms => delays.push(ms),
    baseDelayMs: 10,
    maxDelayMs: 100,
  });
  assert.equal(response.status, 200);
  assert.equal(calls, 3);
  assert.deepEqual(delays, [10, 20]);
});

test("honors Retry-After for rate-limited or gateway responses", async () => {
  const delays = [];
  let calls = 0;
  const response = await fetchWithRetry("https://upstream.invalid", {}, {
    fetchImpl: async () => ({
      status: calls++ ? 200 : 429,
      headers: {get: name => name === "retry-after" ? "2" : null},
    }),
    sleep: async ms => delays.push(ms),
    baseDelayMs: 10,
  });
  assert.equal(response.status, 200);
  assert.deepEqual(delays, [2000]);
});

test("does not retry permanent client errors", async () => {
  let calls = 0;
  const response = await fetchWithRetry("https://upstream.invalid", {}, {
    fetchImpl: async () => ({status: ++calls === 1 ? 401 : 200, headers: {get: () => null}}),
    sleep: async () => assert.fail("must not sleep"),
  });
  assert.equal(response.status, 401);
  assert.equal(calls, 1);
});

test("retries transient network errors and propagates aborts", async () => {
  let calls = 0;
  const delays = [];
  const response = await fetchWithRetry("https://upstream.invalid", {}, {
    fetchImpl: async () => {
      calls += 1;
      if (calls === 1) throw new TypeError("socket reset");
      return {status: 200, headers: {get: () => null}};
    },
    sleep: async ms => delays.push(ms),
    baseDelayMs: 5,
  });
  assert.equal(response.status, 200);
  assert.deepEqual(delays, [5]);
  await assert.rejects(fetchWithRetry("https://upstream.invalid", {}, {
    fetchImpl: async () => { const error = new Error("aborted"); error.name = "AbortError"; throw error; },
    sleep: async () => assert.fail("must not sleep"),
  }), {name: "AbortError"});
});
