import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { Wal } from "../src/wal/wal.mjs";

test("concurrent appends remain durable and replayable", async t => {
  const dir = await mkdtemp(path.join(tmpdir(), "apex-wal-concurrency-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const wal = new Wal(dir);
  await wal.init();
  await Promise.all(Array.from({ length: 40 }, (_, index) =>
    wal.append({ type: "test-record", index, value: "record-" + index })
  ));
  const records = [];
  const count = await wal.replay(record => records.push(record));
  assert.equal(count, 40);
  assert.deepEqual(records.map(record => record.index).sort((a, b) => a - b), Array.from({ length: 40 }, (_, index) => index));
});
