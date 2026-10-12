import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { Wal } from "../src/wal/wal.mjs";
import { opportunityFingerprint } from "../src/scanner/uniswap-v3.mjs";

test("Pure-WAL appends checksummed records and replays them in order", async () => {
 const dir=await mkdtemp(path.join(os.tmpdir(),"apex-wal-test-"));
 try { const wal=await new Wal(dir).init();await wal.append({type:"test",value:1});await wal.append({type:"test",value:2});const records=[];assert.equal(await wal.replay(record=>records.push(record)),2);assert.deepEqual(records,[{type:"test",value:1},{type:"test",value:2}]); }
 finally { await rm(dir,{recursive:true,force:true}); }
});
test("Pure-WAL rejects records whose contents no longer match their checksum", async () => {
 const dir=await mkdtemp(path.join(os.tmpdir(),"apex-wal-corrupt-"));
 try { const wal=await new Wal(dir).init();await wal.append({type:"test",value:1});const file=path.join(dir,"events.wal");const contents=await readFile(file,"utf8");await writeFile(file,contents.replace('"value":1','"value":2'),"utf8");await assert.rejects(()=>wal.replay(),/WAL checksum mismatch/); }
 finally { await rm(dir,{recursive:true,force:true}); }
});
test("opportunity fingerprints stay stable as observation time and estimated profit change", () => {
 const base={kind:"uniswap-v3-cross-pool",chain_id:1,token_in:"A",token_out:"B",buy_pool:"pool-a",sell_pool:"pool-b",buy_fee:500,sell_fee:3000,observed_at:"2026-10-12T00:00:00.000Z",estimated_net_profit_usd:2};
 assert.equal(opportunityFingerprint(base),opportunityFingerprint({...base,observed_at:"2026-10-12T00:01:00.000Z",estimated_net_profit_usd:4}));
 assert.notEqual(opportunityFingerprint(base),opportunityFingerprint({...base,sell_pool:"pool-c"}));
});
