import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

test("remote evaluation requires explicit opt-in and a dedicated token", async () => {
  const source = await readFile(new URL("../src/index.mjs", import.meta.url), "utf8");
  const start = source.indexOf('u.pathname==="/api/eval/generate"');
  const end = source.indexOf('u.pathname==="/api/opportunities"', start);
  assert.ok(start >= 0 && end > start, "evaluation route must exist");
  const route = source.slice(start, end);
  assert.match(route, /const useModel=b\.useModel===true/);
  assert.match(route, /if\(useModel&&!authorizeScanRequest\(req,process\.env\.APEX_EVAL_TOKEN\|\|""\)\)return json\(res,401/);
  assert.ok(route.indexOf("authorizeScanRequest(req") < route.indexOf("evalHelper.generate"), "authorization must precede provider calls");
});

test("dashboard defaults to template mode and does not persist the eval token", async () => {
  const source = await readFile(new URL("../src/index.mjs", import.meta.url), "utf8");
  assert.match(source, /id="useModel" type="checkbox"/);
  assert.match(source, /id="evalToken" type="password" autocomplete="off"/);
  assert.match(source, /JSON\.stringify\(\{type:document\.getElementById\("evalType"\)\.value,input,useModel\}\)/);
  assert.doesNotMatch(source, /localStorage\.(setItem|getItem)\(["']evalToken/);
});
