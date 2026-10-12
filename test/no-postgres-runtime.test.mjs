import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const read = async relative => readFile(new URL(relative, import.meta.url), "utf8");

test("active Opportunity Engine runtime has no PostgreSQL dependency", async () => {
  const [index, config, packageText, blueprint] = await Promise.all([
    read("../src/index.mjs"),
    read("../src/config.mjs"),
    read("../package.json"),
    read("../render.yaml")
  ]);
  const pkg = JSON.parse(packageText);
  assert.doesNotMatch(index, /from\s+["']pg["']/);
  assert.doesNotMatch(index, /PostgresQueue|DATABASE_URL|pool\.query/);
  assert.doesNotMatch(config, /databaseUrl|DATABASE_URL/);
  assert.equal(Boolean(pkg.dependencies?.pg), false);
  assert.doesNotMatch(blueprint, /DATABASE_URL|fromDatabase/);
});
