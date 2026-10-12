import pg from "pg";

const { Pool } = pg;
const PAGE_SIZE = 100;
const WRITE_CONCURRENCY = 5;
const RETAINED_TABLES = [
  "ingestion_events",
  "ingestion_dead_letter",
  "raw_liquidity_events",
  "engine_state"
];

function iso(value) {
  return value instanceof Date ? value.toISOString() : value ?? null;
}

function numeric(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : value;
}

function normalizeLegacyOpportunity(row) {
  return {
    ...row,
    fingerprint: String(row.fingerprint || "legacy-opportunity-" + row.id),
    legacyId: row.id,
    chain_id: numeric(row.chain_id),
    gross_edge_bps: numeric(row.gross_edge_bps),
    estimated_gas_usd: numeric(row.estimated_gas_usd),
    estimated_slippage_usd: numeric(row.estimated_slippage_usd),
    estimated_net_profit_usd: numeric(row.estimated_net_profit_usd),
    observed_at: iso(row.observed_at),
    source: "legacy-postgres-migration",
    payload: row.payload ?? {}
  };
}

export async function migratePostgresOpportunitiesToKashKorner({
  databaseUrl = "",
  api,
  pool: suppliedPool = null,
  pageSize = PAGE_SIZE,
  writeConcurrency = WRITE_CONCURRENCY
} = {}) {
  if (!api?.configured || typeof api.saveOpportunity !== "function") {
    throw new Error("KashKorner Pure-WAL API URL and token must be configured before migration");
  }
  if (!databaseUrl && !suppliedPool) {
    throw new Error("DATABASE_URL is required only for the one-time read-only legacy opportunity copy");
  }

  const pool = suppliedPool || new Pool({
    connectionString: databaseUrl,
    max: 2,
    connectionTimeoutMillis: 10000,
    idleTimeoutMillis: 10000,
    statement_timeout: 30000
  });
  const ownsPool = !suppliedPool;
  const retainedInLegacyDatabase = {};

  try {
    const exists = await pool.query("SELECT to_regclass($1) AS table_name", ["opportunities"]);
    if (!exists.rows?.[0]?.table_name) {
      return {
        source: "legacy-postgres-read-only",
        target: "kash-korner-pure-wal",
        databaseModified: false,
        rowsRead: 0,
        saved: 0,
        tableMissing: true,
        retainedInLegacyDatabase
      };
    }

    for (const table of RETAINED_TABLES) {
      const tableExists = await pool.query("SELECT to_regclass($1) AS table_name", [table]);
      if (!tableExists.rows?.[0]?.table_name) {
        retainedInLegacyDatabase[table] = 0;
        continue;
      }
      const count = await pool.query("SELECT COUNT(*)::bigint AS rows FROM " + table);
      retainedInLegacyDatabase[table] = Number(count.rows?.[0]?.rows || 0);
    }

    let offset = 0;
    let rowsRead = 0;
    let saved = 0;
    const limit = Math.max(1, Math.min(1000, Math.floor(Number(pageSize) || PAGE_SIZE)));
    const concurrency = Math.max(1, Math.min(20, Math.floor(Number(writeConcurrency) || WRITE_CONCURRENCY)));

    while (true) {
      const result = await pool.query(
        "SELECT * FROM opportunities ORDER BY id ASC LIMIT $1 OFFSET $2",
        [limit, offset]
      );
      const rows = result.rows || [];
      if (!rows.length) break;
      rowsRead += rows.length;

      const normalized = rows.map(normalizeLegacyOpportunity);
      for (let index = 0; index < normalized.length; index += concurrency) {
        const batch = normalized.slice(index, index + concurrency);
        await Promise.all(batch.map(opportunity => api.saveOpportunity(opportunity)));
        saved += batch.length;
      }

      offset += rows.length;
      if (rows.length < limit) break;
    }

    return {
      source: "legacy-postgres-read-only",
      target: "kash-korner-pure-wal",
      databaseModified: false,
      rowsRead,
      saved,
      retainedInLegacyDatabase
    };
  } finally {
    if (ownsPool) await pool.end();
  }
}
