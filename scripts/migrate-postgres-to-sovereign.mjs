import pg from "pg";

const { Pool } = pg;
const BATCH_SIZE = 25;
const PAGE_SIZE = 500;

function iso(value) {
  return value instanceof Date ? value.toISOString() : value ?? null;
}

function numeric(value) {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : value;
}

function safeId(value, fallback) {
  const id = String(value ?? fallback);
  return id.length <= 128 ? id : "legacy-" + fallback;
}

function fingerprint(row, table) {
  if (row.fingerprint) return String(row.fingerprint);
  return "legacy-" + table + "-" + String(row.id ?? row.key ?? "unknown");
}

function mapRow(table, row) {
  const hash = fingerprint(row, table);
  if (table === "engine_state") {
    return {
      collection: "koin_kob_engine_state",
      id: safeId(row.key, hash),
      record: {
        key: String(row.key),
        value: row.value ?? null,
        updatedAt: iso(row.updated_at),
        source: "legacy-postgres",
        legacyTable: table
      }
    };
  }

  if (table === "opportunities") {
    const id = safeId(hash, "opportunity-" + row.id);
    return {
      collection: "koin_kob_opportunities",
      id,
      record: {
        ...row,
        id,
        legacyId: row.id,
        fingerprint: hash,
        chain_id: numeric(row.chain_id),
        gross_edge_bps: numeric(row.gross_edge_bps),
        estimated_gas_usd: numeric(row.estimated_gas_usd),
        estimated_slippage_usd: numeric(row.estimated_slippage_usd),
        estimated_net_profit_usd: numeric(row.estimated_net_profit_usd),
        observed_at: iso(row.observed_at),
        createdAt: iso(row.created_at),
        source: "legacy-postgres",
        legacyTable: table
      }
    };
  }

  if (table === "raw_liquidity_events") {
    const id = safeId(hash, "raw-" + row.id);
    return {
      collection: "koin_kob_raw_liquidity_events",
      id,
      record: {
        ...row,
        id,
        legacyId: row.id,
        fingerprint: hash,
        blockNumber: numeric(row.block_number),
        transactionHash: row.transaction_hash,
        tickSpacing: numeric(row.tick_spacing),
        fee: numeric(row.fee),
        observedAt: iso(row.observed_at),
        observed_at: iso(row.observed_at),
        source: "legacy-postgres",
        legacyTable: table
      }
    };
  }

  if (table === "ingestion_dead_letter") {
    const id = safeId("dead-letter-" + hash, "dead-letter-" + row.id);
    return {
      collection: "koin_kob_ingestion_events",
      id,
      record: {
        ...row,
        id,
        legacyId: row.id,
        fingerprint: hash,
        status: "dead-letter",
        deadLetter: true,
        failedAt: iso(row.failed_at),
        observed_at: iso(row.failed_at),
        createdAt: iso(row.failed_at),
        source: row.source || "legacy-postgres",
        legacyTable: table
      }
    };
  }

  const id = safeId(hash, "ingestion-" + row.id);
  return {
    collection: "koin_kob_ingestion_events",
    id,
    record: {
      ...row,
      id,
      legacyId: row.id,
      fingerprint: hash,
      observed_at: iso(row.observed_at),
      observedAt: iso(row.observed_at),
      createdAt: iso(row.created_at),
      updatedAt: iso(row.updated_at),
      nextAttemptAt: iso(row.next_attempt_at),
      leaseUntil: iso(row.lease_until),
      leaseToken: row.lease_token,
      lastError: row.last_error,
      source: row.source || "legacy-postgres",
      legacyTable: table
    }
  };
}

const TABLES = [
  { name: "opportunities", order: "id" },
  { name: "ingestion_events", order: "id" },
  { name: "ingestion_dead_letter", order: "id" },
  { name: "raw_liquidity_events", order: "id" },
  { name: "engine_state", order: "key" }
];

export async function migratePostgresToSovereign({
  databaseUrl = "",
  store,
  pool: suppliedPool = null,
  pageSize = PAGE_SIZE,
  batchSize = BATCH_SIZE
} = {}) {
  if (!store?.configured || typeof store.bulk !== "function") throw new Error("Sovereign Pure-WAL API URL and token must be configured before data migration");
  const markerId = "legacy-postgres-migration-v1";
  const existingMarker = await store.get("koin_kob_engine_state", markerId);
  if (existingMarker?.value?.complete === true) {
    return { ...existingMarker.value, alreadyComplete: true, databaseModified: false };
  }
  if (!databaseUrl && !suppliedPool) throw new Error("DATABASE_URL is required only for the one-time legacy data copy");

  const pool = suppliedPool || new Pool({
    connectionString: databaseUrl,
    max: 2,
    connectionTimeoutMillis: 10000,
    idleTimeoutMillis: 10000,
    statement_timeout: 30000
  });
  const ownsPool = !suppliedPool;
  const totals = {};

  try {
    for (const table of TABLES) {
      const exists = await pool.query("SELECT to_regclass($1) AS table_name", [table.name]);
      if (!exists.rows?.[0]?.table_name) {
        totals[table.name] = { rows: 0, written: 0, existing: 0, skipped: true };
        continue;
      }

      let offset = 0;
      let rowsRead = 0;
      let written = 0;
      let existing = 0;
      while (true) {
        const result = await pool.query(
          "SELECT * FROM " + table.name + " ORDER BY " + table.order + " ASC LIMIT $1 OFFSET $2",
          [Math.max(1, Math.min(2000, Number(pageSize) || PAGE_SIZE)), offset]
        );
        const rows = result.rows || [];
        if (!rows.length) break;

        const items = rows.map(row => mapRow(table.name, row));
        const size = Math.max(1, Math.min(100, Number(batchSize) || BATCH_SIZE));
        for (let index = 0; index < items.length; index += size) {
          const batch = items.slice(index, index + size);
          const response = await store.bulk(batch);
          written += Number(response.written || 0);
          existing += Number(response.existing || 0);
        }

        rowsRead += rows.length;
        offset += rows.length;
        if (rows.length < Math.max(1, Math.min(2000, Number(pageSize) || PAGE_SIZE))) break;
      }

      totals[table.name] = { rows: rowsRead, written, existing, skipped: false };
    }
  } finally {
    if (ownsPool) await pool.end();
  }

  const result = {
    source: "legacy-postgres-read-only",
    target: "sovereign-pure-wal",
    databaseModified: false,
    complete: true,
    completedAt: new Date().toISOString(),
    tables: totals,
    rowsRead: Object.values(totals).reduce((sum, item) => sum + item.rows, 0),
    written: Object.values(totals).reduce((sum, item) => sum + item.written, 0),
    existing: Object.values(totals).reduce((sum, item) => sum + item.existing, 0)
  };
  await store.put("koin_kob_engine_state", markerId, { key: markerId, value: result, updatedAt: result.completedAt });
  return result;
}
