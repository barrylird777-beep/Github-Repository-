export async function migrateLegacyPostgresToPureWal({ remoteStore, databaseUrl, poolFactory = null, logger = console } = {}) {
  if (!remoteStore) throw new TypeError('remote Pure-WAL store is required');
  const markerKey = 'legacy-postgres-migration-v1';
  const marker = await remoteStore.get('koin_kob_engine_state', markerKey);
  if (marker) {
    logger.log('legacy PostgreSQL snapshot already migrated', JSON.stringify(marker.value?.counts || {}));
    return { skipped: true, counts: marker.value?.counts || {} };
  }
  if (!databaseUrl) {
    logger.log('no legacy PostgreSQL source configured; Pure-WAL is authoritative');
    return { skipped: true, counts: {} };
  }

  let pool;
  if (poolFactory) {
    pool = await poolFactory(databaseUrl);
  } else {
    const { Pool } = await import('pg');
    pool = new Pool({ connectionString: databaseUrl, max: 2, idleTimeoutMillis: 10000, connectionTimeoutMillis: 10000 });
  }

  const counts = { ingestionEvents: 0, opportunities: 0, engineState: 0, rawLiquidityEvents: 0 };
  const iso = value => value ? new Date(value).toISOString() : new Date().toISOString();

  async function copyPaged(sql, collection, mapper, countKey) {
    let lastId = 0;
    while (true) {
      const result = await pool.query(sql, [lastId, 250]);
      if (!result.rows.length) break;
      for (const row of result.rows) {
        const id = String(row.fingerprint || '');
        if (!id) continue;
        await remoteStore.put(collection, id, mapper(row, id));
      }
      lastId = Number(result.rows[result.rows.length - 1].id);
      counts[countKey] += result.rows.length;
    }
  }

  try {
    await copyPaged(
      'SELECT id,fingerprint,source,sequence,observed_at,payload,status,attempts,lease_token,lease_until,last_error,next_attempt_at,created_at,updated_at FROM ingestion_events WHERE id > $1 ORDER BY id ASC LIMIT $2',
      'koin_kob_ingestion_events',
      (row, id) => ({
        id, fingerprint: id, source: row.source, sequence: row.sequence, payload: row.payload,
        observed_at: iso(row.observed_at), status: row.status, attempts: Number(row.attempts || 0),
        leaseToken: row.lease_token || null, leaseUntil: row.lease_until ? iso(row.lease_until) : null,
        lastError: row.last_error || null, nextAttemptAt: iso(row.next_attempt_at),
        createdAt: iso(row.created_at), updatedAt: iso(row.updated_at)
      }),
      'ingestionEvents'
    );

    await copyPaged(
      'SELECT id,fingerprint,kind,chain_id,token_in,token_out,buy_pool,sell_pool,gross_edge_bps,estimated_gas_usd,estimated_slippage_usd,estimated_net_profit_usd,observed_at,payload,created_at FROM opportunities WHERE id > $1 ORDER BY id ASC LIMIT $2',
      'koin_kob_opportunities',
      (row, id) => ({
        ...((row.payload && typeof row.payload === 'object') ? row.payload : {}),
        id, fingerprint: id, kind: row.kind, chain_id: row.chain_id,
        token_in: row.token_in, token_out: row.token_out, buy_pool: row.buy_pool, sell_pool: row.sell_pool,
        gross_edge_bps: Number(row.gross_edge_bps || 0),
        estimated_gas_usd: row.estimated_gas_usd == null ? null : Number(row.estimated_gas_usd),
        estimated_slippage_usd: row.estimated_slippage_usd == null ? null : Number(row.estimated_slippage_usd),
        estimated_net_profit_usd: row.estimated_net_profit_usd == null ? null : Number(row.estimated_net_profit_usd),
        observed_at: iso(row.observed_at), createdAt: iso(row.created_at)
      }),
      'opportunities'
    );

    const state = await pool.query('SELECT key,value,updated_at FROM engine_state ORDER BY key ASC');
    for (const row of state.rows) {
      await remoteStore.put('koin_kob_engine_state', String(row.key), {
        id: String(row.key), key: String(row.key), value: row.value, updatedAt: iso(row.updated_at)
      });
      counts.engineState += 1;
    }

    await copyPaged(
      'SELECT id,fingerprint,block_number,transaction_hash,token0,token1,fee,tick_spacing,pool,observed_at,payload FROM raw_liquidity_events WHERE id > $1 ORDER BY id ASC LIMIT $2',
      'koin_kob_raw_liquidity_events',
      (row, id) => ({
        ...((row.payload && typeof row.payload === 'object') ? row.payload : {}),
        id, fingerprint: id, blockNumber: Number(row.block_number),
        transactionHash: row.transaction_hash, token0: row.token0, token1: row.token1,
        fee: Number(row.fee), tickSpacing: Number(row.tick_spacing), pool: row.pool,
        observedAt: iso(row.observed_at), observed_at: iso(row.observed_at)
      }),
      'rawLiquidityEvents'
    );

    const result = {
      counts,
      migratedAt: new Date().toISOString(),
      source: 'legacy-render-postgres'
    };
    await remoteStore.put('koin_kob_engine_state', markerKey, {
      id: markerKey, key: markerKey, value: result, updatedAt: result.migratedAt
    });
    logger.log('legacy PostgreSQL snapshot copied to Pure-WAL', JSON.stringify(counts));
    return { skipped: false, ...result };
  } finally {
    await pool.end();
  }
}
