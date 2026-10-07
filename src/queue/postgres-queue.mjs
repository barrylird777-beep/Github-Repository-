import crypto from "node:crypto";

function backoff(attempts) {
  const base = Math.min(300, 2 ** Math.min(attempts, 8));
  const jitter = Math.floor(Math.random() * Math.max(1, Math.floor(base * 0.25)));
  return base + jitter;
}

export class PostgresQueue {
  constructor(pool, {leaseSeconds=60, maxAttempts=8}={}) {
    if (!pool) throw new TypeError("pool is required");
    this.pool = pool;
    this.leaseSeconds = Math.max(10, Number(leaseSeconds) || 60);
    this.maxAttempts = Math.max(1, Number(maxAttempts) || 8);
  }

  async enqueue(event) {
    await this.pool.query(
      `INSERT INTO ingestion_events
       (fingerprint,source,sequence,payload,observed_at,next_attempt_at)
       VALUES($1,$2,$3,$4,$5,now())
       ON CONFLICT(fingerprint) DO NOTHING`,
      [event.fingerprint,event.source,event.sequence,event.payload,event.observed_at]
    );
  }

  async claim(limit=8) {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const picked = await client.query(
        `SELECT id
           FROM ingestion_events
          WHERE status='queued'
            AND next_attempt_at <= now()
            AND attempts < $1
          ORDER BY id
          FOR UPDATE SKIP LOCKED
          LIMIT $2`,
        [this.maxAttempts, Math.max(1, Number(limit) || 1)]
      );

      const jobs = [];
      for (const row of picked.rows) {
        const token = crypto.randomUUID();
        const updated = await client.query(
          `UPDATE ingestion_events
              SET status='processing',
                  attempts=attempts+1,
                  lease_token=$2::uuid,
                  lease_until=now()+($3::int*interval '1 second'),
                  updated_at=now()
            WHERE id=$1
            RETURNING *`,
          [row.id, token, this.leaseSeconds]
        );
        if (updated.rowCount === 1) jobs.push(updated.rows[0]);
      }

      await client.query("COMMIT");
      return jobs;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async heartbeat(job) {
    const result = await this.pool.query(
      `UPDATE ingestion_events
          SET lease_until=now()+($3::int*interval '1 second'),updated_at=now()
        WHERE id=$1
          AND lease_token=$2::uuid
          AND status='processing'`,
      [job.id, job.lease_token, this.leaseSeconds]
    );
    return result.rowCount === 1;
  }

  async process(job, handler) {
    let heartbeatTimer = null;
    let leaseLost = false;

    try {
      heartbeatTimer = setInterval(() => {
        this.heartbeat(job).then(ok => {
          if (!ok) leaseLost = true;
        }).catch(() => {
          leaseLost = true;
        });
      }, Math.max(5000, Math.floor(this.leaseSeconds * 1000 / 3)));

      await handler(job);

      if (leaseLost || !(await this.heartbeat(job))) {
        throw new Error("lease lost");
      }

      const result = await this.pool.query(
        `UPDATE ingestion_events
            SET status='processed',
                lease_token=NULL,
                lease_until=NULL,
                updated_at=now()
          WHERE id=$1
            AND lease_token=$2::uuid
            AND status='processing'`,
        [job.id, job.lease_token]
      );

      if (result.rowCount !== 1) throw new Error("lease lost");
    } finally {
      if (heartbeatTimer) clearInterval(heartbeatTimer);
    }
  }

  async fail(job, error) {
    const message = String(error?.message || error);
    const client = await this.pool.connect();

    try {
      await client.query("BEGIN");

      const current = await client.query(
        `SELECT attempts,payload,source,sequence,observed_at
           FROM ingestion_events
          WHERE id=$1 AND lease_token=$2::uuid AND status='processing'
          FOR UPDATE`,
        [job.id, job.lease_token]
      );

      if (current.rowCount !== 1) {
        await client.query("ROLLBACK");
        return false;
      }

      const attempts = current.rows[0].attempts;
      const terminal = attempts >= this.maxAttempts;

      if (terminal) {
        await client.query(
          `INSERT INTO ingestion_dead_letter
             (ingestion_id,fingerprint,source,sequence,payload,attempts,last_error,failed_at)
           SELECT id,fingerprint,source,sequence,payload,attempts,$2,now()
             FROM ingestion_events
            WHERE id=$1
           ON CONFLICT(ingestion_id) DO UPDATE
             SET attempts=EXCLUDED.attempts,last_error=EXCLUDED.last_error,failed_at=EXCLUDED.failed_at`,
          [job.id, message]
        );
      }

      await client.query(
        `UPDATE ingestion_events
            SET status=$3,
                lease_token=NULL,
                lease_until=NULL,
                last_error=$2,
                next_attempt_at=CASE WHEN $3='queued'
                  THEN now()+($4::int*interval '1 second')
                  ELSE next_attempt_at END,
                updated_at=now()
          WHERE id=$1 AND lease_token=$5::uuid`,
        [job.id, message, terminal ? "failed" : "queued", backoff(attempts), job.lease_token]
      );

      await client.query("COMMIT");
      return true;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }

  async recoverExpired() {
    await this.pool.query(
      `UPDATE ingestion_events
          SET status=CASE WHEN attempts >= $1 THEN 'failed' ELSE 'queued' END,
              lease_token=NULL,
              lease_until=NULL,
              next_attempt_at=CASE WHEN attempts >= $1 THEN next_attempt_at ELSE now() END,
              updated_at=now()
        WHERE status='processing'
          AND lease_until < now()`,
      [this.maxAttempts]
    );
  }

  async saveOpportunity(o, observedAt) {
    await this.pool.query(
      `INSERT INTO opportunities
       (fingerprint,kind,chain_id,token_in,token_out,buy_pool,sell_pool,
        gross_edge_bps,estimated_gas_usd,estimated_slippage_usd,
        estimated_net_profit_usd,observed_at,payload)
       VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
       ON CONFLICT(fingerprint) DO UPDATE SET
        gross_edge_bps=EXCLUDED.gross_edge_bps,
        estimated_gas_usd=EXCLUDED.estimated_gas_usd,
        estimated_slippage_usd=EXCLUDED.estimated_slippage_usd,
        estimated_net_profit_usd=EXCLUDED.estimated_net_profit_usd,
        observed_at=EXCLUDED.observed_at,
        payload=EXCLUDED.payload`,
      [o.fingerprint,o.kind,o.chain_id,o.token_in,o.token_out,o.buy_pool,o.sell_pool,
       o.gross_edge_bps,o.estimated_gas_usd,o.estimated_slippage_usd,
       o.estimated_net_profit_usd,observedAt,o]
    );
  }

  async stats() {
    const result = await this.pool.query(
      `SELECT
        count(*) FILTER(WHERE status='queued') queued,
        count(*) FILTER(WHERE status='processing') processing,
        count(*) FILTER(WHERE status='processed') processed,
        count(*) FILTER(WHERE status='failed') failed,
        (SELECT count(*) FROM ingestion_dead_letter) dead_letter
       FROM ingestion_events`
    );
    return result.rows[0];
  }

  async shutdown() {
    await this.recoverExpired();
  }
}
