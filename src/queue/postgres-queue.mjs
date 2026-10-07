import crypto from "node:crypto";
export class PostgresQueue{
  constructor(pool,{leaseSeconds=60,maxAttempts=8}={}){this.pool=pool;this.leaseSeconds=leaseSeconds;this.maxAttempts=maxAttempts}
  async enqueue(e){await this.pool.query("INSERT INTO ingestion_events(fingerprint,source,sequence,payload,observed_at) VALUES($1,$2,$3,$4,$5) ON CONFLICT(fingerprint) DO NOTHING",[e.fingerprint,e.source,e.sequence,e.payload,e.observed_at])}
  async claim(limit=8){
    const c=await this.pool.connect();
    try{await c.query("BEGIN");
      const r=await c.query(`WITH picked AS (
        SELECT id FROM ingestion_events
        WHERE status='queued' OR (status='processing' AND lease_until<now())
        ORDER BY id FOR UPDATE SKIP LOCKED LIMIT $1
      ) UPDATE ingestion_events e SET status='processing',attempts=e.attempts+1,
        lease_token=$2::uuid,lease_until=now()+($3::int*interval '1 second'),updated_at=now()
        FROM picked WHERE e.id=picked.id RETURNING e.*`,[limit,crypto.randomUUID(),this.leaseSeconds]);
      await c.query("COMMIT");return r.rows;
    }catch(e){await c.query("ROLLBACK");throw e}finally{c.release()}
  }
  async heartbeat(job){const r=await this.pool.query("UPDATE ingestion_events SET lease_until=now()+($3::int*interval '1 second'),updated_at=now() WHERE id=$1 AND lease_token=$2::uuid AND status='processing'",[job.id,job.lease_token,this.leaseSeconds]);return r.rowCount===1}
  async process(job,handler){
    await handler(job);
    const r=await this.pool.query("UPDATE ingestion_events SET status='processed',lease_token=NULL,lease_until=NULL,updated_at=now() WHERE id=$1 AND lease_token=$2::uuid AND status='processing'",[job.id,job.lease_token]);
    if(r.rowCount!==1)throw new Error("lease lost");
  }
  async fail(job,error){
    await this.pool.query(`UPDATE ingestion_events SET status=CASE WHEN attempts>=$3 THEN 'failed' ELSE 'queued' END,
      lease_token=NULL,lease_until=NULL,last_error=$4,updated_at=now() WHERE id=$1 AND lease_token=$2::uuid`,
      [job.id,job.lease_token,this.maxAttempts,String(error?.message||error)]);
  }
  async recoverExpired(){await this.pool.query("UPDATE ingestion_events SET status=CASE WHEN attempts>=$1 THEN 'failed' ELSE 'queued' END,lease_token=NULL,lease_until=NULL,updated_at=now() WHERE status='processing' AND lease_until<now()",[this.maxAttempts])}
  async saveOpportunity(o,observedAt){
    await this.pool.query(`INSERT INTO opportunities
      (fingerprint,kind,chain_id,token_in,token_out,buy_pool,sell_pool,gross_edge_bps,estimated_gas_usd,estimated_slippage_usd,estimated_net_profit_usd,observed_at,payload)
      VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
      ON CONFLICT(fingerprint) DO UPDATE SET gross_edge_bps=EXCLUDED.gross_edge_bps,
      estimated_gas_usd=EXCLUDED.estimated_gas_usd,estimated_slippage_usd=EXCLUDED.estimated_slippage_usd,
      estimated_net_profit_usd=EXCLUDED.estimated_net_profit_usd,observed_at=EXCLUDED.observed_at,payload=EXCLUDED.payload`,
      [o.fingerprint,o.kind,o.chain_id,o.token_in,o.token_out,o.buy_pool,o.sell_pool,o.gross_edge_bps,o.estimated_gas_usd,o.estimated_slippage_usd,o.estimated_net_profit_usd,observedAt,o]);
  }
  async stats(){const r=await this.pool.query("SELECT count(*) FILTER(WHERE status='queued') queued,count(*) FILTER(WHERE status='processing') processing,count(*) FILTER(WHERE status='processed') processed,count(*) FILTER(WHERE status='failed') failed FROM ingestion_events");return r.rows[0]}
  async shutdown(){await this.recoverExpired()}
}