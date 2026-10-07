const n=(v,d,min=0)=>{const x=Number(v);return Number.isFinite(x)&&x>=min?x:d};
export const config={
  port:n(process.env.PORT,10000,1),
  databaseUrl:process.env.DATABASE_URL||"",
  rpcUrl:process.env.RPC_URL||"https://cloudflare-eth.com",
  chainId:n(process.env.CHAIN_ID,1,1),
  pollMs:n(process.env.POLL_MS,15000,250),
  workerBatchSize:n(process.env.WORKER_BATCH_SIZE,8,1),
  scannerMs:n(process.env.SCANNER_MS,30000,10000),sniperMs:n(process.env.SNIPER_MS,5000,1000),
  scanNotionalUsd:n(process.env.SCAN_NOTIONAL_USD,1000,10),
  walDir:process.env.WAL_DIR||"./data",
  leaseSeconds:n(process.env.LEASE_SECONDS,60,10),
  maxAttempts:n(process.env.MAX_ATTEMPTS,8,1)
};