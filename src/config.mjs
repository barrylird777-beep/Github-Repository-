const n=(v,d,min=0)=>{const x=Number(v);return Number.isFinite(x)&&x>=min?x:d};
export const config={
  port:n(process.env.PORT,10000,1),
  rpcUrl:process.env.RPC_URL||"https://ethereum-rpc.publicnode.com",
  chainId:n(process.env.CHAIN_ID,1,1),
  scannerMs:n(process.env.SCANNER_MS,30000,10000),sniperMs:n(process.env.SNIPER_MS,5000,1000),
  scanNotionalUsd:n(process.env.SCAN_NOTIONAL_USD,1000,10),
  walDir:process.env.WAL_DIR||"./data",
meshSecret:process.env.MESH_SECRET||"",meshPeers:process.env.MESH_PEERS||""
};