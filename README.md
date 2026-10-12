# KoinKob / KashKorner Opportunity Intelligence Worker

This repository preserves the historical Opportunity Engine / Opportunity Feed Intelligence capability as a **supporting worker for KashKorner**, not a seventh Apex app. KoinKob remains the autonomous-economy simulation app; this worker feeds KashKorner's durable opportunity surface.

## Runtime contract

- Live opportunity records are written to the canonical Apex API's KashKorner Pure-WAL namespace.
- The active scanner does not initialize PostgreSQL or a SQL-backed queue.
- `APEX_OPPORTUNITY_ENGINE_API_URL` and `APEX_OPPORTUNITY_ENGINE_TOKEN` configure authenticated opportunity writes.
- `DATABASE_URL` is used only when `MIGRATE_POSTGRES_TO_SOVEREIGN=true` for a one-time, read-only copy of legacy opportunity rows.
- The migration never mutates or deletes the source database. Other legacy queue/telemetry tables remain untouched in that database as an archive; the active scanner no longer reads them.
- After migration logs confirm the copied row count and read-back checks pass, set `MIGRATE_POSTGRES_TO_SOVEREIGN=false`. Do not delete the old database unless the user explicitly approves it.

## Local verification

```sh
npm install
npm run check
npm test
```
