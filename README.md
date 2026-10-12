# KoinKob Opportunity Intelligence Worker

This repository preserves the historical Opportunity Engine / Opportunity Feed Intelligence capability as a **supporting worker for KoinKob**, not a seventh Apex app.

## Runtime contract

- Opportunity, ingestion, engine-state, and raw-liquidity records are written to the canonical Apex API's Pure-WAL/Ring-WAL store.
- The active scanner does not initialize PostgreSQL or a SQL-backed queue.
- `APEX_OPPORTUNITY_ENGINE_API_URL` and `APEX_OPPORTUNITY_ENGINE_TOKEN` configure the authenticated Pure-WAL receiver.
- `DATABASE_URL` is read only by the explicitly enabled, one-time `MIGRATE_POSTGRES_TO_SOVEREIGN=true` migration path. That migration performs read-only SELECTs and copies records into Pure-WAL; it never deletes or mutates source rows.
- Do not remove the legacy Render database until migrated record counts and read-back checks have been verified and the user explicitly approves deletion.

## Local verification

```sh
npm install
npm run check
npm test
```
