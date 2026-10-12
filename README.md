# Opportunity Feed Intelligence

Opportunity Engine is the market-scanning service supporting canonical **KashKorner**. Its authoritative opportunity records are written to PlanetApeX's sovereign Pure-WAL store through the KashKorner API; PostgreSQL is not part of the active runtime.

## Required configuration

- `RPC_URL`: chain RPC endpoint for scanning.
- `APEX_SOVEREIGN_API_URL`: canonical Apex API base URL (defaults to `https://apex-studio-production.up.railway.app`).
- `APEX_OPPORTUNITY_ENGINE_TOKEN`: dedicated service-to-service token shared with the Apex API. Keep it secret; never commit it.
- `WAL_DIR`: local append-only retry journal path.

The local WAL is an outbox/retry journal, **not** the authoritative store. If the API URL or token is missing, health reports `degraded`; the service must not claim that opportunities were durably saved.

## Endpoints

- `GET /health`: runtime, scanner, and sovereign-store status.
- `GET /api/opportunities`: reads opportunity records from KashKorner's Pure-WAL API.
- `POST /api/scan`: triggers a scan.
- `POST /api/ingest`: appends an ingestion event to the local WAL.

## Verification

`npm run check` performs syntax checks. `npm test` runs the WAL, outbox, and runtime regression tests.

Legacy PostgreSQL queue/backup files are retained for historical recovery only; the active entrypoint does not import them. The existing database resource is not deleted by this migration.
