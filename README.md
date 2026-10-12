# Github-Repository-

## Sovereign runtime

Working integration assumption: this historical Opportunity Engine's market-opportunity feed belongs in KoinKob's Pure-WAL data namespace; the legacy service is not being treated as a seventh canonical Apex app.

- Runtime records and queue state are stored in the Apex API's Pure-WAL store under the `koin_kob_*` collections.
- Required runtime settings: `APEX_WORKER_API_URL` and `APEX_OPPORTUNITY_ENGINE_TOKEN`.
- `DATABASE_URL` is only a one-time, read-only migration source. The migration is idempotent and writes a completion marker to Pure-WAL before the legacy database connection is removed.
- Do not use PostgreSQL as the ongoing queue, opportunity, or engine-state backend.
