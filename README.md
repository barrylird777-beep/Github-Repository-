# Github-Repository-

## Sovereign runtime

This historical Opportunity Engine is being retained as a supporting scanner for KoinKob, not as a seventh canonical Apex app.

- Runtime records and queue state are stored in the Apex API's Pure-WAL store under the `koin_kob_*` collections.
- Required runtime settings: `APEX_WORKER_API_URL` and `APEX_OPPORTUNITY_ENGINE_TOKEN`.
- `DATABASE_URL` is only a one-time, read-only migration source. The migration is idempotent and writes a completion marker to Pure-WAL before the legacy database connection is removed.
- Do not use PostgreSQL as the ongoing queue, opportunity, or engine-state backend.
