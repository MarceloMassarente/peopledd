-- peopledd job queue and metadata (Postgres)
-- Apply once per environment: psql "$DATABASE_URL" -f migrations/001_jobs.sql

CREATE TABLE IF NOT EXISTS jobs (
    job_id UUID PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN (
        'queued', 'running', 'succeeded', 'failed', 'cancelled'
    )),
    owner_sub TEXT,
    client_request_id TEXT,
    input_payload JSONB NOT NULL,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    error_message TEXT,
    final_report_json JSONB,
    dd_brief_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at);

CREATE INDEX IF NOT EXISTS idx_jobs_owner_created ON jobs (owner_sub, created_at DESC);

-- Idempotency: one active row per (owner, client_request_id) when both set
CREATE UNIQUE INDEX IF NOT EXISTS idx_jobs_owner_client_request
ON jobs (COALESCE(owner_sub, ''), client_request_id)
WHERE client_request_id IS NOT NULL AND client_request_id <> '';
