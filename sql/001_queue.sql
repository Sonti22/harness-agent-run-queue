CREATE TABLE dialogs (
  id text PRIMARY KEY CHECK (length(id) BETWEEN 1 AND 256),
  last_sequence bigint NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
  last_error jsonb
);

CREATE TABLE runs (
  id uuid PRIMARY KEY,
  dialog_id text NOT NULL REFERENCES dialogs(id),
  dialog_sequence bigint NOT NULL CHECK (dialog_sequence > 0),
  idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  payload jsonb NOT NULL,
  status text NOT NULL DEFAULT 'queued' CHECK (status IN ('queued', 'running', 'completed', 'failed')),
  attempts integer NOT NULL DEFAULT 0 CHECK (attempts BETWEEN 0 AND 3),
  worker_id text,
  lease_token uuid,
  lease_until timestamptz,
  result jsonb,
  error text,
  created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (dialog_id, idempotency_key),
  UNIQUE (dialog_id, dialog_sequence),
  CHECK (
    (status = 'running' AND worker_id IS NOT NULL AND length(worker_id) BETWEEN 1 AND 256
      AND lease_token IS NOT NULL AND lease_until IS NOT NULL AND attempts > 0)
    OR
    (status <> 'running' AND worker_id IS NULL AND lease_token IS NULL AND lease_until IS NULL)
  ),
  CHECK (status <> 'queued' OR attempts < 3),
  CHECK (status NOT IN ('completed', 'failed') OR attempts > 0)
);

-- A final database-level guard, even if a caller bypasses the queue's locking protocol.
CREATE UNIQUE INDEX runs_one_running_per_dialog ON runs (dialog_id) WHERE status = 'running';
CREATE INDEX runs_pending_fifo ON runs (dialog_id, dialog_sequence) WHERE status IN ('queued', 'running');
CREATE INDEX runs_expired_leases ON runs (lease_until) WHERE status = 'running';
