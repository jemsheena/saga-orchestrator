-- Snapshot store for generic aggregates (Milestone 8)
CREATE TABLE IF NOT EXISTS snapshot_store (
  snapshot_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregate_id text NOT NULL,
  aggregate_type text NOT NULL,
  aggregate_version bigint NOT NULL,
  snapshot_version integer NOT NULL,
  payload bytea NOT NULL,
  created_at timestamp with time zone NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_snapshot_aggregate ON snapshot_store (aggregate_type, aggregate_id, aggregate_version DESC);
