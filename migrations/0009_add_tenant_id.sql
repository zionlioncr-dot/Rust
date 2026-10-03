-- ============================================================
-- Multi-tenant migration
-- Adds tenant_id to audit_events and outbox_events.
--
-- Existing historical rows are assigned to the explicit
-- migration tenant "legacy".
-- ============================================================

ALTER TABLE audit_events
    ADD COLUMN IF NOT EXISTS tenant_id TEXT;

ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS tenant_id TEXT;

-- Preserve existing data by assigning historical records
-- to an explicit migration tenant.
UPDATE audit_events
SET tenant_id = 'legacy'
WHERE tenant_id IS NULL;

UPDATE outbox_events
SET tenant_id = 'legacy'
WHERE tenant_id IS NULL;

ALTER TABLE audit_events
    ALTER COLUMN tenant_id SET NOT NULL;

ALTER TABLE outbox_events
    ALTER COLUMN tenant_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_created_at
    ON audit_events (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_outbox_events_tenant_published_created_at
    ON outbox_events (tenant_id, published, created_at);
