BEGIN;

CREATE TABLE IF NOT EXISTS aml_alerts (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    transaction_id UUID NOT NULL,
    rule_code TEXT NOT NULL,
    severity TEXT NOT NULL
        CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    description TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'investigating', 'closed', 'false_positive')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_aml_alert_tenant_event_rule
        UNIQUE (tenant_id, event_id, rule_code)
);

CREATE INDEX IF NOT EXISTS idx_aml_alerts_tenant_status_created
    ON aml_alerts (tenant_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_aml_alerts_tenant_transaction
    ON aml_alerts (tenant_id, transaction_id);

CREATE INDEX IF NOT EXISTS idx_aml_alerts_tenant_rule_created
    ON aml_alerts (tenant_id, rule_code, created_at DESC);

CREATE TABLE IF NOT EXISTS aml_processed_events (
    tenant_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, event_id)
);

COMMIT;
