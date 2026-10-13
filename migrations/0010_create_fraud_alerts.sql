-- ============================================================
-- Anti-Fraud alerts
--
-- Stores fraud decisions generated from audit events.
-- Tenant isolation is enforced at the data-access layer.
-- ============================================================

CREATE TABLE IF NOT EXISTS fraud_alerts
(
    id UUID PRIMARY KEY,

    tenant_id TEXT NOT NULL,

    event_id UUID NOT NULL,

    audit_id UUID NOT NULL,

    username TEXT NOT NULL,

    risk_level TEXT NOT NULL,

    triggered_rules JSONB NOT NULL DEFAULT '[]'::jsonb,

    evidence JSONB NOT NULL DEFAULT '[]'::jsonb,

    status TEXT NOT NULL DEFAULT 'OPEN',

    created_at TIMESTAMPTZ NOT NULL,

    updated_at TIMESTAMPTZ NOT NULL,

    CONSTRAINT fraud_alerts_risk_level_check
        CHECK (risk_level IN ('Low', 'Medium', 'High', 'Critical')),

    CONSTRAINT fraud_alerts_status_check
        CHECK (status IN ('OPEN', 'REVIEWED', 'CONFIRMED', 'DISMISSED'))
);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_tenant_created_at
    ON fraud_alerts (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_tenant_status_created_at
    ON fraud_alerts (tenant_id, status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_fraud_alerts_tenant_event_id
    ON fraud_alerts (tenant_id, event_id);