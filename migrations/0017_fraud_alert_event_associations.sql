-- Associates each failed-login event with its consolidated fraud alert.
-- Manual migration: never apply automatically during service startup.

CREATE TABLE IF NOT EXISTS fraud_alert_events (
    tenant_id TEXT NOT NULL,
    event_id UUID NOT NULL,
    alert_id UUID NOT NULL REFERENCES fraud_alerts(id) ON DELETE CASCADE,
    audit_id UUID NOT NULL,
    username TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    associated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_fraud_alert_events
        PRIMARY KEY (tenant_id, event_id),

    CONSTRAINT ck_fraud_alert_events_tenant_not_empty
        CHECK (length(trim(tenant_id)) > 0),

    CONSTRAINT ck_fraud_alert_events_username_not_empty
        CHECK (length(trim(username)) > 0)
);

CREATE INDEX IF NOT EXISTS idx_fraud_alert_events_alert
    ON fraud_alert_events (tenant_id, alert_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_fraud_alert_events_user_window
    ON fraud_alert_events (tenant_id, username, occurred_at DESC);
