-- Individual failed-login events for deterministic fraud detection.
-- Manual migration: do not apply automatically during service startup.

CREATE TABLE IF NOT EXISTS fraud_failed_login_events (
    tenant_id TEXT NOT NULL,
    username TEXT NOT NULL,
    event_id UUID NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT pk_fraud_failed_login_events
        PRIMARY KEY (tenant_id, event_id),

    CONSTRAINT ck_fraud_failed_login_tenant_not_empty
        CHECK (length(trim(tenant_id)) > 0),

    CONSTRAINT ck_fraud_failed_login_username_not_empty
        CHECK (length(trim(username)) > 0)
);

-- Efficient event-time queries for each tenant/user.
CREATE INDEX IF NOT EXISTS idx_fraud_failed_login_events_window
    ON fraud_failed_login_events
       (tenant_id, username, occurred_at DESC);

-- Supports future retention analysis and cleanup operations.
CREATE INDEX IF NOT EXISTS idx_fraud_failed_login_events_received
    ON fraud_failed_login_events (received_at);
