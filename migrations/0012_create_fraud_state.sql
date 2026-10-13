-- ============================================================
-- Anti-Fraud behavioral state
--
-- Maintains rolling failed-login counters per tenant/user.
-- The state is used by deterministic fraud rules.
-- ============================================================

CREATE TABLE IF NOT EXISTS fraud_failed_login_state
(
    tenant_id TEXT NOT NULL,

    username TEXT NOT NULL,

    window_start TIMESTAMPTZ NOT NULL,

    attempt_count INTEGER NOT NULL DEFAULT 0,

    updated_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (tenant_id, username),

    CONSTRAINT fraud_failed_login_state_attempt_count_check
        CHECK (attempt_count >= 0)
);

CREATE INDEX IF NOT EXISTS idx_fraud_failed_login_state_tenant_updated
    ON fraud_failed_login_state (tenant_id, updated_at DESC);