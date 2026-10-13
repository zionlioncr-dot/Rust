-- ============================================================
-- Anti-Fraud alert idempotency
--
-- A Kafka event may be delivered more than once.
-- A tenant/event pair must therefore produce at most
-- one fraud alert.
-- ============================================================

CREATE UNIQUE INDEX IF NOT EXISTS ux_fraud_alerts_tenant_event_id
    ON fraud_alerts (tenant_id, event_id);