BEGIN;

CREATE TABLE IF NOT EXISTS aml_assessments (
    assessment_id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    transaction_id UUID NOT NULL,
    model TEXT NOT NULL,
    jurisdiction TEXT NOT NULL,
    suspicious BOOLEAN NOT NULL,
    severity TEXT NOT NULL
        CHECK (severity IN ('low', 'medium', 'high', 'critical')),
    category TEXT NOT NULL,
    description TEXT NOT NULL,
    rationale TEXT NOT NULL,
    confidence DOUBLE PRECISION
        CHECK (confidence IS NULL OR (confidence >= 0 AND confidence <= 1)),
    evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
    regulatory_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
    limitations JSONB NOT NULL DEFAULT '[]'::jsonb,
    transaction_data JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_aml_assessments_tenant_event UNIQUE (tenant_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_aml_assessments_tenant_created
    ON aml_assessments (tenant_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_aml_assessments_tenant_suspicious_created
    ON aml_assessments (tenant_id, suspicious, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_aml_assessments_tenant_transaction
    ON aml_assessments (tenant_id, transaction_id);

COMMIT;
