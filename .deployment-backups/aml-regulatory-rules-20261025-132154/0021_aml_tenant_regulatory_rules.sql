-- Perfil normativo versionado por entidad.
-- No se insertan perfiles ni monedas por defecto.
CREATE TABLE IF NOT EXISTS aml_tenant_regulatory_profiles (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    jurisdiction VARCHAR(16) NOT NULL,
    base_currency CHAR(3) NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT aml_regulatory_profile_tenant_nonempty
        CHECK (BTRIM(tenant_id) <> ''),
    CONSTRAINT aml_regulatory_profile_jurisdiction_nonempty
        CHECK (BTRIM(jurisdiction) <> ''),
    CONSTRAINT aml_regulatory_profile_currency_format
        CHECK (base_currency ~ '^[A-Z]{3}$'),
    CONSTRAINT aml_regulatory_profile_dates
        CHECK (effective_to IS NULL OR effective_to >= effective_from),
    CONSTRAINT aml_regulatory_profile_status
        CHECK (status IN ('pending', 'active', 'suspended'))
);

CREATE INDEX IF NOT EXISTS idx_aml_regulatory_profiles_tenant_dates
    ON aml_tenant_regulatory_profiles
       (tenant_id, effective_from DESC, effective_to);

CREATE UNIQUE INDEX IF NOT EXISTS uq_aml_regulatory_profile_active_tenant
    ON aml_tenant_regulatory_profiles (tenant_id)
    WHERE status = 'active';

-- Las reglas extraídas por Ollama comienzan pendientes.
-- Solo una regla aprobada, vigente y respaldada por evidencia
-- podrá utilizarse para evaluar transacciones.
CREATE TABLE IF NOT EXISTS aml_regulatory_rules (
    id UUID PRIMARY KEY,
    profile_id UUID NOT NULL
        REFERENCES aml_tenant_regulatory_profiles(id),
    tenant_id TEXT NOT NULL,
    rule_code TEXT NOT NULL,
    threshold_amount NUMERIC(38, 18) NOT NULL,
    threshold_currency CHAR(3) NOT NULL,
    minimum_transactions INTEGER,
    regulation_code TEXT NOT NULL,
    article TEXT NOT NULL,
    document_version TEXT NOT NULL,
    source_document_id UUID,
    source_url TEXT NOT NULL,
    source_excerpt TEXT NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE,
    status TEXT NOT NULL DEFAULT 'pending',
    extraction_model TEXT,
    extraction_payload JSONB,
    approved_by TEXT,
    approved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT aml_regulatory_rule_tenant_nonempty
        CHECK (BTRIM(tenant_id) <> ''),
    CONSTRAINT aml_regulatory_rule_code
        CHECK (rule_code IN (
            'high_value_transaction',
            'structuring_activity'
        )),
    CONSTRAINT aml_regulatory_rule_threshold_positive
        CHECK (threshold_amount > 0),
    CONSTRAINT aml_regulatory_rule_currency_format
        CHECK (threshold_currency ~ '^[A-Z]{3}$'),
    CONSTRAINT aml_regulatory_rule_structuring_count
        CHECK (
            (rule_code = 'structuring_activity'
                AND minimum_transactions >= 2)
            OR
            (rule_code = 'high_value_transaction'
                AND minimum_transactions IS NULL)
        ),
    CONSTRAINT aml_regulatory_rule_legal_basis
        CHECK (
            BTRIM(regulation_code) <> ''
            AND BTRIM(article) <> ''
            AND BTRIM(document_version) <> ''
            AND BTRIM(source_excerpt) <> ''
            AND source_url LIKE 'https://%'
        ),
    CONSTRAINT aml_regulatory_rule_dates
        CHECK (effective_to IS NULL OR effective_to >= effective_from),
    CONSTRAINT aml_regulatory_rule_status
        CHECK (status IN ('pending', 'approved', 'rejected', 'superseded')),
    CONSTRAINT aml_regulatory_rule_approval
        CHECK (
            status <> 'approved'
            OR (
                NULLIF(BTRIM(approved_by), '') IS NOT NULL
                AND approved_at IS NOT NULL
            )
        )
);

CREATE INDEX IF NOT EXISTS idx_aml_regulatory_rules_tenant_lookup
    ON aml_regulatory_rules
       (tenant_id, rule_code, status, effective_from DESC);

CREATE INDEX IF NOT EXISTS idx_aml_regulatory_rules_profile
    ON aml_regulatory_rules (profile_id, status);

COMMENT ON TABLE aml_tenant_regulatory_profiles IS
    'Versioned tenant jurisdiction and base-currency profiles; no implicit defaults.';

COMMENT ON TABLE aml_regulatory_rules IS
    'Tenant-scoped AML rule candidates with legal provenance and explicit approval.';
