BEGIN;

ALTER TABLE aml_regulatory_rules
    ADD COLUMN deadline_days INTEGER;

ALTER TABLE aml_regulatory_rules
    ALTER COLUMN threshold_amount DROP NOT NULL,
    ALTER COLUMN threshold_currency DROP NOT NULL,
    ALTER COLUMN effective_from DROP NOT NULL;

ALTER TABLE aml_regulatory_rules
    DROP CONSTRAINT aml_regulatory_rule_code,
    DROP CONSTRAINT aml_regulatory_rule_threshold_positive,
    DROP CONSTRAINT aml_regulatory_rule_currency_format,
    DROP CONSTRAINT aml_regulatory_rule_structuring_count,
    DROP CONSTRAINT aml_regulatory_rule_dates,
    DROP CONSTRAINT aml_regulatory_rule_approval;

ALTER TABLE aml_regulatory_rules
    ADD CONSTRAINT aml_regulatory_rule_code
    CHECK (rule_code IN (
        'high_value_transaction',
        'structuring_activity',
        'monthly_aggregate_transaction',
        'regulatory_reporting_deadline'
    ));

ALTER TABLE aml_regulatory_rules
    ADD CONSTRAINT aml_regulatory_rule_threshold_positive
    CHECK (threshold_amount IS NULL OR threshold_amount > 0);

ALTER TABLE aml_regulatory_rules
    ADD CONSTRAINT aml_regulatory_rule_currency_format
    CHECK (
        threshold_currency IS NULL
        OR threshold_currency ~ '^[A-Z]{3}$'
    );

ALTER TABLE aml_regulatory_rules
    ADD CONSTRAINT aml_regulatory_rule_parameters
    CHECK (
        (
            rule_code = 'high_value_transaction'
            AND threshold_amount IS NOT NULL
            AND threshold_currency IS NOT NULL
            AND minimum_transactions IS NULL
            AND deadline_days IS NULL
        )
        OR
        (
            rule_code = 'structuring_activity'
            AND threshold_amount IS NOT NULL
            AND threshold_currency IS NOT NULL
            AND minimum_transactions >= 2
            AND deadline_days IS NULL
        )
        OR
        (
            rule_code = 'monthly_aggregate_transaction'
            AND threshold_amount IS NOT NULL
            AND threshold_currency IS NOT NULL
            AND minimum_transactions IS NULL
            AND deadline_days IS NULL
        )
        OR
        (
            rule_code = 'regulatory_reporting_deadline'
            AND threshold_amount IS NULL
            AND threshold_currency IS NULL
            AND minimum_transactions IS NULL
            AND deadline_days > 0
        )
    );

ALTER TABLE aml_regulatory_rules
    ADD CONSTRAINT aml_regulatory_rule_dates
    CHECK (
        effective_from IS NULL
        OR effective_to IS NULL
        OR effective_to >= effective_from
    );

ALTER TABLE aml_regulatory_rules
    ADD CONSTRAINT aml_regulatory_rule_approval
    CHECK (
        status <> 'approved'
        OR (
            NULLIF(BTRIM(approved_by), '') IS NOT NULL
            AND approved_at IS NOT NULL
            AND effective_from IS NOT NULL
        )
    );

COMMENT ON COLUMN aml_regulatory_rules.deadline_days IS
    'Plazo en días naturales para reglas de cumplimiento; no es un umbral monetario.';

COMMIT;
