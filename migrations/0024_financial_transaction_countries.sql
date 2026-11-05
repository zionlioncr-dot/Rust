ALTER TABLE financial_transactions
    ADD COLUMN IF NOT EXISTS source_country TEXT,
    ADD COLUMN IF NOT EXISTS destination_country TEXT;

ALTER TABLE financial_transactions
    DROP CONSTRAINT IF EXISTS financial_transactions_source_country_format,
    DROP CONSTRAINT IF EXISTS financial_transactions_destination_country_format;

ALTER TABLE financial_transactions
    ADD CONSTRAINT financial_transactions_source_country_format
        CHECK (source_country IS NULL OR source_country ~ '^[A-Z]{2}$'),
    ADD CONSTRAINT financial_transactions_destination_country_format
        CHECK (destination_country IS NULL OR destination_country ~ '^[A-Z]{2}$');

CREATE INDEX IF NOT EXISTS idx_financial_transactions_tenant_source_country_time
    ON financial_transactions (tenant_id, source_country, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_financial_transactions_tenant_destination_country_time
    ON financial_transactions (tenant_id, destination_country, occurred_at DESC);
