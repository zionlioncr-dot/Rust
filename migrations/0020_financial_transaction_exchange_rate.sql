-- FX supplied by the originating bank in the transaction payload.
-- Historical rows remain NULL; new transactions require both fields in application validation.
ALTER TABLE financial_transactions
    ADD COLUMN IF NOT EXISTS exchange_rate NUMERIC(38, 18);

ALTER TABLE financial_transactions
    ADD COLUMN IF NOT EXISTS base_currency VARCHAR(3);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'financial_transactions_exchange_rate_positive'
          AND conrelid = 'financial_transactions'::regclass
    ) THEN
        ALTER TABLE financial_transactions
            ADD CONSTRAINT financial_transactions_exchange_rate_positive
            CHECK (exchange_rate IS NULL OR exchange_rate > 0);
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'financial_transactions_base_currency_format'
          AND conrelid = 'financial_transactions'::regclass
    ) THEN
        ALTER TABLE financial_transactions
            ADD CONSTRAINT financial_transactions_base_currency_format
            CHECK (
                base_currency IS NULL
                OR base_currency ~ '^[A-Z]{3}$'
            );
    END IF;
END
$$;
