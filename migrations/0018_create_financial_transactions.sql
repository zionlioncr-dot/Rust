CREATE TABLE IF NOT EXISTS financial_transactions (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    external_transaction_id TEXT,
    amount NUMERIC(38, 18) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL,
    source_account_id TEXT NOT NULL,
    destination_account_id TEXT NOT NULL,
    counterparty TEXT,
    country_code TEXT NOT NULL,
    channel TEXT NOT NULL,
    client_ip TEXT,
    occurred_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- AML queries commonly inspect transactions within a tenant and time window.
CREATE INDEX IF NOT EXISTS idx_financial_transactions_tenant_occurred
    ON financial_transactions (tenant_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_financial_transactions_tenant_source_time
    ON financial_transactions (tenant_id, source_account_id, occurred_at DESC);

CREATE INDEX IF NOT EXISTS idx_financial_transactions_tenant_destination_time
    ON financial_transactions (tenant_id, destination_account_id, occurred_at DESC);

-- Bank retries must not duplicate the same externally identified transaction.
CREATE UNIQUE INDEX IF NOT EXISTS uq_financial_transactions_tenant_external_id
    ON financial_transactions (tenant_id, external_transaction_id)
    WHERE external_transaction_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_financial_transactions_tenant_country_time
    ON financial_transactions (tenant_id, country_code, occurred_at DESC);
