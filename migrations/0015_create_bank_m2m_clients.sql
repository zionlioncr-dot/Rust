-- OAuth 2.0 client credentials for bank servers.
-- The client secret is never stored in plaintext.

CREATE TABLE IF NOT EXISTS bank_m2m_clients (
    client_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    client_secret_hash BYTEA NOT NULL,
    scopes TEXT[] NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT ck_bank_m2m_secret_hash_length
        CHECK (octet_length(client_secret_hash) = 32),

    CONSTRAINT ck_bank_m2m_client_id_not_empty
        CHECK (length(trim(client_id)) > 0),

    CONSTRAINT ck_bank_m2m_tenant_id_not_empty
        CHECK (length(trim(tenant_id)) > 0),

    CONSTRAINT ck_bank_m2m_scopes_not_empty
        CHECK (cardinality(scopes) > 0)
);

CREATE INDEX IF NOT EXISTS idx_bank_m2m_clients_tenant
    ON bank_m2m_clients (tenant_id);

CREATE INDEX IF NOT EXISTS idx_bank_m2m_clients_enabled
    ON bank_m2m_clients (enabled);
