-- ============================================================
-- Bank IP allowlist
--
-- Authorized source networks for bank integrations.
--
-- source_ip is the real TCP peer address observed by the
-- API Gateway. Authorization is always tenant-scoped.
--
-- PostgreSQL inet supports both IPv4/IPv6 addresses and CIDRs.
-- ============================================================

CREATE TABLE IF NOT EXISTS bank_ip_allowlist
(
    id UUID PRIMARY KEY,

    tenant_id TEXT NOT NULL,

    network INET NOT NULL,

    description TEXT,

    enabled BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bank_ip_allowlist_tenant
    ON bank_ip_allowlist (tenant_id);

CREATE INDEX IF NOT EXISTS idx_bank_ip_allowlist_network
    ON bank_ip_allowlist
    USING GIST (network inet_ops);

CREATE UNIQUE INDEX IF NOT EXISTS ux_bank_ip_allowlist_tenant_network
    ON bank_ip_allowlist (tenant_id, network);