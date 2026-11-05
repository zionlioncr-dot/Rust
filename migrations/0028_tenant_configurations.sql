BEGIN;

CREATE TABLE IF NOT EXISTS tenant_configurations (
    tenant_id TEXT PRIMARY KEY,
    time_zone TEXT NOT NULL,
    config_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT tenant_configurations_tenant_id_not_empty
        CHECK (length(btrim(tenant_id)) > 0),

    CONSTRAINT tenant_configurations_time_zone_not_empty
        CHECK (length(btrim(time_zone)) > 0),

    CONSTRAINT tenant_configurations_version_positive
        CHECK (config_version > 0)
);

COMMENT ON TABLE tenant_configurations IS
    'Configuración operacional independiente de los perfiles regulatorios AML.';

COMMENT ON COLUMN tenant_configurations.time_zone IS
    'Zona horaria IANA utilizada para calendarios operacionales del tenant.';

COMMENT ON COLUMN tenant_configurations.config_version IS
    'Versión de configuración para control de caché e invalidación.';

COMMIT;
