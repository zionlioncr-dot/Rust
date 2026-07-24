CREATE TABLE IF NOT EXISTS audit_events
(
    id UUID PRIMARY KEY,

    username TEXT NOT NULL,

    action TEXT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL
);