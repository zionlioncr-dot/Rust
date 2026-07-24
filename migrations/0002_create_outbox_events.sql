CREATE TABLE IF NOT EXISTS outbox_events
(
    id UUID PRIMARY KEY,

    aggregate_type TEXT NOT NULL,

    aggregate_id UUID NOT NULL,

    event_type TEXT NOT NULL,

    payload JSONB NOT NULL,

    created_at TIMESTAMPTZ NOT NULL,

    published BOOLEAN NOT NULL DEFAULT FALSE
);