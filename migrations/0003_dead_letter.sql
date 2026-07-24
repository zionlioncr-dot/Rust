CREATE TABLE IF NOT EXISTS dead_letter_events
(
    id UUID PRIMARY KEY,

    event_id UUID NOT NULL,

    event_type TEXT NOT NULL,

    payload JSONB NOT NULL,

    error TEXT NOT NULL,

    attempts INTEGER NOT NULL,

    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_dead_letter_event_id
ON dead_letter_events(event_id);

CREATE INDEX idx_dead_letter_type
ON dead_letter_events(event_type);