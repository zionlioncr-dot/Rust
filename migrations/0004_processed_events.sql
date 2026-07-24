CREATE TABLE IF NOT EXISTS processed_events
(
    event_id UUID PRIMARY KEY,

    consumer TEXT NOT NULL,

    handler TEXT NOT NULL,

    processed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_processed_consumer
ON processed_events(consumer);