CREATE INDEX idx_outbox_published
ON outbox_events(published);

CREATE INDEX idx_outbox_created
ON outbox_events(created_at);

CREATE INDEX idx_audit_created
ON audit_events(created_at);