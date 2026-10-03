CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS rag_documents
(
    id UUID PRIMARY KEY,

    source TEXT NOT NULL,

    content TEXT NOT NULL,

    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,

    embedding vector(768) NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rag_documents_source
ON rag_documents(source);

CREATE INDEX IF NOT EXISTS idx_rag_documents_created_at
ON rag_documents(created_at);
