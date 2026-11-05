BEGIN;

-- Índice GIN para búsqueda textual de normativa en español.
CREATE INDEX IF NOT EXISTS idx_rag_documents_content_fts_es
ON rag_documents
USING GIN (to_tsvector('spanish', content));

COMMIT;
