use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagDocument {
    pub id: Uuid,
    pub source: String,
    pub content: String,
    pub metadata: Value,
    pub embedding: Vec<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagSearchResult {
    pub id: Uuid,
    pub source: String,
    pub content: String,
    pub metadata: Value,
    pub similarity: f64,
}

#[derive(Clone)]
pub struct RagRepository {
    pool: PgPool,
}

impl RagRepository {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert_document(&self, document: &RagDocument) -> Result<()> {
        if document.embedding.len() != 768 {
            bail!(
                "invalid embedding dimension: expected 768, got {}",
                document.embedding.len()
            );
        }

        let embedding = format_vector(&document.embedding);

        sqlx::query(
            r#"
            INSERT INTO rag_documents
                (id, source, content, metadata, embedding)
            VALUES
                ($1, $2, $3, $4, $5::vector)
            ON CONFLICT (id)
            DO UPDATE SET
                source = EXCLUDED.source,
                content = EXCLUDED.content,
                metadata = EXCLUDED.metadata,
                embedding = EXCLUDED.embedding
            "#,
        )
        .bind(document.id)
        .bind(&document.source)
        .bind(&document.content)
        .bind(&document.metadata)
        .bind(embedding)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Pure semantic search using pgvector cosine similarity.
    pub async fn search_similar(
        &self,
        embedding: &[f32],
        limit: i64,
    ) -> Result<Vec<RagSearchResult>> {
        if embedding.len() != 768 {
            bail!(
                "invalid embedding dimension: expected 768, got {}",
                embedding.len()
            );
        }

        let limit = limit.clamp(1, 20);
        let embedding = format_vector(embedding);

        let results = sqlx::query_as::<_, RagSearchRow>(
            r#"
            SELECT
                id,
                source,
                content,
                metadata,
                1 - (embedding <=> $1::vector) AS similarity
            FROM rag_documents
            ORDER BY embedding <=> $1::vector
            LIMIT $2
            "#,
        )
        .bind(embedding)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(results.into_iter().map(RagSearchResult::from).collect())
    }

    /// Hybrid RAG retrieval.
    ///
    /// First retrieves the semantic candidate set from pgvector.
    /// Then reranks the candidates using:
    ///
    /// - semantic similarity
    /// - source/path relevance
    /// - technical identifier relevance
    /// - content relevance
    /// - metadata relevance
    /// - source diversity
    pub async fn search_hybrid(
        &self,
        embedding: &[f32],
        query: &str,
        limit: i64,
    ) -> Result<Vec<RagSearchResult>> {
        if embedding.len() != 768 {
            bail!(
                "invalid embedding dimension: expected 768, got {}",
                embedding.len()
            );
        }

        let query = query.trim();

        if query.is_empty() {
            return self.search_similar(embedding, limit).await;
        }

        let limit = limit.clamp(1, 20);

        /*
         * The current corpus contains only 65 documents.
         *
         * Searching the complete corpus avoids losing highly relevant
         * technical files before the lexical reranker gets a chance
         * to inspect them.
         */
        let candidate_limit = 65_i64;

        let embedding_string = format_vector(embedding);

        let candidates = sqlx::query_as::<_, RagSearchRow>(
            r#"
            SELECT
                id,
                source,
                content,
                metadata,
                1 - (embedding <=> $1::vector) AS similarity
            FROM rag_documents
            ORDER BY embedding <=> $1::vector
            LIMIT $2
            "#,
        )
        .bind(embedding_string)
        .bind(candidate_limit)
        .fetch_all(&self.pool)
        .await?;

        let query_tokens = tokenize(query);

        if query_tokens.is_empty() {
            return Ok(candidates
                .into_iter()
                .take(limit as usize)
                .map(RagSearchResult::from)
                .collect());
        }

        let mut ranked = candidates
            .into_iter()
            .map(|row| {
                let lexical = lexical_score(&row, &query_tokens);

                let source_quality = source_quality_score(&row.source);

                /*
                 * Semantic similarity remains important, but explicit
                 * technical identifiers and file paths get stronger
                 * signals than generic README text.
                 */
                let score = (row.similarity * 0.50) + (lexical * 0.42) + (source_quality * 0.08);

                RankedCandidate { row, score }
            })
            .collect::<Vec<_>>();

        ranked.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let selected = diversify_results(ranked, limit as usize);

        Ok(selected
            .into_iter()
            .map(|candidate| RagSearchResult::from(candidate.row))
            .collect())
    }

    pub async fn count(&self) -> Result<i64> {
        let row = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM rag_documents
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(row)
    }
}

#[derive(Debug, sqlx::FromRow)]
struct RagSearchRow {
    id: Uuid,
    source: String,
    content: String,
    metadata: Value,
    similarity: f64,
}

impl From<RagSearchRow> for RagSearchResult {
    fn from(row: RagSearchRow) -> Self {
        Self {
            id: row.id,
            source: row.source,
            content: row.content,
            metadata: row.metadata,
            similarity: row.similarity,
        }
    }
}

#[derive(Debug)]
struct RankedCandidate {
    row: RagSearchRow,
    score: f64,
}

/// Tokenizes natural-language and technical queries.
///
/// Important technical identifiers such as:
///
/// - audit-consumer
/// - audit-service
/// - outbox-worker
/// - AuditCreated
/// - EventEnvelope
/// - AuditHandler
///
/// are preserved as compound tokens and also decomposed into individual
/// identifiers.
fn tokenize(input: &str) -> Vec<String> {
    let stop_words = [
        "a", "al", "and", "are", "as", "at", "con", "de", "del", "desde", "el", "en", "es", "for",
        "from", "how", "in", "is", "la", "las", "los", "of", "on", "para", "que", "se", "the",
        "to", "un", "una", "using", "y",
    ];

    let normalized = input.to_lowercase();

    let mut tokens = Vec::new();

    /*
     * First preserve compound identifiers.
     *
     * Example:
     *
     * audit-consumer
     * outbox-worker
     */
    for raw in normalized.split_whitespace() {
        let cleaned = raw.trim_matches(|c: char| !c.is_alphanumeric() && c != '-' && c != '_');

        if cleaned.contains('-') || cleaned.contains('_') {
            let compound = cleaned.to_string();

            if compound.len() >= 3
                && !stop_words.contains(&compound.as_str())
                && !tokens.contains(&compound)
            {
                tokens.push(compound);
            }
        }
    }

    /*
     * Then add individual identifiers.
     *
     * Example:
     *
     * audit-consumer
     *
     * becomes:
     *
     * audit
     * consumer
     */
    for raw in normalized.split(|c: char| !c.is_alphanumeric() && c != '_') {
        let token = raw.trim().to_string();

        if token.len() < 3 {
            continue;
        }

        if stop_words.contains(&token.as_str()) {
            continue;
        }

        if !tokens.contains(&token) {
            tokens.push(token);
        }
    }

    tokens
}

/// Calculates lexical relevance for a RAG document.
fn lexical_score(row: &RagSearchRow, query_tokens: &[String]) -> f64 {
    if query_tokens.is_empty() {
        return 0.0;
    }

    let content = normalize_identifier_text(&row.content);

    let source = normalize_identifier_text(&row.source);

    let path = row
        .metadata
        .get("path")
        .and_then(Value::as_str)
        .unwrap_or("");

    let path = normalize_identifier_text(path);

    let topic = row
        .metadata
        .get("topic")
        .and_then(Value::as_str)
        .unwrap_or("");

    let topic = normalize_identifier_text(topic);

    let component = row
        .metadata
        .get("component")
        .and_then(Value::as_str)
        .unwrap_or("");

    let component = normalize_identifier_text(component);

    let mut content_hits = 0.0_f64;
    let mut path_hits = 0.0_f64;
    let mut metadata_hits = 0.0_f64;

    for token in query_tokens {
        let token = normalize_identifier_text(token);

        if token.len() < 3 {
            continue;
        }

        if contains_identifier(&content, &token) {
            content_hits += 1.0;
        }

        if contains_identifier(&path, &token) {
            path_hits += 1.0;
        }

        if contains_identifier(&source, &token) {
            metadata_hits += 1.0;
        }

        if contains_identifier(&topic, &token) {
            metadata_hits += 0.5;
        }

        if contains_identifier(&component, &token) {
            metadata_hits += 0.5;
        }
    }

    let token_count = query_tokens.len() as f64;

    let content_signal = (content_hits / token_count).min(1.0);

    let path_signal = (path_hits / token_count).min(1.0);

    let metadata_signal = (metadata_hits / token_count).min(1.0);

    /*
     * Explicit technical identifier/path matching.
     *
     * This is especially important for queries containing:
     *
     * audit-consumer
     * audit-service
     * outbox-worker
     * AuditHandler
     * AuditCreated
     * EventEnvelope
     */
    let identifier_signal = identifier_path_signal(&path, query_tokens);

    /*
     * Path relevance receives the strongest lexical signal.
     *
     * Example:
     *
     * query:
     *   audit-consumer ... AuditHandler
     *
     * document:
     *   apps/audit-consumer/src/handler/audit_handler.rs
     *
     * should receive a substantially stronger score than README.md.
     */
    let score = (path_signal * 0.35)
        + (identifier_signal * 0.40)
        + (content_signal * 0.20)
        + (metadata_signal * 0.05);

    score.min(1.0)
}

/// Normalizes technical identifiers.
///
/// Example:
///
/// audit-consumer
///      ↓
/// audit consumer
///
/// audit_handler.rs
///      ↓
/// audit handler rs
fn normalize_identifier_text(input: &str) -> String {
    input.to_lowercase().replace(['-', '_', '/', '.', ':'], " ")
}

/// Determines whether a normalized identifier occurs in text.
fn contains_identifier(text: &str, token: &str) -> bool {
    let normalized_text = normalize_identifier_text(text);

    let normalized_token = normalize_identifier_text(token);

    /*
     * Exact token match.
     */
    if normalized_text
        .split_whitespace()
        .any(|part| part == normalized_token)
    {
        return true;
    }

    /*
     * Compound identifier match.
     *
     * Example:
     *
     * normalized path:
     *
     * apps audit consumer src handler audit handler rs
     *
     * token:
     *
     * audit consumer
     */
    let compact_text = normalized_text.replace(' ', "");

    let compact_token = normalized_token.replace(' ', "");

    compact_text.contains(&compact_token)
}

/// Gives a strong signal when a query explicitly names a technical
/// component/file/concept and that identifier occurs in the document path.
fn identifier_path_signal(path: &str, query_tokens: &[String]) -> f64 {
    let compact_path = normalize_identifier_text(path).replace(' ', "");

    let important_identifiers = [
        "auditconsumer",
        "auditservice",
        "outboxworker",
        "audithandler",
        "eventdispatcher",
        "eventenvelope",
        "auditcreated",
        "schemaregistry",
        "kafka",
        "redpanda",
    ];

    let mut matched: f64 = 0.0;
    let mut requested: f64 = 0.0;

    for identifier in important_identifiers {
        let requested_identifier = query_tokens.iter().any(|token| {
            let normalized = normalize_identifier_text(token).replace(' ', "");

            normalized == identifier
        });

        if !requested_identifier {
            continue;
        }

        requested += 1.0;

        if compact_path.contains(identifier) {
            matched += 1.0;
        }
    }

    if requested == 0.0 {
        return 0.0;
    }

    (matched / requested).min(1.0)
}

/// README is useful as general documentation, but source files should
/// receive a small quality advantage when the query is asking about
/// implementation details.
fn source_quality_score(source: &str) -> f64 {
    if source.eq_ignore_ascii_case("README.md") {
        0.0
    } else {
        1.0
    }
}

/// Prevents the final result set from being dominated by one source.
///
/// This is particularly important for README.md because it contains
/// broad descriptions of many platform components.
fn diversify_results(mut candidates: Vec<RankedCandidate>, limit: usize) -> Vec<RankedCandidate> {
    let mut selected = Vec::with_capacity(limit);

    let mut source_counts = std::collections::HashMap::<String, usize>::new();

    while selected.len() < limit && !candidates.is_empty() {
        let mut best_index = 0usize;

        let mut best_score = f64::NEG_INFINITY;

        for (index, candidate) in candidates.iter().enumerate() {
            let source_count = source_counts
                .get(&candidate.row.source)
                .copied()
                .unwrap_or(0);

            let diversity_penalty = match source_count {
                0 => 0.0,
                1 => 0.10,
                2 => 0.20,
                _ => 0.30,
            };

            /*
             * Once one README chunk has been selected,
             * subsequent README chunks receive an additional penalty.
             */
            let readme_penalty = if candidate.row.source == "README.md" && source_count > 0 {
                0.12
            } else {
                0.0
            };

            let adjusted_score = candidate.score - diversity_penalty - readme_penalty;

            if adjusted_score > best_score {
                best_score = adjusted_score;
                best_index = index;
            }
        }

        let candidate = candidates.remove(best_index);

        *source_counts
            .entry(candidate.row.source.clone())
            .or_insert(0) += 1;

        selected.push(candidate);
    }

    selected
}

fn format_vector(values: &[f32]) -> String {
    let values = values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>();

    format!("[{}]", values.join(","))
}
