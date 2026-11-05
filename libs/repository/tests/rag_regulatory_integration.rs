use chrono::NaiveDate;
use repository::RagRepository;
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, query, query_scalar};
use uuid::Uuid;

fn vector_string(values: &[f32]) -> String {
    format!(
        "[{}]",
        values.iter()
            .map(|value| value.to_string())
            .collect::<Vec<_>>()
            .join(",")
    )
}

#[tokio::test]
#[ignore = "requiere TEST_DATABASE_URL apuntando exclusivamente a financial_test"]
async fn regulatory_search_combines_vector_and_spanish_lexical_results() -> anyhow::Result<()> {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .expect("Define TEST_DATABASE_URL apuntando a financial_test");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url)
        .await?;

    // Protección obligatoria: no escribir si la conexión no es financial_test.
    let database: String = query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;

    assert_eq!(
        database,
        "financial_test",
        "La prueba solo puede ejecutarse contra financial_test"
    );

    let repository = RagRepository::new(pool.clone());
    let as_of = NaiveDate::from_ymd_opt(2026, 11, 2).unwrap();

    let lexical_id = Uuid::new_v4();
    let vector_id = Uuid::new_v4();
    let unverified_id = Uuid::new_v4();
    let wrong_jurisdiction_id = Uuid::new_v4();
    let expired_id = Uuid::new_v4();

    let mut query_embedding = vec![0.0_f32; 768];
    query_embedding[0] = 1.0;

    let mut lexical_embedding = vec![0.0_f32; 768];
    lexical_embedding[1] = 1.0;

    let fixtures = vec![
        (
            lexical_id,
            "fixture-lexical",
            "La estructuración financiera sospechosa requiere monitoreo regulatorio.",
            json!({
                "document_type": "regulation",
                "jurisdiction": "CR",
                "verified": true,
                "regulation_code": "TEST-001",
                "article": "1",
                "source_url": "https://example.org/test-001",
                "effective_from": "2025-01-01",
                "effective_to": null
            }),
            vector_string(&lexical_embedding),
        ),
        (
            vector_id,
            "fixture-vector",
            "Disposición sobre conservación documental y controles internos.",
            json!({
                "document_type": "regulation",
                "jurisdiction": "CR",
                "verified": true,
                "regulation_code": "TEST-002",
                "article": "2",
                "source_url": "https://example.org/test-002",
                "effective_from": "2025-01-01",
                "effective_to": null
            }),
            vector_string(&query_embedding),
        ),
        (
            unverified_id,
            "fixture-unverified",
            "La estructuración financiera sospechosa requiere monitoreo.",
            json!({
                "document_type": "regulation",
                "jurisdiction": "CR",
                "verified": false,
                "regulation_code": "TEST-003",
                "article": "3",
                "source_url": "https://example.org/test-003",
                "effective_from": "2025-01-01",
                "effective_to": null
            }),
            vector_string(&query_embedding),
        ),
        (
            wrong_jurisdiction_id,
            "fixture-wrong-jurisdiction",
            "La estructuración financiera sospechosa requiere monitoreo.",
            json!({
                "document_type": "regulation",
                "jurisdiction": "PA",
                "verified": true,
                "regulation_code": "TEST-004",
                "article": "4",
                "source_url": "https://example.org/test-004",
                "effective_from": "2025-01-01",
                "effective_to": null
            }),
            vector_string(&query_embedding),
        ),
        (
            expired_id,
            "fixture-expired",
            "La estructuración financiera sospechosa requiere monitoreo.",
            json!({
                "document_type": "regulation",
                "jurisdiction": "CR",
                "verified": true,
                "regulation_code": "TEST-005",
                "article": "5",
                "source_url": "https://example.org/test-005",
                "effective_from": "2020-01-01",
                "effective_to": "2025-12-31"
            }),
            vector_string(&query_embedding),
        ),
    ];

    let fixture_ids = vec![
        lexical_id,
        vector_id,
        unverified_id,
        wrong_jurisdiction_id,
        expired_id,
    ];

    // Insertar únicamente fixtures sintéticos en financial_test.
    let setup_result: anyhow::Result<()> = async {
        for (id, source, content, metadata, embedding) in &fixtures {
            query(
                r#"
                INSERT INTO rag_documents (id, source, content, metadata, embedding)
                VALUES ($1, $2, $3, $4, $5::vector)
                "#,
            )
            .bind(id)
            .bind(source)
            .bind(content)
            .bind(metadata)
            .bind(embedding)
            .execute(&pool)
            .await?;
        }
        Ok(())
    }
    .await;

    if let Err(error) = setup_result {
        query("DELETE FROM rag_documents WHERE id = ANY($1)")
            .bind(&fixture_ids)
            .execute(&pool)
            .await?;
        return Err(error);
    }

    let search_result = repository
        .search_regulatory(
            &query_embedding,
            "estructuración financiera sospechosa",
            "CR",
            as_of,
            5,
        )
        .await;

    // Limpiar antes de evaluar assertions, incluso si la búsqueda falla.
    query("DELETE FROM rag_documents WHERE id = ANY($1)")
        .bind(&fixture_ids)
        .execute(&pool)
        .await?;

    let results = search_result?;

    assert_eq!(
        results.len(),
        2,
        "Solo deben aparecer los dos documentos regulatorios elegibles"
    );

    assert!(
        results.iter().any(|result| result.id == lexical_id),
        "Debe recuperarse el documento por coincidencia léxica española"
    );

    assert!(
        results.iter().any(|result| result.id == vector_id),
        "Debe recuperarse el documento por similitud vectorial"
    );

    assert_eq!(
        results[0].id,
        lexical_id,
        "RRF debe favorecer el documento que aparece en ambos rankings"
    );

    assert!(
        !results.iter().any(|result| result.id == unverified_id),
        "No debe recuperarse normativa no verificada"
    );

    assert!(
        !results.iter().any(|result| result.id == wrong_jurisdiction_id),
        "No debe recuperarse normativa de otra jurisdicción"
    );

    assert!(
        !results.iter().any(|result| result.id == expired_id),
        "No debe recuperarse normativa vencida"
    );

    Ok(())
}
