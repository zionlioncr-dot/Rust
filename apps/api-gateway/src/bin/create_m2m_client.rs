use anyhow::{bail, Context, Result};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

const ALLOWED_SCOPES: &[&str] = &[
    "audit:read",
    "audit:write",
    "fraud:read",
    "fraud:write",
    "transactions:read",
    "transactions:write",
];

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let args: Vec<String> = std::env::args().collect();

    if args.len() != 4 {
        bail!(
            "Usage: cargo run -p api-gateway --bin create_m2m_client -- \
             <client_id> <tenant_id> <scope1,scope2>"
        );
    }

    let client_id = args[1].trim();
    let tenant_id = args[2].trim();

    if client_id.is_empty() || tenant_id.is_empty() {
        bail!("client_id and tenant_id cannot be empty");
    }

    let mut scopes: Vec<String> = args[3]
        .split(',')
        .map(str::trim)
        .filter(|scope| !scope.is_empty())
        .map(str::to_string)
        .collect();

    scopes.sort();
    scopes.dedup();

    if scopes.is_empty() {
        bail!("At least one scope is required");
    }

    for scope in &scopes {
        if !ALLOWED_SCOPES.contains(&scope.as_str()) {
            bail!("Unsupported scope: {scope}");
        }
    }

    let database_url = std::env::var("DATABASE_URL").context("DATABASE_URL must be configured")?;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&database_url)
        .await
        .context("Could not connect to PostgreSQL")?;

    // Two UUIDv4 values provide a high-entropy, one-time client secret.
    let client_secret = format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple());
    let secret_hash = Sha256::digest(client_secret.as_bytes()).to_vec();

    sqlx::query(
        r#"
        INSERT INTO bank_m2m_clients
            (client_id, tenant_id, client_secret_hash, scopes)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(client_id)
    .bind(tenant_id)
    .bind(secret_hash)
    .bind(&scopes)
    .execute(&pool)
    .await
    .context("Could not create M2M client; client_id may already exist")?;

    println!("M2M client created.");
    println!("client_id: {client_id}");
    println!("tenant_id: {tenant_id}");
    println!("scopes: {}", scopes.join(" "));
    println!("client_secret (save now; it will not be shown again):");
    println!("{client_secret}");

    Ok(())
}
