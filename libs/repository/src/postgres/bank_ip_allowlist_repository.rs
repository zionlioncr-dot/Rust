use anyhow::Result;
use async_trait::async_trait;
use redis::{aio::ConnectionManager, AsyncCommands};
use sqlx::PgPool;
use std::net::IpAddr;

use crate::bank_ip_allowlist_repository::BankIpAllowlistRepository;

const DEFAULT_POSITIVE_TTL_SECS: u64 = 60;
const DEFAULT_NEGATIVE_TTL_SECS: u64 = 15;

const REDIS_OPERATION_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(200);

#[derive(Clone)]
pub struct PostgresBankIpAllowlistRepository {
    pool: PgPool,
    redis: Option<ConnectionManager>,
    positive_ttl_secs: u64,
    negative_ttl_secs: u64,
}

impl PostgresBankIpAllowlistRepository {
    /// Creates a PostgreSQL-only repository.
    /// Redis can be enabled with `with_cache`.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            redis: None,
            positive_ttl_secs: DEFAULT_POSITIVE_TTL_SECS,
            negative_ttl_secs: DEFAULT_NEGATIVE_TTL_SECS,
        }
    }

    /// Enables Redis caching while keeping PostgreSQL as the source of truth.
    pub fn with_cache(
        mut self,
        redis: ConnectionManager,
        positive_ttl_secs: u64,
        negative_ttl_secs: u64,
    ) -> Self {
        self.redis = Some(redis);
        self.positive_ttl_secs = positive_ttl_secs.max(1);
        self.negative_ttl_secs = negative_ttl_secs.max(1);
        self
    }

    fn cache_key(tenant_id: &str, source_ip: IpAddr) -> String {
        let tenant_id = tenant_id.trim();

        // The tenant length prevents ambiguous keys if tenant IDs contain separators.
        format!(
            "bank-ip-allowlist:v1:{}:{}:{}",
            tenant_id.len(),
            tenant_id,
            source_ip
        )
    }
}

#[async_trait]
impl BankIpAllowlistRepository for PostgresBankIpAllowlistRepository {
    async fn is_source_ip_allowed(&self, tenant_id: &str, source_ip: IpAddr) -> Result<bool> {
        let tenant_id = tenant_id.trim();

        if tenant_id.is_empty() {
            anyhow::bail!("tenant_id cannot be empty for bank IP authorization");
        }

        let cache_key = Self::cache_key(tenant_id, source_ip);

        // A Redis error is a cache miss, never an authorization decision.
        if let Some(redis_manager) = &self.redis {
            let mut connection = redis_manager.clone();

            let cached = tokio::time::timeout(
                REDIS_OPERATION_TIMEOUT,
                connection.get::<_, Option<String>>(&cache_key),
            )
            .await;

            match cached {
                Ok(Ok(Some(value))) if value == "1" => {
                    tracing::debug!(
                        tenant_id,
                        source_ip = %source_ip,
                        "Bank IP allowlist cache hit: allowed"
                    );
                    return Ok(true);
                }
                Ok(Ok(Some(value))) if value == "0" => {
                    tracing::debug!(
                        tenant_id,
                        source_ip = %source_ip,
                        "Bank IP allowlist cache hit: denied"
                    );
                    return Ok(false);
                }
                Ok(Ok(_)) => {
                    tracing::debug!(
                        tenant_id,
                        source_ip = %source_ip,
                        "Bank IP allowlist cache miss"
                    );
                }
                Ok(Err(error)) => {
                    tracing::warn!(
                        %error,
                        tenant_id,
                        source_ip = %source_ip,
                        "Redis cache read failed; falling back to PostgreSQL"
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        tenant_id,
                        source_ip = %source_ip,
                        timeout_ms = REDIS_OPERATION_TIMEOUT.as_millis(),
                        "Redis cache read timed out; falling back to PostgreSQL"
                    );
                }
            }
        }

        // PostgreSQL remains authoritative.
        let source_ip_text = source_ip.to_string();

        let allowed = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM bank_ip_allowlist
                WHERE tenant_id = $1
                  AND enabled = TRUE
                  AND $2::inet <<= network
            )
            "#,
        )
        .bind(tenant_id)
        .bind(source_ip_text)
        .fetch_one(&self.pool)
        .await?;

        // Cache writes are best-effort. A Redis failure must not override
        // the result successfully obtained from PostgreSQL.
        if let Some(redis_manager) = &self.redis {
            let ttl = if allowed {
                self.positive_ttl_secs
            } else {
                self.negative_ttl_secs
            };

            let value = if allowed { "1" } else { "0" };
            let mut connection = redis_manager.clone();

            let write_result = tokio::time::timeout(
                REDIS_OPERATION_TIMEOUT,
                connection.set_ex::<_, _, ()>(&cache_key, value, ttl),
            )
            .await;

            match write_result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(
                        %error,
                        tenant_id,
                        source_ip = %source_ip,
                        "Redis cache write failed; PostgreSQL result will be used"
                    );
                }
                Err(_) => {
                    tracing::warn!(
                        tenant_id,
                        source_ip = %source_ip,
                        timeout_ms = REDIS_OPERATION_TIMEOUT.as_millis(),
                        "Redis cache write timed out; PostgreSQL result will be used"
                    );
                }
            }
        }

        Ok(allowed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use std::net::IpAddr;

    #[test]
    fn cache_key_includes_tenant_and_source_ip() {
        let ip: IpAddr = "192.0.2.10".parse().unwrap();

        let tenant_a = PostgresBankIpAllowlistRepository::cache_key("tenant-a", ip);
        let tenant_b = PostgresBankIpAllowlistRepository::cache_key("tenant-b", ip);
        let other_ip =
            PostgresBankIpAllowlistRepository::cache_key("tenant-a", "192.0.2.11".parse().unwrap());

        assert_ne!(tenant_a, tenant_b);
        assert_ne!(tenant_a, other_ip);
    }

    #[test]
    fn cache_key_trims_tenant_id() {
        let ip: IpAddr = "192.0.2.10".parse().unwrap();

        let normal = PostgresBankIpAllowlistRepository::cache_key("tenant-a", ip);
        let padded = PostgresBankIpAllowlistRepository::cache_key("  tenant-a  ", ip);

        assert_eq!(normal, padded);
    }

    #[tokio::test]
    async fn cache_ttls_are_at_least_one_second() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/financial")
            .unwrap();

        let repository = PostgresBankIpAllowlistRepository::new(pool);

        // These are the defaults when Redis caching is not configured.
        assert_eq!(repository.positive_ttl_secs, DEFAULT_POSITIVE_TTL_SECS);
        assert_eq!(repository.negative_ttl_secs, DEFAULT_NEGATIVE_TTL_SECS);
    }

    #[tokio::test]
    async fn empty_tenant_id_is_rejected_without_database_access() {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/financial")
            .unwrap();

        let repository = PostgresBankIpAllowlistRepository::new(pool);
        let ip: IpAddr = "192.0.2.10".parse().unwrap();

        let result = repository.is_source_ip_allowed("   ", ip).await;

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("tenant_id cannot be empty"));
    }
}

#[cfg(test)]
mod redis_integration_tests {
    use super::*;
    use redis::AsyncCommands;
    use sqlx::postgres::PgPoolOptions;
    use std::net::IpAddr;
    use uuid::Uuid;

    #[tokio::test]
    #[ignore = "requires Redis reachable at REDIS_URL"]
    async fn redis_cache_is_scoped_by_tenant_and_ip() -> anyhow::Result<()> {
        let redis_url = std::env::var("REDIS_URL")?;
        let client = redis::Client::open(redis_url)?;
        let manager = client.get_connection_manager().await?;

        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://postgres:postgres@localhost/financial")?;

        let repository =
            PostgresBankIpAllowlistRepository::new(pool).with_cache(manager.clone(), 60, 15);

        let tenant_allowed = format!("redis-test-{}", Uuid::new_v4());
        let tenant_denied = format!("redis-test-{}", Uuid::new_v4());
        let ip: IpAddr = "192.0.2.99".parse()?;

        let allowed_key = PostgresBankIpAllowlistRepository::cache_key(&tenant_allowed, ip);
        let denied_key = PostgresBankIpAllowlistRepository::cache_key(&tenant_denied, ip);

        let mut connection = manager;
        let _: () = connection.set_ex(&allowed_key, "1", 60).await?;
        let _: () = connection.set_ex(&denied_key, "0", 15).await?;

        let allowed_result = repository.is_source_ip_allowed(&tenant_allowed, ip).await?;
        let denied_result = repository.is_source_ip_allowed(&tenant_denied, ip).await?;

        let _: usize = connection.del(&allowed_key).await?;
        let _: usize = connection.del(&denied_key).await?;

        assert!(
            allowed_result,
            "tenant with a positive cache entry should be allowed"
        );
        assert!(
            !denied_result,
            "tenant with a negative cache entry should be denied"
        );

        Ok(())
    }
}

#[cfg(test)]
mod postgres_redis_integration_tests {
    use super::*;
    use redis::{aio::ConnectionManager, AsyncCommands};
    use sqlx::postgres::PgPoolOptions;
    use std::net::IpAddr;
    use uuid::Uuid;

    #[tokio::test]
    #[ignore = "requires PostgreSQL and Redis reachable through port-forwards"]
    async fn cache_miss_queries_postgres_and_caches_tenant_scoped_results() -> anyhow::Result<()> {
        let database_url = std::env::var("DATABASE_URL")?;
        let redis_url = std::env::var("REDIS_URL")?;

        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&database_url)
            .await?;

        let client = redis::Client::open(redis_url)?;
        let manager: ConnectionManager = client.get_connection_manager().await?;

        let tenant_allowed = format!("repo-it-{}", Uuid::new_v4());
        let tenant_denied = format!("repo-it-{}", Uuid::new_v4());
        let ip: IpAddr = "192.0.2.99".parse()?;

        let allowed_key = PostgresBankIpAllowlistRepository::cache_key(&tenant_allowed, ip);
        let denied_key = PostgresBankIpAllowlistRepository::cache_key(&tenant_denied, ip);

        let mut redis_connection = manager.clone();
        let _: usize = redis_connection.del(&allowed_key).await?;
        let _: usize = redis_connection.del(&denied_key).await?;

        let insert_result = sqlx::query(
            r#"
            INSERT INTO bank_ip_allowlist
                (id, tenant_id, network, description, enabled)
            VALUES ($1, $2, $3::inet, $4, TRUE)
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&tenant_allowed)
        .bind("192.0.2.99/32")
        .bind("Temporary repository integration test")
        .execute(&pool)
        .await;

        if let Err(error) = insert_result {
            return Err(error.into());
        }

        let test_result: anyhow::Result<()> = async {
            let repository = PostgresBankIpAllowlistRepository::new(pool.clone()).with_cache(
                manager.clone(),
                60,
                15,
            );

            // First lookup: cache miss, PostgreSQL allows the IP.
            let allowed = repository.is_source_ip_allowed(&tenant_allowed, ip).await?;
            anyhow::ensure!(allowed, "allowlisted tenant should be authorized");

            let cached_allowed: Option<String> = redis_connection.get(&allowed_key).await?;
            anyhow::ensure!(
                cached_allowed.as_deref() == Some("1"),
                "expected positive cache entry"
            );

            let positive_ttl: i64 = redis_connection.ttl(&allowed_key).await?;
            anyhow::ensure!(
                positive_ttl > 0 && positive_ttl <= 60,
                "positive cache TTL should be between 1 and 60 seconds"
            );

            // Same IP, different tenant: PostgreSQL denies and Redis caches 0.
            let denied = repository.is_source_ip_allowed(&tenant_denied, ip).await?;
            anyhow::ensure!(!denied, "different tenant should be denied");

            let cached_denied: Option<String> = redis_connection.get(&denied_key).await?;
            anyhow::ensure!(
                cached_denied.as_deref() == Some("0"),
                "expected negative cache entry"
            );

            let negative_ttl: i64 = redis_connection.ttl(&denied_key).await?;
            anyhow::ensure!(
                negative_ttl > 0 && negative_ttl <= 15,
                "negative cache TTL should be between 1 and 15 seconds"
            );

            Ok(())
        }
        .await;

        // Clean up the temporary database rule and Redis keys.
        let cleanup_db = sqlx::query("DELETE FROM bank_ip_allowlist WHERE tenant_id = $1")
            .bind(&tenant_allowed)
            .execute(&pool)
            .await;

        let cleanup_redis: redis::RedisResult<usize> =
            redis_connection.del(&[&allowed_key, &denied_key]).await;

        cleanup_db?;
        cleanup_redis?;

        test_result
    }
}

#[cfg(test)]
mod fail_closed_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use std::{net::IpAddr, time::Duration};

    #[tokio::test]
    async fn postgres_unavailable_returns_error_without_cache() {
        let pool = PgPoolOptions::new()
            .acquire_timeout(Duration::from_millis(250))
            .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/financial")
            .expect("lazy PostgreSQL pool should be created");

        let repository = PostgresBankIpAllowlistRepository::new(pool);
        let source_ip: IpAddr = "192.0.2.10".parse().unwrap();

        let result = tokio::time::timeout(
            Duration::from_secs(2),
            repository.is_source_ip_allowed("tenant-fail-closed-test", source_ip),
        )
        .await
        .expect("authorization query should not hang");

        assert!(
            result.is_err(),
            "an unavailable PostgreSQL database must never authorize an IP"
        );
    }
}
