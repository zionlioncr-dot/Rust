use anyhow::Result;
use async_trait::async_trait;
use std::net::IpAddr;

#[async_trait]
pub trait BankIpAllowlistRepository: Send + Sync {
    /// Returns true when the source IP is authorized for the tenant.
    ///
    /// The lookup is tenant-scoped and supports both individual
    /// addresses and CIDR networks.
    async fn is_source_ip_allowed(&self, tenant_id: &str, source_ip: IpAddr) -> Result<bool>;
}
