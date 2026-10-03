use anyhow::{Result, anyhow};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TenantContext {
    pub tenant_id: String,
}

impl TenantContext {
    pub fn new(tenant_id: impl Into<String>) -> Result<Self> {
        let tenant_id = tenant_id.into().trim().to_string();

        if tenant_id.is_empty() {
            return Err(anyhow!("tenant_id cannot be empty"));
        }

        Ok(Self { tenant_id })
    }

    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
}
