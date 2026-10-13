use chrono::{DateTime, Utc};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct FraudEvent {
    pub tenant_id: String,
    pub event_id: Uuid,
    pub audit_id: Uuid,
    pub user: String,
    pub action: String,
    pub created_at: DateTime<Utc>,
    pub client_ip: Option<String>,
}

impl FraudEvent {
    pub fn new(
        tenant_id: String,
        event_id: Uuid,
        audit_id: Uuid,
        user: String,
        action: String,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            tenant_id,
            event_id,
            audit_id,
            user,
            action,
            created_at,
            client_ip: None,
        }
    }

    pub fn with_client_ip(mut self, client_ip: Option<String>) -> Self {
        self.client_ip = client_ip;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn event() -> FraudEvent {
        FraudEvent::new(
            "tenant-test".to_string(),
            Uuid::new_v4(),
            Uuid::new_v4(),
            "alice".to_string(),
            "PASSWORD_RESET".to_string(),
            Utc::now(),
        )
    }

    #[test]
    fn preserves_client_ip() {
        let event = event().with_client_ip(Some("203.0.113.25".to_string()));

        assert_eq!(event.client_ip.as_deref(), Some("203.0.113.25"));
    }

    #[test]
    fn client_ip_is_optional() {
        let event = event();

        assert_eq!(event.client_ip, None);
    }
}
