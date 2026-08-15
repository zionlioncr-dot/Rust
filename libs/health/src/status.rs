use serde::Serialize;

#[derive(Serialize)]
pub struct CheckStatus {
    pub name: String,
    pub status: String,
}

#[derive(Serialize)]
pub struct HealthStatus {
    pub ready: bool,
    pub checks: Vec<CheckStatus>,
}
