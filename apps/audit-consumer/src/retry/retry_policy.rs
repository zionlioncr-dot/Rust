use std::time::Duration;

/// Política de reintentos con Exponential Backoff.
///
/// Ejemplo:
///
/// attempt 1 -> 100 ms
/// attempt 2 -> 200 ms
/// attempt 3 -> 400 ms
/// attempt 4 -> 800 ms
/// attempt 5 -> 1600 ms
///
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Número máximo de intentos.
    pub max_attempts: u32,

    /// Delay inicial.
    pub initial_delay: Duration,

    /// Multiplicador exponencial.
    pub multiplier: u32,

    /// Delay máximo permitido.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_delay: Duration::from_millis(100),
            multiplier: 2,
            max_delay: Duration::from_secs(10),
        }
    }
}

impl RetryPolicy {
    pub fn new(
        max_attempts: u32,
        initial_delay: Duration,
        multiplier: u32,
        max_delay: Duration,
    ) -> Self {
        Self {
            max_attempts,
            initial_delay,
            multiplier,
            max_delay,
        }
    }

    /// Calcula el tiempo de espera para un intento.
    ///
    /// Fórmula:
    ///
    /// initial_delay × multiplier^(attempt-1)
    ///
    pub fn delay_for(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        let delay_ms =
            self.initial_delay.as_millis() as u64 * (self.multiplier as u64).pow(attempt - 1);

        let delay = Duration::from_millis(delay_ms);

        std::cmp::min(delay, self.max_delay)
    }

    /// Indica si aún se puede reintentar.
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }
}
