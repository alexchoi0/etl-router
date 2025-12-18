use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub max_retries: u32,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub backoff_multiplier: f64,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 30_000,
            backoff_multiplier: 2.0,
        }
    }
}

impl RetryPolicy {
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            ..Default::default()
        }
    }

    pub fn with_max_retries(mut self, max: u32) -> Self {
        self.max_retries = max;
        self
    }

    pub fn backoff_duration(&self, retry_count: u32) -> Duration {
        if retry_count == 0 {
            return Duration::from_millis(self.initial_backoff_ms);
        }

        let backoff = self.initial_backoff_ms as f64
            * self.backoff_multiplier.powi(retry_count as i32);
        let capped = backoff.min(self.max_backoff_ms as f64);
        Duration::from_millis(capped as u64)
    }

    pub fn should_retry(&self, retry_count: u32) -> bool {
        retry_count < self.max_retries
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqPolicy {
    pub retry: RetryPolicy,
    pub max_age_ms: u64,
    pub max_buffer_size: usize,
    pub batch_size: usize,
    pub flush_interval_ms: u64,
}

impl Default for DlqPolicy {
    fn default() -> Self {
        Self {
            retry: RetryPolicy::default(),
            max_age_ms: 7 * 24 * 60 * 60 * 1000, // 7 days
            max_buffer_size: 10_000,
            batch_size: 100,
            flush_interval_ms: 1000,
        }
    }
}

impl DlqPolicy {
    pub fn aggressive() -> Self {
        Self {
            retry: RetryPolicy {
                max_retries: 5,
                initial_backoff_ms: 50,
                max_backoff_ms: 60_000,
                backoff_multiplier: 2.0,
            },
            max_age_ms: 24 * 60 * 60 * 1000, // 1 day
            max_buffer_size: 50_000,
            batch_size: 500,
            flush_interval_ms: 500,
        }
    }

    pub fn conservative() -> Self {
        Self {
            retry: RetryPolicy {
                max_retries: 1,
                initial_backoff_ms: 1000,
                max_backoff_ms: 5000,
                backoff_multiplier: 1.5,
            },
            max_age_ms: 30 * 24 * 60 * 60 * 1000, // 30 days
            max_buffer_size: 5_000,
            batch_size: 50,
            flush_interval_ms: 5000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_duration() {
        let policy = RetryPolicy::default();

        assert_eq!(policy.backoff_duration(0), Duration::from_millis(100));
        assert_eq!(policy.backoff_duration(1), Duration::from_millis(200));
        assert_eq!(policy.backoff_duration(2), Duration::from_millis(400));
    }

    #[test]
    fn test_backoff_capped() {
        let policy = RetryPolicy {
            max_backoff_ms: 500,
            ..Default::default()
        };

        assert_eq!(policy.backoff_duration(10), Duration::from_millis(500));
    }

    #[test]
    fn test_should_retry() {
        let policy = RetryPolicy {
            max_retries: 3,
            ..Default::default()
        };

        assert!(policy.should_retry(0));
        assert!(policy.should_retry(1));
        assert!(policy.should_retry(2));
        assert!(!policy.should_retry(3));
        assert!(!policy.should_retry(4));
    }
}
