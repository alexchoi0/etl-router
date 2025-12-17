use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::RwLock;
use chrono::{Utc, Duration};
use uuid::Uuid;

use crate::types::{ErrorType, OperationalError, ErrorStats, ErrorTypeCount, ErrorFilter};

const MAX_ERRORS: usize = 10000;
const DEFAULT_LIMIT: i32 = 50;

#[derive(Clone)]
pub struct ErrorBuffer {
    inner: Arc<RwLock<ErrorBufferInner>>,
}

struct ErrorBufferInner {
    errors: VecDeque<OperationalError>,
}

impl ErrorBuffer {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(ErrorBufferInner {
                errors: VecDeque::with_capacity(MAX_ERRORS),
            })),
        }
    }

    pub async fn add_error(
        &self,
        error_type: ErrorType,
        message: String,
        pipeline_id: Option<String>,
        service_id: Option<String>,
        stage_id: Option<String>,
        details: Option<String>,
    ) -> String {
        let id = Uuid::new_v4().to_string();
        let error = OperationalError {
            id: id.clone(),
            error_type,
            pipeline_id,
            service_id,
            stage_id,
            message,
            details,
            timestamp: Utc::now(),
            retry_count: 0,
            resolved: false,
        };

        let mut inner = self.inner.write().await;
        if inner.errors.len() >= MAX_ERRORS {
            inner.errors.pop_front();
        }
        inner.errors.push_back(error);
        id
    }

    pub async fn get_errors(&self, filter: Option<ErrorFilter>) -> Vec<OperationalError> {
        let inner = self.inner.read().await;
        let filter = filter.unwrap_or(ErrorFilter {
            error_type: None,
            pipeline_id: None,
            resolved: None,
            limit: Some(DEFAULT_LIMIT),
            offset: Some(0),
        });

        let limit = filter.limit.unwrap_or(DEFAULT_LIMIT) as usize;
        let offset = filter.offset.unwrap_or(0) as usize;

        inner.errors
            .iter()
            .rev()
            .filter(|e| {
                if let Some(ref et) = filter.error_type {
                    if &e.error_type != et {
                        return false;
                    }
                }
                if let Some(ref pid) = filter.pipeline_id {
                    if e.pipeline_id.as_ref() != Some(pid) {
                        return false;
                    }
                }
                if let Some(resolved) = filter.resolved {
                    if e.resolved != resolved {
                        return false;
                    }
                }
                true
            })
            .skip(offset)
            .take(limit)
            .cloned()
            .collect()
    }

    pub async fn get_error(&self, id: &str) -> Option<OperationalError> {
        let inner = self.inner.read().await;
        inner.errors.iter().find(|e| e.id == id).cloned()
    }

    pub async fn resolve_error(&self, id: &str) -> bool {
        let mut inner = self.inner.write().await;
        if let Some(error) = inner.errors.iter_mut().find(|e| e.id == id) {
            error.resolved = true;
            true
        } else {
            false
        }
    }

    pub async fn get_stats(&self) -> ErrorStats {
        let inner = self.inner.read().await;
        let now = Utc::now();
        let hour_ago = now - Duration::hours(1);

        let total_errors = inner.errors.len() as u64;
        let unresolved_count = inner.errors.iter().filter(|e| !e.resolved).count() as u64;
        let errors_last_hour = inner.errors.iter()
            .filter(|e| e.timestamp > hour_ago)
            .count() as u64;

        let mut type_counts: std::collections::HashMap<ErrorType, u64> = std::collections::HashMap::new();
        for error in inner.errors.iter() {
            *type_counts.entry(error.error_type).or_insert(0) += 1;
        }

        let errors_by_type: Vec<ErrorTypeCount> = type_counts
            .into_iter()
            .map(|(error_type, count)| ErrorTypeCount { error_type, count })
            .collect();

        ErrorStats {
            total_errors,
            unresolved_count,
            errors_by_type,
            errors_last_hour,
        }
    }

    pub async fn clear_resolved(&self) {
        let mut inner = self.inner.write().await;
        inner.errors.retain(|e| !e.resolved);
    }
}

impl Default for ErrorBuffer {
    fn default() -> Self {
        Self::new()
    }
}
