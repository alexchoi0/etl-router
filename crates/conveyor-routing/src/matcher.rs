use serde::{Deserialize, Serialize};
use regex::Regex;

use conveyor_proto::common::Record;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Condition {
    RecordType(String),
    MetadataMatch { key: String, pattern: String },
    MetadataExists(String),
    MetadataEquals { key: String, value: String },
    And(Vec<Condition>),
    Or(Vec<Condition>),
    Not(Box<Condition>),
    Always,
    Never,
}

impl Condition {
    pub fn evaluate(&self, record: &Record) -> bool {
        match self {
            Condition::RecordType(expected) => {
                record.record_type == *expected
            }
            Condition::MetadataMatch { key, pattern } => {
                if let Some(value) = record.metadata.get(key) {
                    if let Ok(re) = Regex::new(pattern) {
                        re.is_match(value)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            Condition::MetadataExists(key) => {
                record.metadata.contains_key(key)
            }
            Condition::MetadataEquals { key, value } => {
                record.metadata.get(key).map(|v| v == value).unwrap_or(false)
            }
            Condition::And(conditions) => {
                conditions.iter().all(|c| c.evaluate(record))
            }
            Condition::Or(conditions) => {
                conditions.iter().any(|c| c.evaluate(record))
            }
            Condition::Not(condition) => {
                !condition.evaluate(record)
            }
            Condition::Always => true,
            Condition::Never => false,
        }
    }
}

impl Default for Condition {
    fn default() -> Self {
        Condition::Always
    }
}
