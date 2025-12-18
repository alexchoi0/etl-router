use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Watermark {
    pub timestamp: i64,
    pub source_id: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct HeapEntry {
    watermark: i64,
    source_id: String,
    generation: u64,
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.watermark
            .cmp(&other.watermark)
            .then_with(|| self.source_id.cmp(&other.source_id))
    }
}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
struct SourceWatermarkState {
    watermark: i64,
    last_update: Instant,
    idle_timeout: Option<Duration>,
    generation: u64,
}

#[derive(Debug)]
pub struct WatermarkTracker {
    source_state: HashMap<String, SourceWatermarkState>,
    min_heap: BinaryHeap<Reverse<HeapEntry>>,
    allowed_lateness: Duration,
}

impl WatermarkTracker {
    pub fn new(source_ids: Vec<String>, allowed_lateness: Duration) -> Self {
        let mut source_state = HashMap::new();
        let mut min_heap = BinaryHeap::new();

        for id in source_ids {
            let state = SourceWatermarkState {
                watermark: i64::MIN,
                last_update: Instant::now(),
                idle_timeout: None,
                generation: 0,
            };

            min_heap.push(Reverse(HeapEntry {
                watermark: i64::MIN,
                source_id: id.clone(),
                generation: 0,
            }));

            source_state.insert(id, state);
        }

        Self {
            source_state,
            min_heap,
            allowed_lateness,
        }
    }

    pub fn set_idle_timeout(&mut self, source_id: &str, timeout: Duration) {
        if let Some(state) = self.source_state.get_mut(source_id) {
            state.idle_timeout = Some(timeout);
        }
    }

    pub fn update(&mut self, source_id: &str, timestamp: i64) {
        if let Some(state) = self.source_state.get_mut(source_id) {
            if timestamp > state.watermark {
                state.watermark = timestamp;
                state.last_update = Instant::now();
                state.generation += 1;

                self.min_heap.push(Reverse(HeapEntry {
                    watermark: timestamp,
                    source_id: source_id.to_string(),
                    generation: state.generation,
                }));
            }
        }
    }

    pub fn combined_watermark(&mut self) -> i64 {
        self.cleanup_stale_entries();

        self.min_heap
            .peek()
            .map(|Reverse(entry)| entry.watermark)
            .unwrap_or(i64::MIN)
    }

    fn cleanup_stale_entries(&mut self) {
        while let Some(Reverse(entry)) = self.min_heap.peek() {
            if let Some(state) = self.source_state.get(&entry.source_id) {
                if entry.generation == state.generation {
                    break;
                }
            }
            self.min_heap.pop();
        }
    }

    pub fn is_late(&mut self, event_time: i64) -> bool {
        let combined = self.combined_watermark();
        if combined == i64::MIN {
            return false;
        }

        let lateness_ms = self.allowed_lateness.as_millis() as i64;
        event_time < (combined - lateness_ms)
    }

    pub fn advance_idle_sources(&mut self, processing_time: i64) {
        let now = Instant::now();
        let mut updates = Vec::new();

        for (source_id, state) in &self.source_state {
            if let Some(timeout) = state.idle_timeout {
                if now.duration_since(state.last_update) >= timeout {
                    if processing_time > state.watermark {
                        updates.push((source_id.clone(), processing_time));
                    }
                }
            }
        }

        for (source_id, timestamp) in updates {
            self.update(&source_id, timestamp);
        }
    }

    pub fn get_source_watermark(&self, source_id: &str) -> Option<i64> {
        self.source_state.get(source_id).map(|s| s.watermark)
    }

    pub fn source_count(&self) -> usize {
        self.source_state.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_combined_watermark_minimum() {
        let mut tracker = WatermarkTracker::new(
            vec!["source1".into(), "source2".into()],
            Duration::from_secs(0),
        );

        tracker.update("source1", 1000);
        tracker.update("source2", 500);

        assert_eq!(tracker.combined_watermark(), 500);
    }

    #[test]
    fn test_combined_watermark_advances() {
        let mut tracker = WatermarkTracker::new(
            vec!["source1".into(), "source2".into()],
            Duration::from_secs(0),
        );

        tracker.update("source1", 1000);
        tracker.update("source2", 500);
        assert_eq!(tracker.combined_watermark(), 500);

        tracker.update("source2", 1500);
        assert_eq!(tracker.combined_watermark(), 1000);
    }

    #[test]
    fn test_is_late_with_lateness() {
        let mut tracker = WatermarkTracker::new(
            vec!["source1".into()],
            Duration::from_millis(100),
        );

        tracker.update("source1", 1000);

        assert!(!tracker.is_late(950));
        assert!(!tracker.is_late(900));
        assert!(tracker.is_late(899));
    }

    #[test]
    fn test_watermark_only_advances() {
        let mut tracker = WatermarkTracker::new(
            vec!["source1".into()],
            Duration::from_secs(0),
        );

        tracker.update("source1", 1000);
        tracker.update("source1", 500);

        assert_eq!(tracker.combined_watermark(), 1000);
    }

    #[test]
    fn test_stale_entries_cleaned_up() {
        let mut tracker = WatermarkTracker::new(
            vec!["source1".into(), "source2".into()],
            Duration::from_secs(0),
        );

        tracker.update("source1", 100);
        tracker.update("source1", 200);
        tracker.update("source1", 300);
        tracker.update("source2", 150);

        assert_eq!(tracker.combined_watermark(), 150);
        assert!(tracker.min_heap.len() <= 3);
    }

    #[test]
    fn test_many_sources() {
        let sources: Vec<String> = (0..100).map(|i| format!("source{}", i)).collect();
        let mut tracker = WatermarkTracker::new(sources.clone(), Duration::from_secs(0));

        for (i, source) in sources.iter().enumerate() {
            tracker.update(source, (i * 100) as i64);
        }

        assert_eq!(tracker.combined_watermark(), 0);

        tracker.update("source0", 5000);
        assert_eq!(tracker.combined_watermark(), 100);
    }
}
