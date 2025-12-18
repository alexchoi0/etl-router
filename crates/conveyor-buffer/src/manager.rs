use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::Result;
use dashmap::DashMap;
use tracing::warn;

use conveyor_config::BufferSettings;
use conveyor_proto::common::Record;

#[derive(Debug, Clone)]
pub struct BufferedRecord {
    pub record: Record,
    pub source_id: String,
    pub pipeline_id: String,
    pub target_stage_id: String,
    pub buffered_at: Instant,
    pub retry_count: u32,
}

struct StageBuffer {
    records: VecDeque<BufferedRecord>,
    max_size: usize,
}

impl StageBuffer {
    fn new(max_size: usize) -> Self {
        Self {
            records: VecDeque::new(),
            max_size,
        }
    }

    fn push(&mut self, record: BufferedRecord) -> bool {
        if self.records.len() >= self.max_size {
            return false;
        }
        self.records.push_back(record);
        true
    }

    fn pop_batch(&mut self, max_batch_size: usize) -> Vec<BufferedRecord> {
        let count = std::cmp::min(max_batch_size, self.records.len());
        self.records.drain(..count).collect()
    }

    fn len(&self) -> usize {
        self.records.len()
    }

    fn utilization(&self) -> f64 {
        self.records.len() as f64 / self.max_size as f64
    }
}

pub struct BufferManager {
    stage_buffers: DashMap<String, StageBuffer>,
    source_buffers: DashMap<String, VecDeque<BufferedRecord>>,

    max_total_records: usize,
    max_per_stage: usize,
    max_per_source: usize,
    backpressure_threshold: f64,

    total_records: AtomicUsize,
}

impl BufferManager {
    pub fn new(settings: BufferSettings) -> Self {
        Self {
            stage_buffers: DashMap::new(),
            source_buffers: DashMap::new(),
            max_total_records: settings.max_total_records,
            max_per_stage: settings.max_per_stage,
            max_per_source: settings.max_per_source,
            backpressure_threshold: settings.backpressure_threshold,
            total_records: AtomicUsize::new(0),
        }
    }

    pub fn with_limits(
        max_total_records: usize,
        max_per_stage: usize,
        max_per_source: usize,
        backpressure_threshold: f64,
    ) -> Self {
        Self {
            stage_buffers: DashMap::new(),
            source_buffers: DashMap::new(),
            max_total_records,
            max_per_stage,
            max_per_source,
            backpressure_threshold,
            total_records: AtomicUsize::new(0),
        }
    }

    pub async fn buffer_for_stage(
        &self,
        stage_id: &str,
        record: BufferedRecord,
    ) -> Result<()> {
        let total = self.total_records.load(Ordering::Acquire);
        if total >= self.max_total_records {
            return Err(anyhow::anyhow!("Global buffer full"));
        }

        let mut buffer = self
            .stage_buffers
            .entry(stage_id.to_string())
            .or_insert_with(|| StageBuffer::new(self.max_per_stage));

        if buffer.push(record) {
            self.total_records.fetch_add(1, Ordering::Release);
            Ok(())
        } else {
            Err(anyhow::anyhow!("Stage buffer full: {}", stage_id))
        }
    }

    pub async fn buffer_batch_for_stage(
        &self,
        stage_id: &str,
        records: Vec<BufferedRecord>,
    ) -> Result<usize> {
        let total = self.total_records.load(Ordering::Acquire);
        let available = self.max_total_records.saturating_sub(total);

        if available == 0 {
            return Err(anyhow::anyhow!("Global buffer full"));
        }

        let mut buffer = self
            .stage_buffers
            .entry(stage_id.to_string())
            .or_insert_with(|| StageBuffer::new(self.max_per_stage));

        let mut buffered = 0;
        for record in records {
            if buffered >= available {
                break;
            }
            if buffer.push(record) {
                buffered += 1;
            } else {
                break;
            }
        }

        self.total_records.fetch_add(buffered, Ordering::Release);
        Ok(buffered)
    }

    pub async fn get_batch(&self, stage_id: &str, max_batch_size: usize) -> Vec<BufferedRecord> {
        if let Some(mut buffer) = self.stage_buffers.get_mut(stage_id) {
            let batch = buffer.pop_batch(max_batch_size);
            self.total_records.fetch_sub(batch.len(), Ordering::Release);
            batch
        } else {
            Vec::new()
        }
    }

    pub async fn return_to_buffer(&self, stage_id: &str, records: Vec<BufferedRecord>) {
        let mut buffer = self
            .stage_buffers
            .entry(stage_id.to_string())
            .or_insert_with(|| StageBuffer::new(self.max_per_stage));

        for mut record in records {
            record.retry_count += 1;
            if buffer.push(record) {
                self.total_records.fetch_add(1, Ordering::Release);
            } else {
                warn!(stage_id = %stage_id, "Failed to return record to buffer - buffer full");
            }
        }
    }

    pub async fn should_backpressure(&self, source_id: &str) -> bool {
        let total = self.total_records.load(Ordering::Acquire);
        let global_utilization = total as f64 / self.max_total_records as f64;

        if global_utilization > self.backpressure_threshold {
            return true;
        }

        if let Some(buffer) = self.source_buffers.get(source_id) {
            let source_utilization = buffer.len() as f64 / self.max_per_source as f64;
            if source_utilization > self.backpressure_threshold {
                return true;
            }
        }

        false
    }

    pub async fn available_credits(&self, source_id: &str) -> u64 {
        let total = self.total_records.load(Ordering::Acquire);
        let global_available = self.max_total_records.saturating_sub(total);

        let source_used = self
            .source_buffers
            .get(source_id)
            .map(|b| b.len())
            .unwrap_or(0);
        let source_available = self.max_per_source.saturating_sub(source_used);

        std::cmp::min(global_available, source_available) as u64
    }

    pub async fn get_stage_buffer_size(&self, stage_id: &str) -> usize {
        self.stage_buffers
            .get(stage_id)
            .map(|b| b.len())
            .unwrap_or(0)
    }

    pub async fn get_stage_utilization(&self, stage_id: &str) -> f64 {
        self.stage_buffers
            .get(stage_id)
            .map(|b| b.utilization())
            .unwrap_or(0.0)
    }

    pub async fn get_global_utilization(&self) -> f64 {
        let total = self.total_records.load(Ordering::Acquire);
        total as f64 / self.max_total_records as f64
    }

    pub async fn get_total_buffered(&self) -> usize {
        self.total_records.load(Ordering::Acquire)
    }

    pub async fn get_stages_with_data(&self) -> Vec<String> {
        self.stage_buffers
            .iter()
            .filter(|entry| entry.value().len() > 0)
            .map(|entry| entry.key().clone())
            .collect()
    }
}

impl Default for BufferManager {
    fn default() -> Self {
        Self::with_limits(100_000, 10_000, 5_000, 0.8)
    }
}
