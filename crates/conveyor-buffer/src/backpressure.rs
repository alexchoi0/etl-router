use dashmap::DashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureSignal {
    None,
    SlowDown { delay_ms: u64 },
    Pause,
}

pub struct BackpressureController {
    source_states: DashMap<String, SourceBackpressureState>,
    high_watermark: f64,
    low_watermark: f64,
}

struct SourceBackpressureState {
    current_signal: BackpressureSignal,
    credits_granted: u64,
    credits_used: u64,
}

impl BackpressureController {
    pub fn new(high_watermark: f64, low_watermark: f64) -> Self {
        Self {
            source_states: DashMap::new(),
            high_watermark,
            low_watermark,
        }
    }

    pub async fn compute_signal(&self, source_id: &str, utilization: f64) -> BackpressureSignal {
        let mut state = self.source_states
            .entry(source_id.to_string())
            .or_insert_with(|| SourceBackpressureState {
                current_signal: BackpressureSignal::None,
                credits_granted: 0,
                credits_used: 0,
            });

        let new_signal = if utilization >= self.high_watermark {
            BackpressureSignal::Pause
        } else if utilization > self.low_watermark {
            let delay = ((utilization - self.low_watermark)
                / (self.high_watermark - self.low_watermark)
                * 100.0) as u64;
            BackpressureSignal::SlowDown {
                delay_ms: delay.max(10),
            }
        } else {
            BackpressureSignal::None
        };

        state.current_signal = new_signal;
        new_signal
    }

    pub async fn grant_credits(&self, source_id: &str, credits: u64) {
        let mut state = self.source_states
            .entry(source_id.to_string())
            .or_insert_with(|| SourceBackpressureState {
                current_signal: BackpressureSignal::None,
                credits_granted: 0,
                credits_used: 0,
            });
        state.credits_granted += credits;
    }

    pub async fn use_credits(&self, source_id: &str, credits: u64) {
        if let Some(mut state) = self.source_states.get_mut(source_id) {
            state.credits_used += credits;
        }
    }

    pub async fn get_available_credits(&self, source_id: &str) -> u64 {
        self.source_states
            .get(source_id)
            .map(|s| s.credits_granted.saturating_sub(s.credits_used))
            .unwrap_or(0)
    }

    pub async fn get_current_signal(&self, source_id: &str) -> BackpressureSignal {
        self.source_states
            .get(source_id)
            .map(|s| s.current_signal)
            .unwrap_or(BackpressureSignal::None)
    }

    pub async fn reset_credits(&self, source_id: &str) {
        if let Some(mut state) = self.source_states.get_mut(source_id) {
            state.credits_granted = 0;
            state.credits_used = 0;
        }
    }
}

impl Default for BackpressureController {
    fn default() -> Self {
        Self::new(0.9, 0.7)
    }
}
