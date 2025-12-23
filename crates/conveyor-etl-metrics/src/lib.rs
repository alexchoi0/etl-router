mod prometheus;

pub use prometheus::MetricsExporter;

use metrics::{counter, gauge, histogram};

pub fn record_records_received(source_id: &str, count: u64) {
    counter!("conveyor_etl_router_records_received_total", "source_id" => source_id.to_string())
        .increment(count);
}

pub fn record_records_routed(pipeline_id: &str, stage_id: &str, count: u64) {
    counter!(
        "conveyor_etl_router_records_routed_total",
        "pipeline_id" => pipeline_id.to_string(),
        "stage_id" => stage_id.to_string()
    )
    .increment(count);
}

pub fn record_records_delivered(sink_id: &str, count: u64) {
    counter!("conveyor_etl_router_records_delivered_total", "sink_id" => sink_id.to_string())
        .increment(count);
}

pub fn record_routing_latency(pipeline_id: &str, latency_ms: f64) {
    histogram!(
        "conveyor_etl_router_routing_latency_ms",
        "pipeline_id" => pipeline_id.to_string()
    )
    .record(latency_ms);
}

pub fn record_buffer_utilization(stage_id: &str, utilization: f64) {
    gauge!(
        "conveyor_etl_router_buffer_utilization",
        "stage_id" => stage_id.to_string()
    )
    .set(utilization);
}

pub fn record_active_services(service_type: &str, count: f64) {
    gauge!(
        "conveyor_etl_router_active_services",
        "service_type" => service_type.to_string()
    )
    .set(count);
}

pub fn record_raft_state(is_leader: bool, term: u64) {
    gauge!("conveyor_etl_router_raft_is_leader").set(if is_leader { 1.0 } else { 0.0 });
    gauge!("conveyor_etl_router_raft_term").set(term as f64);
}

pub fn record_backpressure_events(source_id: &str) {
    counter!(
        "conveyor_etl_router_backpressure_events_total",
        "source_id" => source_id.to_string()
    )
    .increment(1);
}

pub fn record_retry_events(stage_id: &str) {
    counter!(
        "conveyor_etl_router_retry_events_total",
        "stage_id" => stage_id.to_string()
    )
    .increment(1);
}

pub fn record_checkpoint_saved(service_id: &str) {
    counter!(
        "conveyor_etl_router_checkpoints_saved_total",
        "service_id" => service_id.to_string()
    )
    .increment(1);
}

pub fn record_group_rebalance(group_id: &str) {
    counter!(
        "conveyor_etl_router_group_rebalances_total",
        "group_id" => group_id.to_string()
    )
    .increment(1);
}
