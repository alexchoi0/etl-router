use async_graphql::{SimpleObject, InputObject, Enum};
use chrono::{DateTime, Utc};

#[derive(SimpleObject, Clone)]
pub struct ClusterStatus {
    pub node_id: u64,
    pub role: NodeRole,
    pub current_term: u64,
    pub leader_id: Option<u64>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub log_length: u64,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum NodeRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(SimpleObject, Clone)]
pub struct Service {
    pub service_id: String,
    pub service_name: String,
    pub service_type: ServiceType,
    pub endpoint: String,
    pub labels: Vec<Label>,
    pub health: ServiceHealth,
    pub group_id: Option<String>,
    pub registered_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
}

#[derive(SimpleObject, Clone)]
pub struct Label {
    pub key: String,
    pub value: String,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum ServiceType {
    Source,
    Transform,
    Sink,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
pub enum ServiceHealth {
    Healthy,
    Unhealthy,
    Unknown,
}

#[derive(SimpleObject, Clone)]
pub struct Pipeline {
    pub pipeline_id: String,
    pub name: String,
    pub enabled: bool,
    pub version: u64,
}

#[derive(SimpleObject, Clone)]
pub struct SourceOffset {
    pub source_id: String,
    pub partition: u32,
    pub offset: u64,
}

#[derive(SimpleObject, Clone)]
pub struct GroupOffsets {
    pub group_id: String,
    pub source_id: String,
    pub partitions: Vec<PartitionOffset>,
}

#[derive(SimpleObject, Clone)]
pub struct PartitionOffset {
    pub partition: u32,
    pub offset: u64,
}

#[derive(SimpleObject, Clone)]
pub struct ConsumerGroup {
    pub group_id: String,
    pub stage_id: String,
    pub members: Vec<String>,
    pub generation: u64,
    pub partition_assignments: Vec<MemberAssignment>,
}

#[derive(SimpleObject, Clone)]
pub struct MemberAssignment {
    pub member_id: String,
    pub partitions: Vec<u32>,
}

#[derive(SimpleObject, Clone)]
pub struct ServiceCheckpoint {
    pub service_id: String,
    pub checkpoint_id: String,
    pub source_offsets: Vec<SourceOffsetEntry>,
    pub created_at: DateTime<Utc>,
}

#[derive(SimpleObject, Clone)]
pub struct SourceOffsetEntry {
    pub source_id: String,
    pub offset: u64,
}

#[derive(SimpleObject, Clone)]
pub struct Watermark {
    pub source_id: String,
    pub partition: u32,
    pub position: u64,
    pub event_time: Option<DateTime<Utc>>,
}

#[derive(InputObject)]
pub struct RegisterServiceInput {
    pub service_id: String,
    pub service_name: String,
    pub service_type: ServiceType,
    pub endpoint: String,
    pub labels: Option<Vec<LabelInput>>,
    pub group_id: Option<String>,
}

#[derive(InputObject)]
pub struct LabelInput {
    pub key: String,
    pub value: String,
}

#[derive(InputObject)]
pub struct CommitOffsetInput {
    pub source_id: String,
    pub partition: u32,
    pub offset: u64,
}

#[derive(InputObject)]
pub struct CommitGroupOffsetInput {
    pub group_id: String,
    pub source_id: String,
    pub partition: u32,
    pub offset: u64,
}

#[derive(InputObject)]
pub struct CreatePipelineInput {
    pub pipeline_id: String,
    pub name: String,
    pub config: String,
}

#[derive(InputObject)]
pub struct AdvanceWatermarkInput {
    pub source_id: String,
    pub partition: u32,
    pub position: u64,
    pub event_time: Option<DateTime<Utc>>,
}

#[derive(SimpleObject)]
pub struct MutationResult {
    pub success: bool,
    pub message: Option<String>,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Hash)]
pub enum ErrorType {
    PipelineFailure,
    TransformError,
    SinkError,
    SourceError,
    DlqRecord,
    ServiceUnhealthy,
    Timeout,
    ValidationError,
    Unknown,
}

#[derive(SimpleObject, Clone)]
pub struct OperationalError {
    pub id: String,
    pub error_type: ErrorType,
    pub pipeline_id: Option<String>,
    pub service_id: Option<String>,
    pub stage_id: Option<String>,
    pub message: String,
    pub details: Option<String>,
    pub timestamp: DateTime<Utc>,
    pub retry_count: u32,
    pub resolved: bool,
}

#[derive(SimpleObject, Clone)]
pub struct ErrorTypeCount {
    pub error_type: ErrorType,
    pub count: u64,
}

#[derive(SimpleObject, Clone)]
pub struct ErrorStats {
    pub total_errors: u64,
    pub unresolved_count: u64,
    pub errors_by_type: Vec<ErrorTypeCount>,
    pub errors_last_hour: u64,
}

#[derive(InputObject)]
pub struct ErrorFilter {
    pub error_type: Option<ErrorType>,
    pub pipeline_id: Option<String>,
    pub resolved: Option<bool>,
    pub limit: Option<i32>,
    pub offset: Option<i32>,
}
