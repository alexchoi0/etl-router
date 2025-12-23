use std::collections::HashMap;
use serde::{Deserialize, Serialize};

pub const API_VERSION: &str = "etl.dev/v1";
pub const KIND_PIPELINE: &str = "Pipeline";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineManifest {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: PipelineSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta {
    pub name: String,
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub annotations: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PipelineSpec {
    #[serde(default)]
    pub description: String,
    pub stages: Vec<StageDsl>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
}

fn default_enabled() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageDsl {
    pub id: String,
    pub name: String,
    #[serde(rename = "type")]
    pub stage_type: StageTypeDsl,
    #[serde(default)]
    pub service: ServiceSelectorDsl,
    #[serde(default = "default_parallelism")]
    pub parallelism: u32,
    #[serde(default)]
    pub config: Option<StageConfigDsl>,
    #[serde(default)]
    pub sources: Option<Vec<FanInSourceDsl>>,
    #[serde(default)]
    pub sinks: Option<Vec<FanOutSinkDsl>>,
    #[serde(default)]
    pub watermark: Option<FanInWatermarkDsl>,
}

fn default_parallelism() -> u32 {
    1
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StageTypeDsl {
    Source,
    Transform,
    Lookup,
    FanIn,
    FanOut,
    Sink,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ServiceSelectorDsl {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub group: Option<String>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
    #[serde(default)]
    pub load_balance: LoadBalanceDsl,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalanceDsl {
    #[default]
    RoundRobin,
    LeastConnections,
    WeightedRandom,
    ConsistentHash,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionDsl {
    #[serde(default)]
    pub record_type: Option<String>,
    #[serde(default)]
    pub metadata_exists: Option<String>,
    #[serde(default)]
    pub metadata_equals: Option<MetadataEqualsDsl>,
    #[serde(default)]
    pub metadata_matches: Option<MetadataMatchesDsl>,
    #[serde(default)]
    pub and: Option<Vec<ConditionDsl>>,
    #[serde(default)]
    pub or: Option<Vec<ConditionDsl>>,
    #[serde(default)]
    pub not: Option<Box<ConditionDsl>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataEqualsDsl {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataMatchesDsl {
    pub key: String,
    pub pattern: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum StageConfigDsl {
    Source(SourceConfigDsl),
    Transform(TransformConfigDsl),
    Lookup(LookupConfigDsl),
    Sink(SinkConfigDsl),
}

// ============================================================================
// SOURCE CONFIGURATIONS
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "source_type", rename_all = "lowercase")]
pub enum SourceConfigDsl {
    Kafka(KafkaSourceConfig),
    Kinesis(KinesisSourceConfig),
    Pulsar(PulsarSourceConfig),
    Rabbitmq(RabbitmqSourceConfig),
    Sqs(SqsSourceConfig),
    Redis(RedisSourceConfig),
    Nats(NatsSourceConfig),
    File(FileSourceConfig),
    S3(S3SourceConfig),
    Gcs(GcsSourceConfig),
    Http(HttpSourceConfig),
    Websocket(WebsocketSourceConfig),
    Postgres(PostgresSourceConfig),
    Mysql(MysqlSourceConfig),
    Mongodb(MongodbSourceConfig),
    Grpc(GrpcSourceConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSourceConfig {
    pub brokers: Vec<String>,
    pub topic: String,
    #[serde(default)]
    pub consumer_group: Option<String>,
    #[serde(default)]
    pub auto_offset_reset: Option<String>,
    #[serde(default)]
    pub security: Option<KafkaSecurityConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaSecurityConfig {
    #[serde(default)]
    pub protocol: Option<String>,
    #[serde(default)]
    pub sasl_mechanism: Option<String>,
    #[serde(default)]
    pub sasl_username: Option<String>,
    #[serde(default)]
    pub sasl_password_env: Option<String>,
    #[serde(default)]
    pub ssl_ca_location: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KinesisSourceConfig {
    pub stream_name: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub shard_iterator_type: Option<String>,
    #[serde(default)]
    pub credentials: Option<AwsCredentialsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsCredentialsConfig {
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub access_key_id_env: Option<String>,
    #[serde(default)]
    pub secret_access_key_env: Option<String>,
    #[serde(default)]
    pub role_arn: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulsarSourceConfig {
    pub service_url: String,
    pub topic: String,
    #[serde(default)]
    pub subscription: Option<String>,
    #[serde(default)]
    pub subscription_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RabbitmqSourceConfig {
    pub url: String,
    pub queue: String,
    #[serde(default)]
    pub prefetch_count: Option<u32>,
    #[serde(default)]
    pub auto_ack: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqsSourceConfig {
    pub queue_url: String,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub max_messages: Option<u32>,
    #[serde(default)]
    pub wait_time_seconds: Option<u32>,
    #[serde(default)]
    pub credentials: Option<AwsCredentialsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisSourceConfig {
    pub url: String,
    #[serde(default)]
    pub channel: Option<String>,
    #[serde(default)]
    pub stream: Option<String>,
    #[serde(default)]
    pub consumer_group: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsSourceConfig {
    pub url: String,
    pub subject: String,
    #[serde(default)]
    pub queue_group: Option<String>,
    #[serde(default)]
    pub jetstream: Option<bool>,
    #[serde(default)]
    pub durable: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileSourceConfig {
    pub path: String,
    #[serde(default)]
    pub format: Option<FileFormat>,
    #[serde(default)]
    pub watch: Option<bool>,
    #[serde(default)]
    pub pattern: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Json,
    Csv,
    Parquet,
    Avro,
    Lines,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3SourceConfig {
    pub bucket: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub format: Option<FileFormat>,
    #[serde(default)]
    pub credentials: Option<AwsCredentialsConfig>,
    #[serde(default)]
    pub poll_interval_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcsSourceConfig {
    pub bucket: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub format: Option<FileFormat>,
    #[serde(default)]
    pub credentials_file: Option<String>,
    #[serde(default)]
    pub poll_interval_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpSourceConfig {
    pub listen_addr: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub methods: Option<Vec<String>>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    pub cert_file: String,
    pub key_file: String,
    #[serde(default)]
    pub ca_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebsocketSourceConfig {
    pub listen_addr: String,
    #[serde(default)]
    pub path: Option<String>,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresSourceConfig {
    pub connection_string: String,
    #[serde(default)]
    pub publication: Option<String>,
    #[serde(default)]
    pub slot_name: Option<String>,
    #[serde(default)]
    pub tables: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlSourceConfig {
    pub connection_string: String,
    #[serde(default)]
    pub server_id: Option<u32>,
    #[serde(default)]
    pub tables: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongodbSourceConfig {
    pub connection_string: String,
    pub database: String,
    #[serde(default)]
    pub collection: Option<String>,
    #[serde(default)]
    pub pipeline: Option<Vec<HashMap<String, serde_json::Value>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcSourceConfig {
    pub listen_addr: String,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub max_message_size: Option<usize>,
}

// ============================================================================
// TRANSFORM CONFIGURATIONS
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "transform_type", rename_all = "lowercase")]
pub enum TransformConfigDsl {
    // Stateless transforms
    Filter(FilterTransformConfig),
    Map(MapTransformConfig),
    Project(ProjectTransformConfig),
    Rename(RenameTransformConfig),
    Cast(CastTransformConfig),
    Mask(MaskTransformConfig),
    Validate(ValidateTransformConfig),
    FlatMap(FlatMapTransformConfig),
    Split(SplitTransformConfig),
    // Stateful transforms
    Dedupe(DedupeTransformConfig),
    RateLimit(RateLimitTransformConfig),
    Aggregate(AggregateTransformConfig),
    Join(JoinTransformConfig),
    Sessionize(SessionizeTransformConfig),
}

// Stateless transforms

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterTransformConfig {
    pub condition: ConditionDsl,
    #[serde(default)]
    pub negate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MapTransformConfig {
    pub mappings: Vec<FieldMapping>,
    #[serde(default)]
    pub drop_unmapped: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMapping {
    pub target: String,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub expression: Option<String>,
    #[serde(default)]
    pub default: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectTransformConfig {
    pub fields: Vec<String>,
    #[serde(default)]
    pub exclude: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RenameTransformConfig {
    pub renames: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CastTransformConfig {
    pub casts: HashMap<String, FieldType>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Int,
    Int64,
    Float,
    Float64,
    Bool,
    Timestamp,
    Date,
    Json,
    Bytes,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaskTransformConfig {
    pub fields: Vec<MaskFieldConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaskFieldConfig {
    pub field: String,
    #[serde(default)]
    pub strategy: MaskStrategy,
    #[serde(default)]
    pub replacement: Option<String>,
    #[serde(default)]
    pub preserve_length: Option<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaskStrategy {
    #[default]
    Redact,
    Hash,
    Partial,
    Nullify,
    Tokenize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateTransformConfig {
    #[serde(default)]
    pub schema: Option<serde_json::Value>,
    #[serde(default)]
    pub schema_registry_url: Option<String>,
    #[serde(default)]
    pub subject: Option<String>,
    #[serde(default)]
    pub on_invalid: ValidationAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationAction {
    #[default]
    Drop,
    Error,
    Tag,
    Route,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlatMapTransformConfig {
    pub field: String,
    #[serde(default)]
    pub target_field: Option<String>,
    #[serde(default)]
    pub keep_parent: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitTransformConfig {
    pub routes: Vec<SplitRoute>,
    #[serde(default)]
    pub default_output: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitRoute {
    pub condition: ConditionDsl,
    pub output: String,
}

// Stateful transforms

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupeTransformConfig {
    pub key_fields: Vec<String>,
    pub window: WindowConfig,
    #[serde(default)]
    pub keep: DedupeKeep,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DedupeKeep {
    #[default]
    First,
    Last,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WindowConfig {
    #[serde(default)]
    pub tumbling: Option<DurationConfig>,
    #[serde(default)]
    pub sliding: Option<SlidingWindowConfig>,
    #[serde(default)]
    pub session: Option<DurationConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DurationConfig {
    #[serde(default)]
    pub seconds: Option<u64>,
    #[serde(default)]
    pub minutes: Option<u64>,
    #[serde(default)]
    pub hours: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlidingWindowConfig {
    pub size: DurationConfig,
    pub slide: DurationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitTransformConfig {
    pub key_field: Option<String>,
    pub max_rate: u64,
    pub window: DurationConfig,
    #[serde(default)]
    pub on_exceed: RateLimitAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RateLimitAction {
    #[default]
    Drop,
    Delay,
    Error,
    Tag,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateTransformConfig {
    pub group_by: Vec<String>,
    pub window: WindowConfig,
    pub aggregations: Vec<AggregationConfig>,
    #[serde(default)]
    pub emit: AggregateEmit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregationConfig {
    pub field: String,
    pub function: AggregateFunction,
    #[serde(default)]
    pub output_field: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    First,
    Last,
    CountDistinct,
    Collect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateEmit {
    #[default]
    OnWindowClose,
    OnUpdate,
    Periodic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinTransformConfig {
    pub join_type: JoinType,
    pub right_stream: String,
    pub on: JoinCondition,
    pub window: WindowConfig,
    #[serde(default)]
    pub output_fields: Option<JoinOutputFields>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinCondition {
    pub left_key: String,
    pub right_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinOutputFields {
    #[serde(default)]
    pub left_prefix: Option<String>,
    #[serde(default)]
    pub right_prefix: Option<String>,
    #[serde(default)]
    pub include: Option<Vec<String>>,
    #[serde(default)]
    pub exclude: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionizeTransformConfig {
    pub key_field: String,
    pub gap: DurationConfig,
    #[serde(default)]
    pub max_duration: Option<DurationConfig>,
    #[serde(default)]
    pub emit: SessionEmit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionEmit {
    #[default]
    OnClose,
    OnEvent,
    Both,
}

// ============================================================================
// SINK CONFIGURATIONS
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "sink_type", rename_all = "lowercase")]
pub enum SinkConfigDsl {
    Grpc(GrpcSinkConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcSinkConfig {
    pub endpoint: String,
    #[serde(default)]
    pub tls: Option<TlsConfig>,
    #[serde(default)]
    pub max_message_size: Option<usize>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(default)]
    pub retry: Option<RetryConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryConfig {
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    #[serde(default = "default_backoff_multiplier")]
    pub backoff_multiplier: f64,
}

fn default_max_retries() -> u32 {
    3
}

fn default_initial_backoff_ms() -> u64 {
    100
}

fn default_max_backoff_ms() -> u64 {
    10000
}

fn default_backoff_multiplier() -> f64 {
    2.0
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: default_max_retries(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            backoff_multiplier: default_backoff_multiplier(),
        }
    }
}

// ============================================================================
// LOOKUP CONFIGURATIONS
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupConfigDsl {
    pub keys: Vec<LookupKeyConfigDsl>,
    #[serde(default)]
    pub output_prefix: Option<String>,
    #[serde(default)]
    pub merge_strategy: MergeStrategyDsl,
    #[serde(default)]
    pub on_miss: LookupOnMissDsl,
    #[serde(default)]
    pub timeout_ms: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupKeyConfigDsl {
    pub record_field: String,
    #[serde(default)]
    pub lookup_key: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategyDsl {
    #[default]
    Merge,
    Nest,
    Replace,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LookupOnMissDsl {
    #[default]
    PassThrough,
    Drop,
    Error,
}

// ============================================================================
// FAN-IN / FAN-OUT CONFIGURATIONS
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanInSourceDsl {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub service: ServiceSelectorDsl,
    #[serde(default)]
    pub config: Option<StageConfigDsl>,
    #[serde(default)]
    pub watermark: Option<SourceWatermarkDsl>,
    #[serde(default)]
    pub mapping: Option<Vec<FieldMappingDsl>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanOutSinkDsl {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub service: ServiceSelectorDsl,
    #[serde(default)]
    pub config: Option<StageConfigDsl>,
    #[serde(default)]
    pub mapping: Option<Vec<FieldMappingDsl>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceWatermarkDsl {
    pub event_time_field: String,
    #[serde(default)]
    pub idle_timeout: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanInWatermarkDsl {
    #[serde(default)]
    pub allowed_lateness: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldMappingDsl {
    #[serde(default)]
    pub source: Option<String>,
    pub target: String,
    #[serde(default)]
    pub cast: Option<FieldType>,
    #[serde(default)]
    pub default: Option<serde_json::Value>,
    #[serde(default)]
    pub literal: Option<serde_json::Value>,
}

// ============================================================================
// BACKUP / RESTORE MANIFESTS
// ============================================================================

pub const KIND_BACKUP: &str = "Backup";
pub const KIND_RESTORE: &str = "Restore";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupManifest {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: BackupSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackupSpec {
    pub pipeline: PipelineRef,
    #[serde(default)]
    pub include: Vec<BackupComponent>,
    pub destination: BackupDestination,
    #[serde(default)]
    pub schedule: Option<BackupSchedule>,
    #[serde(default)]
    pub options: BackupOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRef {
    pub name: String,
    #[serde(default)]
    pub namespace: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackupComponent {
    Checkpoints,
    Offsets,
    Configuration,
    State,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BackupDestination {
    S3(S3BackupConfig),
    Gcs(GcsBackupConfig),
    File(FileBackupConfig),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3BackupConfig {
    pub bucket: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub credentials: Option<AwsCredentialsConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcsBackupConfig {
    pub bucket: String,
    #[serde(default)]
    pub prefix: Option<String>,
    #[serde(default)]
    pub credentials_file: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileBackupConfig {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupSchedule {
    pub cron: String,
    #[serde(default)]
    pub retention: Option<BackupRetention>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupRetention {
    #[serde(default)]
    pub count: Option<u32>,
    #[serde(default)]
    pub days: Option<u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackupOptions {
    #[serde(default)]
    pub compression: Option<BackupCompressionType>,
    #[serde(default)]
    pub encryption: Option<EncryptionConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackupCompressionType {
    None,
    Gzip,
    Zstd,
    Lz4,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub kms_key_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreManifest {
    pub api_version: String,
    pub kind: String,
    pub metadata: ObjectMeta,
    pub spec: RestoreSpec,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreSpec {
    pub from: RestoreSource,
    #[serde(default)]
    pub target: Option<PipelineRef>,
    #[serde(default)]
    pub include: Vec<BackupComponent>,
    #[serde(default)]
    pub options: RestoreOptions,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreSource {
    #[serde(default)]
    pub backup: Option<String>,
    #[serde(default)]
    pub snapshot: Option<String>,
    #[serde(default)]
    pub latest: Option<bool>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RestoreOptions {
    #[serde(default)]
    pub validate_before_restore: bool,
    #[serde(default)]
    pub pause_pipeline: bool,
    #[serde(default)]
    pub resume_after_restore: bool,
}
