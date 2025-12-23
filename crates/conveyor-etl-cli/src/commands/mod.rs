pub mod apply;
pub mod backup;
pub mod delete;
pub mod describe;
pub mod get;
pub mod graph;
pub mod validate;

pub use apply::ApplyArgs;
pub use backup::BackupArgs;
pub use delete::DeleteArgs;
pub use describe::DescribeArgs;
pub use get::GetArgs;
pub use graph::GraphArgs;
pub use validate::ValidateArgs;

#[derive(Clone)]
pub struct Context {
    pub server: String,
    pub namespace: String,
}
