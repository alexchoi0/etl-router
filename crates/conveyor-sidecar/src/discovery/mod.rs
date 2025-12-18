mod grpc_reflection;
mod local_services;

pub use grpc_reflection::GrpcReflectionDiscovery;
pub use local_services::{LocalServiceRegistry, LocalService, ServiceType};
