use std::time::Duration;
use anyhow::{Result, Context};
use tonic::transport::Channel;
use tonic_reflection::pb::v1::{
    server_reflection_client::ServerReflectionClient,
    server_reflection_request::MessageRequest,
    server_reflection_response::MessageResponse,
    ServerReflectionRequest,
};
use tokio_stream::StreamExt;
use tracing::{debug, warn, info};

use super::{LocalService, LocalServiceRegistry, ServiceType};

pub struct GrpcReflectionDiscovery {
    ports: Vec<u16>,
    timeout: Duration,
}

impl GrpcReflectionDiscovery {
    pub fn new(ports: Vec<u16>) -> Self {
        Self {
            ports,
            timeout: Duration::from_secs(2),
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub async fn discover(&self) -> Result<LocalServiceRegistry> {
        let mut registry = LocalServiceRegistry::new();

        for port in &self.ports {
            match self.probe_port(*port).await {
                Ok(services) => {
                    for service in services {
                        info!(
                            "Discovered local service: {} ({:?}) at {}",
                            service.name, service.service_type, service.endpoint
                        );
                        registry.register(service);
                    }
                }
                Err(e) => {
                    warn!("Failed to probe port {}: {}", port, e);
                }
            }
        }

        Ok(registry)
    }

    async fn probe_port(&self, port: u16) -> Result<Vec<LocalService>> {
        let endpoint = format!("http://127.0.0.1:{}", port);
        debug!("Probing {} for gRPC services", endpoint);

        let channel = Channel::from_shared(endpoint.clone())
            .context("Invalid endpoint")?
            .connect_timeout(self.timeout)
            .timeout(self.timeout)
            .connect()
            .await
            .context("Failed to connect")?;

        let mut client = ServerReflectionClient::new(channel);

        let request = ServerReflectionRequest {
            host: String::new(),
            message_request: Some(MessageRequest::ListServices(String::new())),
        };

        let request_stream = tokio_stream::once(request);
        let mut response_stream = client
            .server_reflection_info(request_stream)
            .await
            .context("Reflection call failed")?
            .into_inner();

        let mut services = Vec::new();

        while let Some(response) = response_stream.next().await {
            let response = response.context("Stream error")?;

            if let Some(MessageResponse::ListServicesResponse(list)) = response.message_response {
                for svc in list.service {
                    let service_name = &svc.name;

                    if service_name.starts_with("grpc.") {
                        continue;
                    }

                    let service_type = ServiceType::from_proto_service(service_name);

                    if service_type == ServiceType::Unknown {
                        debug!("Skipping unknown service type: {}", service_name);
                        continue;
                    }

                    let local_name = extract_service_name(service_name);

                    services.push(LocalService {
                        name: local_name,
                        service_type,
                        endpoint: format!("127.0.0.1:{}", port),
                        proto_service: service_name.clone(),
                    });
                }
            }
        }

        Ok(services)
    }
}

fn extract_service_name(proto_service: &str) -> String {
    proto_service
        .rsplit('.')
        .next()
        .unwrap_or(proto_service)
        .trim_end_matches("Service")
        .to_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_service_name() {
        assert_eq!(extract_service_name("etl.source.SourceService"), "source");
        assert_eq!(extract_service_name("etl.transform.TransformService"), "transform");
        assert_eq!(extract_service_name("etl.sink.SinkService"), "sink");
        assert_eq!(extract_service_name("MyService"), "my");
    }

    #[test]
    fn test_service_type_from_proto() {
        assert_eq!(
            ServiceType::from_proto_service("etl.source.SourceService"),
            ServiceType::Source
        );
        assert_eq!(
            ServiceType::from_proto_service("etl.transform.TransformService"),
            ServiceType::Transform
        );
        assert_eq!(
            ServiceType::from_proto_service("etl.sink.SinkService"),
            ServiceType::Sink
        );
        assert_eq!(
            ServiceType::from_proto_service("grpc.health.v1.Health"),
            ServiceType::Unknown
        );
    }
}
