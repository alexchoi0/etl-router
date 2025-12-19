use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use dashmap::DashMap;
use anyhow::Result;
use tracing::{info, warn};
use serde::{Deserialize, Serialize};

use conveyor_raft::RaftNode;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceHealth {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ServiceType {
    Source,
    Transform,
    Sink,
}

impl ServiceType {
    pub fn from_proto(proto: i32) -> Self {
        use conveyor_proto::common::ServiceType as ProtoServiceType;
        match ProtoServiceType::try_from(proto) {
            Ok(ProtoServiceType::Source) => ServiceType::Source,
            Ok(ProtoServiceType::Transform) => ServiceType::Transform,
            Ok(ProtoServiceType::Sink) => ServiceType::Sink,
            _ => ServiceType::Source,
        }
    }

    pub fn to_proto(self) -> i32 {
        use conveyor_proto::common::ServiceType as ProtoServiceType;
        match self {
            ServiceType::Source => ProtoServiceType::Source as i32,
            ServiceType::Transform => ProtoServiceType::Transform as i32,
            ServiceType::Sink => ProtoServiceType::Sink as i32,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredService {
    pub service_id: String,
    pub service_name: String,
    pub service_type: ServiceType,
    pub endpoint: String,
    pub labels: HashMap<String, String>,
    pub health: ServiceHealth,
    pub group_id: Option<String>,
    #[serde(skip)]
    pub registered_at: Option<Instant>,
    #[serde(skip)]
    pub last_heartbeat: Option<Instant>,
    pub lease_duration: Duration,
}

impl RegisteredService {
    pub fn is_lease_expired(&self) -> bool {
        if let Some(last_hb) = self.last_heartbeat {
            last_hb.elapsed() > self.lease_duration
        } else if let Some(reg_at) = self.registered_at {
            reg_at.elapsed() > self.lease_duration
        } else {
            true
        }
    }
}

pub struct ServiceRegistry {
    #[allow(dead_code)]
    raft_node: Arc<RwLock<RaftNode>>,
    services: DashMap<String, RegisteredService>,
    by_name: DashMap<String, Vec<String>>,
    by_group: DashMap<String, Vec<String>>,
    default_lease_duration: Duration,
}

impl ServiceRegistry {
    pub fn new(raft_node: Arc<RwLock<RaftNode>>) -> Self {
        Self {
            raft_node,
            services: DashMap::new(),
            by_name: DashMap::new(),
            by_group: DashMap::new(),
            default_lease_duration: Duration::from_secs(30),
        }
    }

    pub async fn register(
        &self,
        service_id: String,
        service_name: String,
        service_type: ServiceType,
        endpoint: String,
        labels: HashMap<String, String>,
        group_id: Option<String>,
    ) -> Result<Duration> {
        let service = RegisteredService {
            service_id: service_id.clone(),
            service_name: service_name.clone(),
            service_type,
            endpoint,
            labels,
            health: ServiceHealth::Healthy,
            group_id: group_id.clone(),
            registered_at: Some(Instant::now()),
            last_heartbeat: Some(Instant::now()),
            lease_duration: self.default_lease_duration,
        };

        self.services.insert(service_id.clone(), service);

        self.by_name
            .entry(service_name)
            .or_insert_with(Vec::new)
            .push(service_id.clone());

        if let Some(gid) = group_id {
            self.by_group
                .entry(gid)
                .or_insert_with(Vec::new)
                .push(service_id);
        }

        info!("Service registered");

        Ok(self.default_lease_duration)
    }

    pub async fn deregister(&self, service_id: &str) -> Result<()> {
        let service = self.services.remove(service_id);

        if let Some((_, svc)) = service {
            if let Some(mut ids) = self.by_name.get_mut(&svc.service_name) {
                ids.retain(|id| id != service_id);
                if ids.is_empty() {
                    drop(ids);
                    self.by_name.remove(&svc.service_name);
                }
            }

            if let Some(gid) = &svc.group_id {
                if let Some(mut ids) = self.by_group.get_mut(gid) {
                    ids.retain(|id| id != service_id);
                    if ids.is_empty() {
                        drop(ids);
                        self.by_group.remove(gid);
                    }
                }
            }

            info!(service_id = %service_id, "Service deregistered");
        }

        Ok(())
    }

    pub async fn heartbeat(&self, service_id: &str) -> Result<Duration> {
        if let Some(mut service) = self.services.get_mut(service_id) {
            service.last_heartbeat = Some(Instant::now());
            Ok(service.lease_duration)
        } else {
            Err(anyhow::anyhow!("Service not found: {}", service_id))
        }
    }

    pub async fn update_health(&self, service_id: &str, health: ServiceHealth) -> Result<()> {
        if let Some(mut service) = self.services.get_mut(service_id) {
            service.health = health;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Service not found: {}", service_id))
        }
    }

    pub async fn get_service(&self, service_id: &str) -> Option<RegisteredService> {
        self.services.get(service_id).map(|r| r.clone())
    }

    pub async fn get_services_by_name(&self, service_name: &str) -> Vec<RegisteredService> {
        self.by_name
            .get(service_name)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.services.get(id).map(|r| r.clone()))
                    .filter(|s| s.health == ServiceHealth::Healthy && !s.is_lease_expired())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub async fn get_services_by_group(&self, group_id: &str) -> Vec<RegisteredService> {
        self.by_group
            .get(group_id)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| self.services.get(id).map(|r| r.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub async fn get_healthy_services_by_labels(
        &self,
        labels: &HashMap<String, String>,
    ) -> Vec<RegisteredService> {
        self.services
            .iter()
            .filter(|r| {
                let s = r.value();
                s.health == ServiceHealth::Healthy
                    && !s.is_lease_expired()
                    && labels.iter().all(|(k, v)| s.labels.get(k) == Some(v))
            })
            .map(|r| r.clone())
            .collect()
    }

    pub async fn list_all(&self) -> Vec<RegisteredService> {
        self.services.iter().map(|r| r.clone()).collect()
    }

    pub async fn cleanup_expired(&self) -> Vec<String> {
        let expired: Vec<String> = self.services
            .iter()
            .filter(|r| r.value().is_lease_expired())
            .map(|r| r.key().clone())
            .collect();

        for id in &expired {
            if let Err(e) = self.deregister(id).await {
                warn!(service_id = %id, error = %e, "Failed to deregister expired service");
            }
        }

        expired
    }
}
