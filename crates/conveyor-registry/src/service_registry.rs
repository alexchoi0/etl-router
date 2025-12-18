use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
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
    services: Arc<RwLock<HashMap<String, RegisteredService>>>,
    by_name: Arc<RwLock<HashMap<String, Vec<String>>>>,
    by_group: Arc<RwLock<HashMap<String, Vec<String>>>>,
    default_lease_duration: Duration,
}

impl ServiceRegistry {
    pub fn new(raft_node: Arc<RwLock<RaftNode>>) -> Self {
        Self {
            raft_node,
            services: Arc::new(RwLock::new(HashMap::new())),
            by_name: Arc::new(RwLock::new(HashMap::new())),
            by_group: Arc::new(RwLock::new(HashMap::new())),
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

        {
            let mut services = self.services.write().await;
            services.insert(service_id.clone(), service);
        }

        {
            let mut by_name = self.by_name.write().await;
            by_name
                .entry(service_name)
                .or_insert_with(Vec::new)
                .push(service_id.clone());
        }

        if let Some(gid) = group_id {
            let mut by_group = self.by_group.write().await;
            by_group
                .entry(gid)
                .or_insert_with(Vec::new)
                .push(service_id);
        }

        info!("Service registered");

        Ok(self.default_lease_duration)
    }

    pub async fn deregister(&self, service_id: &str) -> Result<()> {
        let service = {
            let mut services = self.services.write().await;
            services.remove(service_id)
        };

        if let Some(svc) = service {
            {
                let mut by_name = self.by_name.write().await;
                if let Some(ids) = by_name.get_mut(&svc.service_name) {
                    ids.retain(|id| id != service_id);
                    if ids.is_empty() {
                        by_name.remove(&svc.service_name);
                    }
                }
            }

            if let Some(gid) = &svc.group_id {
                let mut by_group = self.by_group.write().await;
                if let Some(ids) = by_group.get_mut(gid) {
                    ids.retain(|id| id != service_id);
                    if ids.is_empty() {
                        by_group.remove(gid);
                    }
                }
            }

            info!(service_id = %service_id, "Service deregistered");
        }

        Ok(())
    }

    pub async fn heartbeat(&self, service_id: &str) -> Result<Duration> {
        let mut services = self.services.write().await;

        if let Some(service) = services.get_mut(service_id) {
            service.last_heartbeat = Some(Instant::now());
            Ok(service.lease_duration)
        } else {
            Err(anyhow::anyhow!("Service not found: {}", service_id))
        }
    }

    pub async fn update_health(&self, service_id: &str, health: ServiceHealth) -> Result<()> {
        let mut services = self.services.write().await;

        if let Some(service) = services.get_mut(service_id) {
            service.health = health;
            Ok(())
        } else {
            Err(anyhow::anyhow!("Service not found: {}", service_id))
        }
    }

    pub async fn get_service(&self, service_id: &str) -> Option<RegisteredService> {
        let services = self.services.read().await;
        services.get(service_id).cloned()
    }

    pub async fn get_services_by_name(&self, service_name: &str) -> Vec<RegisteredService> {
        let by_name = self.by_name.read().await;
        let services = self.services.read().await;

        by_name
            .get(service_name)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| services.get(id).cloned())
                    .filter(|s| s.health == ServiceHealth::Healthy && !s.is_lease_expired())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub async fn get_services_by_group(&self, group_id: &str) -> Vec<RegisteredService> {
        let by_group = self.by_group.read().await;
        let services = self.services.read().await;

        by_group
            .get(group_id)
            .map(|ids| {
                ids.iter()
                    .filter_map(|id| services.get(id).cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub async fn get_healthy_services_by_labels(
        &self,
        labels: &HashMap<String, String>,
    ) -> Vec<RegisteredService> {
        let services = self.services.read().await;

        services
            .values()
            .filter(|s| {
                s.health == ServiceHealth::Healthy
                    && !s.is_lease_expired()
                    && labels.iter().all(|(k, v)| s.labels.get(k) == Some(v))
            })
            .cloned()
            .collect()
    }

    pub async fn list_all(&self) -> Vec<RegisteredService> {
        let services = self.services.read().await;
        services.values().cloned().collect()
    }

    pub async fn cleanup_expired(&self) -> Vec<String> {
        let mut expired = Vec::new();

        {
            let services = self.services.read().await;
            for (id, service) in services.iter() {
                if service.is_lease_expired() {
                    expired.push(id.clone());
                }
            }
        }

        for id in &expired {
            if let Err(e) = self.deregister(id).await {
                warn!(service_id = %id, error = %e, "Failed to deregister expired service");
            }
        }

        expired
    }
}
