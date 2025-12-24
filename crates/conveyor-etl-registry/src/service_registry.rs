use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, RwLock};
use tracing::{info, warn};

use conveyor_etl_raft::{ConveyorRaft, RouterState};

#[derive(Debug, Clone)]
pub enum ServiceEvent {
    Registered {
        service_id: String,
        service_name: String,
        service_type: ServiceType,
        endpoint: String,
    },
    Deregistered {
        service_id: String,
    },
    HealthChanged {
        service_id: String,
        old_health: ServiceHealth,
        new_health: ServiceHealth,
    },
    EndpointChanged {
        service_id: String,
        old_endpoint: String,
        new_endpoint: String,
    },
    LabelsUpdated {
        service_id: String,
    },
}

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
        use conveyor_etl_proto::common::ServiceType as ProtoServiceType;
        match ProtoServiceType::try_from(proto) {
            Ok(ProtoServiceType::Source) => ServiceType::Source,
            Ok(ProtoServiceType::Transform) => ServiceType::Transform,
            Ok(ProtoServiceType::Sink) => ServiceType::Sink,
            _ => ServiceType::Source,
        }
    }

    pub fn to_proto(self) -> i32 {
        use conveyor_etl_proto::common::ServiceType as ProtoServiceType;
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
    raft: Arc<ConveyorRaft>,
    #[allow(dead_code)]
    state: Arc<RwLock<RouterState>>,
    services: DashMap<String, RegisteredService>,
    by_name: DashMap<String, Vec<String>>,
    by_group: DashMap<String, Vec<String>>,
    default_lease_duration: Duration,
    event_tx: broadcast::Sender<ServiceEvent>,
}

impl ServiceRegistry {
    pub fn new(raft: Arc<ConveyorRaft>, state: Arc<RwLock<RouterState>>) -> Self {
        let (event_tx, _) = broadcast::channel(256);
        Self {
            raft,
            state,
            services: DashMap::new(),
            by_name: DashMap::new(),
            by_group: DashMap::new(),
            default_lease_duration: Duration::from_secs(30),
            event_tx,
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<ServiceEvent> {
        self.event_tx.subscribe()
    }

    fn emit_event(&self, event: ServiceEvent) {
        let _ = self.event_tx.send(event);
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
            endpoint: endpoint.clone(),
            labels,
            health: ServiceHealth::Healthy,
            group_id: group_id.clone(),
            registered_at: Some(Instant::now()),
            last_heartbeat: Some(Instant::now()),
            lease_duration: self.default_lease_duration,
        };

        self.services.insert(service_id.clone(), service);

        self.by_name
            .entry(service_name.clone())
            .or_insert_with(Vec::new)
            .push(service_id.clone());

        if let Some(gid) = group_id {
            self.by_group
                .entry(gid)
                .or_insert_with(Vec::new)
                .push(service_id.clone());
        }

        info!("Service registered");

        self.emit_event(ServiceEvent::Registered {
            service_id,
            service_name,
            service_type,
            endpoint,
        });

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

            self.emit_event(ServiceEvent::Deregistered {
                service_id: service_id.to_string(),
            });
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
            let old_health = service.health;
            if old_health != health {
                service.health = health;
                drop(service);
                self.emit_event(ServiceEvent::HealthChanged {
                    service_id: service_id.to_string(),
                    old_health,
                    new_health: health,
                });
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("Service not found: {}", service_id))
        }
    }

    pub async fn update_labels(
        &self,
        service_id: &str,
        labels: HashMap<String, String>,
    ) -> Result<()> {
        if let Some(mut service) = self.services.get_mut(service_id) {
            service.labels = labels;
            drop(service);
            self.emit_event(ServiceEvent::LabelsUpdated {
                service_id: service_id.to_string(),
            });
            Ok(())
        } else {
            Err(anyhow::anyhow!("Service not found: {}", service_id))
        }
    }

    pub async fn update_endpoint(&self, service_id: &str, endpoint: String) -> Result<()> {
        if let Some(mut service) = self.services.get_mut(service_id) {
            let old_endpoint = service.endpoint.clone();
            if old_endpoint != endpoint {
                service.endpoint = endpoint.clone();
                drop(service);
                self.emit_event(ServiceEvent::EndpointChanged {
                    service_id: service_id.to_string(),
                    old_endpoint,
                    new_endpoint: endpoint,
                });
            }
            Ok(())
        } else {
            Err(anyhow::anyhow!("Service not found: {}", service_id))
        }
    }

    pub async fn add_label(&self, service_id: &str, key: String, value: String) -> Result<()> {
        if let Some(mut service) = self.services.get_mut(service_id) {
            service.labels.insert(key, value);
            drop(service);
            self.emit_event(ServiceEvent::LabelsUpdated {
                service_id: service_id.to_string(),
            });
            Ok(())
        } else {
            Err(anyhow::anyhow!("Service not found: {}", service_id))
        }
    }

    pub async fn remove_label(&self, service_id: &str, key: &str) -> Result<Option<String>> {
        if let Some(mut service) = self.services.get_mut(service_id) {
            let removed = service.labels.remove(key);
            drop(service);
            self.emit_event(ServiceEvent::LabelsUpdated {
                service_id: service_id.to_string(),
            });
            Ok(removed)
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
        let expired: Vec<String> = self
            .services
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
