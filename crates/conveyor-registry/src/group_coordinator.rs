use std::collections::HashMap;
use std::time::Instant;
use dashmap::DashMap;
use anyhow::Result;
use tracing::info;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMember {
    pub service_id: String,
    pub joined_at: u64,
    pub assigned_partitions: Vec<u32>,
    #[serde(skip)]
    pub last_heartbeat: Option<Instant>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionAssignment {
    pub partition_id: u32,
    pub assigned_to: Option<String>,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceGroup {
    pub group_id: String,
    pub stage_id: String,
    pub members: HashMap<String, GroupMember>,
    pub partition_assignment: HashMap<u32, String>,
    pub generation: u64,
    pub total_partitions: u32,
}

impl ServiceGroup {
    pub fn new(group_id: String, stage_id: String, total_partitions: u32) -> Self {
        Self {
            group_id,
            stage_id,
            members: HashMap::new(),
            partition_assignment: HashMap::new(),
            generation: 0,
            total_partitions,
        }
    }
}

#[derive(Debug, Clone)]
pub enum RebalanceEvent {
    PartitionsRevoked {
        service_id: String,
        partitions: Vec<u32>,
        generation: u64,
    },
    PartitionsAssigned {
        service_id: String,
        partitions: Vec<u32>,
        generation: u64,
    },
}

pub struct GroupCoordinator {
    groups: DashMap<String, ServiceGroup>,
}

impl GroupCoordinator {
    pub fn new() -> Self {
        Self {
            groups: DashMap::new(),
        }
    }

    pub async fn create_group(
        &self,
        group_id: String,
        stage_id: String,
        total_partitions: u32,
    ) -> Result<()> {
        if self.groups.contains_key(&group_id) {
            return Err(anyhow::anyhow!("Group already exists: {}", group_id));
        }

        let group = ServiceGroup::new(group_id.clone(), stage_id, total_partitions);
        self.groups.insert(group_id.clone(), group);

        info!(group_id = %group_id, "Group created");
        Ok(())
    }

    pub async fn join_group(
        &self,
        group_id: &str,
        service_id: String,
    ) -> Result<Vec<RebalanceEvent>> {
        let mut group = self.groups
            .get_mut(group_id)
            .ok_or_else(|| anyhow::anyhow!("Group not found: {}", group_id))?;

        if group.members.contains_key(&service_id) {
            return Err(anyhow::anyhow!(
                "Service {} already in group {}",
                service_id,
                group_id
            ));
        }

        let member = GroupMember {
            service_id: service_id.clone(),
            joined_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            assigned_partitions: Vec::new(),
            last_heartbeat: Some(Instant::now()),
        };

        group.members.insert(service_id.clone(), member);

        info!(group_id = %group_id, service_id = %service_id, "Member joined group");

        self.compute_rebalance(&mut group)
    }

    pub async fn leave_group(
        &self,
        group_id: &str,
        service_id: &str,
    ) -> Result<Vec<RebalanceEvent>> {
        let mut group = self.groups
            .get_mut(group_id)
            .ok_or_else(|| anyhow::anyhow!("Group not found: {}", group_id))?;

        if group.members.remove(service_id).is_none() {
            return Err(anyhow::anyhow!(
                "Service {} not in group {}",
                service_id,
                group_id
            ));
        }

        group
            .partition_assignment
            .retain(|_, assigned| assigned != service_id);

        info!(group_id = %group_id, service_id = %service_id, "Member left group");

        self.compute_rebalance(&mut group)
    }

    fn compute_rebalance(&self, group: &mut ServiceGroup) -> Result<Vec<RebalanceEvent>> {
        let mut events = Vec::new();

        let old_assignments: HashMap<String, Vec<u32>> = {
            let mut map: HashMap<String, Vec<u32>> = HashMap::new();
            for (partition, service_id) in &group.partition_assignment {
                map.entry(service_id.clone())
                    .or_insert_with(Vec::new)
                    .push(*partition);
            }
            map
        };

        group.generation += 1;
        let new_generation = group.generation;

        let member_ids: Vec<String> = group.members.keys().cloned().collect();
        let num_members = member_ids.len();

        if num_members == 0 {
            group.partition_assignment.clear();
            for (service_id, partitions) in old_assignments {
                if !partitions.is_empty() {
                    events.push(RebalanceEvent::PartitionsRevoked {
                        service_id,
                        partitions,
                        generation: new_generation,
                    });
                }
            }
            return Ok(events);
        }

        let mut new_assignments: HashMap<String, Vec<u32>> = HashMap::new();
        group.partition_assignment.clear();

        for partition in 0..group.total_partitions {
            let member_idx = partition as usize % num_members;
            let service_id = &member_ids[member_idx];

            group
                .partition_assignment
                .insert(partition, service_id.clone());
            new_assignments
                .entry(service_id.clone())
                .or_insert_with(Vec::new)
                .push(partition);
        }

        for member in group.members.values_mut() {
            member.assigned_partitions = new_assignments
                .get(&member.service_id)
                .cloned()
                .unwrap_or_default();
        }

        for (service_id, old_partitions) in &old_assignments {
            let new_partitions = new_assignments.get(service_id).cloned().unwrap_or_default();
            let revoked: Vec<u32> = old_partitions
                .iter()
                .filter(|p| !new_partitions.contains(p))
                .copied()
                .collect();

            if !revoked.is_empty() {
                events.push(RebalanceEvent::PartitionsRevoked {
                    service_id: service_id.clone(),
                    partitions: revoked,
                    generation: new_generation,
                });
            }
        }

        for (service_id, new_partitions) in &new_assignments {
            let old_partitions = old_assignments.get(service_id).cloned().unwrap_or_default();
            let assigned: Vec<u32> = new_partitions
                .iter()
                .filter(|p| !old_partitions.contains(p))
                .copied()
                .collect();

            if !assigned.is_empty() {
                events.push(RebalanceEvent::PartitionsAssigned {
                    service_id: service_id.clone(),
                    partitions: assigned,
                    generation: new_generation,
                });
            }
        }

        info!(
            group_id = %group.group_id,
            generation = new_generation,
            members = num_members,
            "Rebalance completed"
        );

        Ok(events)
    }

    pub async fn get_assignment(&self, group_id: &str, service_id: &str) -> Option<Vec<u32>> {
        self.groups.get(group_id).and_then(|g| {
            g.members
                .get(service_id)
                .map(|m| m.assigned_partitions.clone())
        })
    }

    pub async fn get_group(&self, group_id: &str) -> Option<ServiceGroup> {
        self.groups.get(group_id).map(|g| g.clone())
    }

    pub async fn get_partition_owner(&self, group_id: &str, partition: u32) -> Option<String> {
        self.groups
            .get(group_id)
            .and_then(|g| g.partition_assignment.get(&partition).cloned())
    }

    pub async fn heartbeat(&self, group_id: &str, service_id: &str) -> Result<()> {
        let mut group = self.groups
            .get_mut(group_id)
            .ok_or_else(|| anyhow::anyhow!("Group not found: {}", group_id))?;

        let member = group
            .members
            .get_mut(service_id)
            .ok_or_else(|| anyhow::anyhow!("Member not found: {}", service_id))?;

        member.last_heartbeat = Some(Instant::now());
        Ok(())
    }

    pub async fn list_groups(&self) -> Vec<String> {
        self.groups.iter().map(|r| r.key().clone()).collect()
    }
}

impl Default for GroupCoordinator {
    fn default() -> Self {
        Self::new()
    }
}
