use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ServiceType {
    Source,
    Transform,
    Sink,
    Unknown,
}

impl ServiceType {
    pub fn from_proto_service(service_name: &str) -> Self {
        if service_name.contains("Source") || service_name.contains("source") {
            ServiceType::Source
        } else if service_name.contains("Transform") || service_name.contains("transform") {
            ServiceType::Transform
        } else if service_name.contains("Sink") || service_name.contains("sink") {
            ServiceType::Sink
        } else {
            ServiceType::Unknown
        }
    }
}

#[derive(Debug, Clone)]
pub struct LocalService {
    pub name: String,
    pub service_type: ServiceType,
    pub endpoint: String,
    pub proto_service: String,
}

#[derive(Debug, Default)]
pub struct LocalServiceRegistry {
    services: HashMap<String, LocalService>,
    by_type: HashMap<ServiceType, Vec<String>>,
}

impl LocalServiceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, service: LocalService) {
        let name = service.name.clone();
        let service_type = service.service_type;

        self.services.insert(name.clone(), service);
        self.by_type
            .entry(service_type)
            .or_default()
            .push(name);
    }

    pub fn get(&self, name: &str) -> Option<&LocalService> {
        self.services.get(name)
    }

    pub fn get_by_type(&self, service_type: ServiceType) -> Vec<&LocalService> {
        self.by_type
            .get(&service_type)
            .map(|names| {
                names
                    .iter()
                    .filter_map(|n| self.services.get(n))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn all_services(&self) -> impl Iterator<Item = &LocalService> {
        self.services.values()
    }

    pub fn has_service(&self, name: &str) -> bool {
        self.services.contains_key(name)
    }

    pub fn sources(&self) -> Vec<&LocalService> {
        self.get_by_type(ServiceType::Source)
    }

    pub fn transforms(&self) -> Vec<&LocalService> {
        self.get_by_type(ServiceType::Transform)
    }

    pub fn sinks(&self) -> Vec<&LocalService> {
        self.get_by_type(ServiceType::Sink)
    }
}
