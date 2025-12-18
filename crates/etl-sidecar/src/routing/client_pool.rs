use anyhow::{Context, Result};
use dashmap::DashMap;
use tonic::transport::Channel;
use tracing::debug;

pub struct ClientPool<T: Clone> {
    clients: DashMap<String, T>,
}

impl<T: Clone> ClientPool<T> {
    pub fn new() -> Self {
        Self {
            clients: DashMap::new(),
        }
    }

    pub async fn get_or_create<F>(&self, endpoint: &str, create: F) -> Result<T>
    where
        F: FnOnce(Channel) -> T,
    {
        if let Some(client) = self.clients.get(endpoint) {
            return Ok(client.clone());
        }

        debug!("Creating new connection to {}", endpoint);

        let channel = Channel::from_shared(format!("http://{}", endpoint))
            .context("Invalid endpoint")?
            .connect()
            .await
            .context("Failed to connect")?;

        let client = create(channel);

        self.clients.insert(endpoint.to_string(), client.clone());

        Ok(client)
    }

    pub fn remove(&self, endpoint: &str) {
        if self.clients.remove(endpoint).is_some() {
            debug!("Removed client for {}", endpoint);
        }
    }

    pub fn clear(&self) {
        self.clients.clear();
    }
}

impl<T: Clone> Default for ClientPool<T> {
    fn default() -> Self {
        Self::new()
    }
}
