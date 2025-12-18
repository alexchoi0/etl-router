use std::net::SocketAddr;
use anyhow::{Result, Context};

#[derive(Debug, Clone)]
pub struct SidecarConfig {
    pub sidecar_id: String,
    pub pod_name: String,
    pub namespace: String,
    pub node_name: Option<String>,
    pub local_ports: Vec<u16>,
    pub cluster_endpoint: String,
    pub listen_addr: SocketAddr,
    pub pod_ip: String,
}

impl SidecarConfig {
    pub fn from_env() -> Result<Self> {
        let pod_name = std::env::var("CONVEYOR_POD_NAME")
            .unwrap_or_else(|_| hostname());

        let namespace = std::env::var("CONVEYOR_NAMESPACE")
            .unwrap_or_else(|_| "default".to_string());

        let node_name = std::env::var("CONVEYOR_NODE_NAME").ok();

        let local_ports = parse_ports(
            &std::env::var("CONVEYOR_LOCAL_PORTS").unwrap_or_default()
        );

        let cluster_endpoint = std::env::var("CONVEYOR_CLUSTER_ENDPOINT")
            .context("CONVEYOR_CLUSTER_ENDPOINT must be set")?;

        let listen_addr: SocketAddr = std::env::var("CONVEYOR_SIDECAR_LISTEN")
            .unwrap_or_else(|_| "0.0.0.0:50053".to_string())
            .parse()
            .context("Invalid CONVEYOR_SIDECAR_LISTEN address")?;

        let pod_ip = std::env::var("CONVEYOR_POD_IP")
            .or_else(|_| get_pod_ip())
            .unwrap_or_else(|_| "127.0.0.1".to_string());

        let sidecar_id = format!("{}/{}", namespace, pod_name);

        Ok(Self {
            sidecar_id,
            pod_name,
            namespace,
            node_name,
            local_ports,
            cluster_endpoint,
            listen_addr,
            pod_ip,
        })
    }

    pub fn sidecar_endpoint(&self) -> String {
        format!("{}:{}", self.pod_ip, self.listen_addr.port())
    }
}

fn parse_ports(s: &str) -> Vec<u16> {
    s.split(',')
        .filter_map(|p| p.trim().parse().ok())
        .collect()
}

fn hostname() -> String {
    std::env::var("HOSTNAME")
        .unwrap_or_else(|_| "unknown".to_string())
}

fn get_pod_ip() -> Result<String> {
    use std::net::UdpSocket;
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect("8.8.8.8:80")?;
    let addr = socket.local_addr()?;
    Ok(addr.ip().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ports() {
        assert_eq!(parse_ports("8080,8081,8082"), vec![8080, 8081, 8082]);
        assert_eq!(parse_ports("8080, 8081, 8082"), vec![8080, 8081, 8082]);
        assert_eq!(parse_ports(""), Vec::<u16>::new());
        assert_eq!(parse_ports("invalid"), Vec::<u16>::new());
        assert_eq!(parse_ports("8080,invalid,8082"), vec![8080, 8082]);
    }
}
