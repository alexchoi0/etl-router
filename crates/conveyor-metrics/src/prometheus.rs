use std::net::SocketAddr;
use anyhow::Result;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use tracing::info;

pub struct MetricsExporter {
    handle: PrometheusHandle,
}

impl MetricsExporter {
    pub fn new() -> Result<Self> {
        let builder = PrometheusBuilder::new();
        let handle = builder.install_recorder()?;

        Ok(Self { handle })
    }

    pub fn render(&self) -> String {
        self.handle.render()
    }

    pub async fn serve(self, addr: SocketAddr) -> Result<()> {
        use hyper::{
            server::conn::http1,
            service::service_fn,
            body::Incoming,
            Request, Response,
        };
        use hyper_util::rt::TokioIo;
        use http_body_util::Full;
        use bytes::Bytes;
        use tokio::net::TcpListener;

        let listener = TcpListener::bind(addr).await?;
        info!(addr = %addr, "Metrics server listening");

        loop {
            let (stream, _) = listener.accept().await?;
            let io = TokioIo::new(stream);
            let handle = self.handle.clone();

            tokio::spawn(async move {
                let service = service_fn(move |_req: Request<Incoming>| {
                    let metrics = handle.render();
                    async move {
                        Ok::<_, hyper::Error>(Response::new(Full::new(Bytes::from(metrics))))
                    }
                });

                if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                    tracing::error!(error = %e, "Metrics connection error");
                }
            });
        }
    }
}

impl Default for MetricsExporter {
    fn default() -> Self {
        Self::new().expect("Failed to create metrics exporter")
    }
}
