use std::net::SocketAddr;
use std::sync::Arc;
use axum::{
    Router,
    routing::get,
    extract::State,
    response::{Html, IntoResponse},
    http::StatusCode,
};
use async_graphql::http::GraphiQLSource;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use tokio::sync::RwLock;
use tower_http::cors::{CorsLayer, Any};
use tracing::info;

use conveyor_raft::RaftNode;
use super::schema::{create_schema, RouterSchema};
use super::error_buffer::ErrorBuffer;

#[derive(Clone)]
pub struct AppState {
    pub schema: RouterSchema,
    pub raft_node: Arc<RwLock<RaftNode>>,
    pub error_buffer: ErrorBuffer,
}

pub struct GraphQLServer {
    addr: SocketAddr,
    state: AppState,
}

impl GraphQLServer {
    pub fn new(addr: SocketAddr, raft_node: Arc<RwLock<RaftNode>>) -> Self {
        let schema = create_schema();
        let error_buffer = ErrorBuffer::new();
        let state = AppState { schema, raft_node, error_buffer };
        Self { addr, state }
    }

    pub fn error_buffer(&self) -> ErrorBuffer {
        self.state.error_buffer.clone()
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let cors = CorsLayer::new()
            .allow_origin(Any)
            .allow_methods(Any)
            .allow_headers(Any);

        let app = Router::new()
            .route("/", get(graphiql).post(graphql_handler))
            .route("/graphql", get(graphiql).post(graphql_handler))
            .route("/health", get(health_check))
            .layer(cors)
            .with_state(self.state);

        info!("Starting GraphQL server on {}", self.addr);
        info!("GraphiQL playground available at http://{}/", self.addr);

        let listener = tokio::net::TcpListener::bind(self.addr).await?;
        axum::serve(listener, app).await?;

        Ok(())
    }
}

async fn graphql_handler(
    State(state): State<AppState>,
    req: GraphQLRequest,
) -> GraphQLResponse {
    let schema = state.schema.clone();
    let raft_node = state.raft_node.clone();
    let error_buffer = state.error_buffer.clone();

    schema
        .execute(req.into_inner().data(raft_node).data(error_buffer))
        .await
        .into()
}

async fn graphiql() -> impl IntoResponse {
    Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

async fn health_check(State(state): State<AppState>) -> impl IntoResponse {
    let node = state.raft_node.read().await;
    let is_leader = node.is_leader().await;
    let leader_id = node.leader_id().await;

    let status = serde_json::json!({
        "status": "ok",
        "node_id": node.id,
        "is_leader": is_leader,
        "leader_id": leader_id,
    });

    (StatusCode::OK, axum::Json(status))
}
