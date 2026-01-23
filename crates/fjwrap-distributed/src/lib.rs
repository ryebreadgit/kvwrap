mod proto;
mod server;

use fjwrap_core::KvStore;
use std::net::SocketAddr;
use std::sync::Arc;

pub use proto::*;
pub use server::KvServiceImpl;

pub async fn run_server<S>(store: Arc<S>, addr: SocketAddr) -> Result<(), tonic::transport::Error>
where
    S: KvStore + Send + Sync + 'static,
{
    let kv_service = KvServiceImpl::new(store);

    tracing::info!(%addr, "starting gRPC server");

    tonic::transport::Server::builder()
        .add_service(kv_service_server::KvServiceServer::new(kv_service))
        .serve(addr)
        .await
}
