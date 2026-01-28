use crate::routing::ShardRouter;
use fjwrap_core::{Error, KvStore};
use fjwrap_proto::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, NodeId, SetRequest, SetResponse,
    ShardId, kv_service_client::KvServiceClient, kv_service_server::KvService,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};

const PROXIED_HEADER: &str = "x-fjwrap-proxied";
pub struct KvServiceImpl<S, R> {
    node_id: NodeId,
    store: Arc<S>,
    router: Arc<R>,
    local_shard_nodes: Vec<ShardId>,
    clients: Arc<RwLock<HashMap<String, KvServiceClient<Channel>>>>,
}

impl<S, R> KvServiceImpl<S, R> {
    pub fn new(store: Arc<S>, router: Arc<R>, node_id: NodeId) -> Self
    where
        R: ShardRouter,
    {
        let local_shard_nodes = match router.node_shard_ids(&node_id) {
            Ok(shard_ids) => shard_ids,
            Err(_) => Vec::new(),
        };

        Self {
            node_id,
            store,
            router,
            local_shard_nodes,
            clients: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn owns_shard(&self, shard_id: &ShardId) -> bool {
        self.local_shard_nodes.iter().any(|s| s == shard_id)
    }

    async fn get_client(&self, address: &str) -> Result<KvServiceClient<Channel>, Status> {
        {
            let clients_read = self.clients.read().await;
            if let Some(client) = clients_read.get(address) {
                return Ok(client.clone());
            }
        }

        let endpoint = format!("http://{}", address);
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| Status::internal(format!("invalid address: {}", e)))?
            .connect()
            .await
            .map_err(|e| Status::internal(format!("failed to connect: {}", e)))?;
        let client = KvServiceClient::new(channel);

        {
            let mut clients_write = self.clients.write().await;
            clients_write.insert(address.to_string(), client.clone());
        }
        Ok(client)
    }

    fn is_proxied_request(request: &Request<impl Sized>) -> bool {
        request
            .metadata()
            .get(PROXIED_HEADER)
            .map_or(false, |v| v == "true")
    }

    fn mark_request_as_proxied(request: &mut Request<impl Sized>) {
        request
            .metadata_mut()
            .insert(PROXIED_HEADER, "true".parse().unwrap());
    }
}

fn core_error_to_status(err: Error) -> Status {
    match err {
        Error::KeyNotFound => Status::not_found("key not found"),
        Error::Storage(e) => Status::internal(format!("storage error: {}", e)),
        Error::Io(e) => Status::internal(format!("io error: {}", e)),
        Error::SerdeJson(e) => Status::internal(format!("serialization error: {}", e)),
        Error::Network(e) => Status::internal(format!("network error: {}", e)),
        Error::Other(msg) => Status::internal(msg),
    }
}

#[tonic::async_trait]
impl<S, R> KvService for KvServiceImpl<S, R>
where
    S: KvStore + Send + Sync + 'static,
    R: ShardRouter + Send + Sync + 'static,
{
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let is_proxied = Self::is_proxied_request(&request);
        let req = request.into_inner();

        tracing::debug!(
            partition = %String::from_utf8_lossy(&req.partition),
            key_len = req.key.len(),
            proxied = is_proxied,
            "get request"
        );

        let shard_id = self
            .router
            .route(&req.partition, &req.key)
            .map_err(|e| Status::internal(format!("routing error: {}", e)))?;

        if self.owns_shard(&shard_id) {
            let result = self
                .store
                .get(&req.partition, &req.key)
                .await
                .map_err(core_error_to_status)?;

            return Ok(Response::new(GetResponse { value: result }));
        }

        if is_proxied {
            return Err(Status::internal("requested shard not owned by this node"));
        }

        let shard_nodes = self
            .router
            .shard_nodes(shard_id)
            .map_err(|e| Status::internal(format!("routing error: {}", e)))?;

        for node in shard_nodes {
            let address = match node.address {
                None => continue,
                Some(address) => address,
            };
            let address = format!("{}:{}", address.host, address.port);
            let mut client = self.get_client(&address).await?;
            let mut proxied_request = Request::new(GetRequest {
                partition: req.partition.clone(),
                key: req.key.clone(),
            });
            Self::mark_request_as_proxied(&mut proxied_request);
            match client.get(proxied_request).await {
                Ok(response) => {
                    return Ok(Response::new(GetResponse {
                        value: response.into_inner().value,
                    }));
                }
                Err(e) => {
                    tracing::warn!(
                        address = %address,
                        error = %e,
                        "failed to get value from shard node"
                    );
                }
            }
        }
        Err(Status::internal("failed to get value from any shard node"))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let is_proxied = Self::is_proxied_request(&request);
        let req = request.into_inner();

        tracing::debug!(
            partition = %String::from_utf8_lossy(&req.partition),
            key_len = req.key.len(),
            value_len = req.value.len(),
            proxied = is_proxied,
            "set request"
        );

        let shard_id = self
            .router
            .route(&req.partition, &req.key)
            .map_err(|e| Status::internal(format!("routing error: {}", e)))?;

        if self.owns_shard(&shard_id) {
            self.store
                .set(&req.partition, &req.key, &req.value)
                .await
                .map_err(core_error_to_status)?;

            return Ok(Response::new(SetResponse {}));
        }

        if is_proxied {
            return Err(Status::internal("requested shard not owned by this node"));
        }

        let shard_nodes = self
            .router
            .shard_nodes(shard_id)
            .map_err(|e| Status::internal(format!("routing error: {}", e)))?;

        for node in shard_nodes {
            let address = match node.address {
                None => continue,
                Some(address) => address,
            };
            let address = format!("{}:{}", address.host, address.port);
            let mut client = self.get_client(&address).await?;
            let mut proxied_request = Request::new(SetRequest {
                partition: req.partition.clone(),
                key: req.key.clone(),
                value: req.value.clone(),
            });
            Self::mark_request_as_proxied(&mut proxied_request);
            match client.set(proxied_request).await {
                Ok(response) => {
                    return Ok(response);
                }
                Err(e) => {
                    tracing::warn!(
                        address = %address,
                        error = %e,
                        "failed to set value on shard node"
                    );
                }
            }
        }

        Err(Status::internal("failed to set value on any shard node"))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let is_proxied = Self::is_proxied_request(&request);
        let req = request.into_inner();

        tracing::debug!(
            partition = %String::from_utf8_lossy(&req.partition),
            key_len = req.key.len(),
            proxied = is_proxied,
            "delete request"
        );

        let shard_id = self
            .router
            .route(&req.partition, &req.key)
            .map_err(|e| Status::internal(format!("routing error: {}", e)))?;

        if self.owns_shard(&shard_id) {
            self.store
                .delete(&req.partition, &req.key)
                .await
                .map_err(core_error_to_status)?;

            return Ok(Response::new(DeleteResponse {}));
        }

        if is_proxied {
            return Err(Status::internal("requested shard not owned by this node"));
        }

        let shard_nodes = self
            .router
            .shard_nodes(shard_id)
            .map_err(|e| Status::internal(format!("routing error: {}", e)))?;

        for node in shard_nodes {
            let address = match node.address {
                None => continue,
                Some(address) => address,
            };
            let address = format!("{}:{}", address.host, address.port);
            let mut client = self.get_client(&address).await?;
            let mut proxied_request = Request::new(DeleteRequest {
                partition: req.partition.clone(),
                key: req.key.clone(),
            });
            Self::mark_request_as_proxied(&mut proxied_request);
            match client.delete(proxied_request).await {
                Ok(response) => {
                    return Ok(response);
                }
                Err(e) => {
                    tracing::warn!(
                        address = %address,
                        error = %e,
                        "failed to delete value on shard node"
                    );
                }
            }
        }

        Err(Status::internal("failed to delete value on any shard node"))
    }
}
