use crate::routing::ShardRouter;
use kvwrap_core::{Error, KvStore, WatchEvent};
use kvwrap_proto::{
    AllKeysRequest, AllKeysResponse, DeleteRequest, DeleteResponse, GetRequest, GetResponse,
    NodeId, SetRequest, SetResponse, ShardId, WatchEventMessage, WatchRequest,
    kv_service_client::KvServiceClient, kv_service_server::KvService,
    watch_event_message::EventType,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{RwLock, mpsc};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};
use tonic::{Request, Response, Status, transport::Channel};

const PROXIED_HEADER: &str = "x-kvwrap-proxied";
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
        Error::Io(e) => Status::internal(format!("io error: {}", e)),
        Error::SerdeJson(e) => Status::internal(format!("serialization error: {}", e)),
        Error::Network(e) => Status::internal(format!("network error: {}", e)),
        Error::Other(msg) => Status::internal(msg),
        _ => Status::internal(format!("storage error: {}", err)),
    }
}

type WatchStream = ReceiverStream<Result<WatchEventMessage, Status>>;
type AllKeysStream = ReceiverStream<Result<AllKeysResponse, Status>>;

#[tonic::async_trait]
impl<S, R> KvService for KvServiceImpl<S, R>
where
    S: KvStore + Send + Sync + 'static,
    R: ShardRouter + Send + Sync + 'static,
{
    type WatchStream = WatchStream;
    type AllKeysStream = AllKeysStream;
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let is_proxied = Self::is_proxied_request(&request);
        let req = request.into_inner();

        tracing::debug!(
            partition = %req.partition,
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
            partition = &req.partition,
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
            partition = &req.partition,
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

    async fn all_keys(
        &self,
        request: Request<AllKeysRequest>,
    ) -> Result<Response<Self::AllKeysStream>, Status> {
        let is_proxied = Self::is_proxied_request(&request);
        let req = request.into_inner();

        tracing::debug!(
            partition = &req.partition,
            prefix_len = req.prefix.len(),
            proxied = is_proxied,
            "all_keys request"
        );

        let shard_id = self
            .router
            .route(&req.partition, req.prefix.as_slice())
            .map_err(|e| Status::internal(format!("routing error: {}", e)))?;

        if self.owns_shard(&shard_id) {
            let rx = self.store.all_keys(
                &req.partition,
                Some(req.prefix.as_slice()),
                req.buffer_size as usize,
            );

            let (tx, grpc_rx) = mpsc::channel(req.buffer_size as usize);

            tokio::spawn(async move {
                while let Ok(result) = rx.recv().await {
                    let msg = match result {
                        Ok(key) => AllKeysResponse { key },
                        Err(e) => {
                            let _ = tx.send(Err(core_error_to_status(e))).await;
                            break;
                        }
                    };
                    if tx.send(Ok(msg)).await.is_err() {
                        break; // client disconnected
                    }
                }
            });

            return Ok(Response::new(ReceiverStream::new(grpc_rx)));
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
            let mut proxied_request = Request::new(AllKeysRequest {
                partition: req.partition.clone(),
                prefix: req.prefix.clone(),
                buffer_size: req.buffer_size,
            });
            Self::mark_request_as_proxied(&mut proxied_request);
            match client.all_keys(proxied_request).await {
                Ok(response) => {
                    let mut stream = response.into_inner();
                    let (tx, grpc_rx) = mpsc::channel(req.buffer_size as usize);

                    tokio::spawn(async move {
                        while let Some(item) = stream.next().await {
                            if tx.send(item).await.is_err() {
                                break; // client disconnected
                            }
                        }
                    });

                    return Ok(Response::new(ReceiverStream::new(grpc_rx)));
                }
                Err(e) => {
                    tracing::warn!(
                        address = %address,
                        error = %e,
                        "failed to fetch all keys from shard node"
                    );
                }
            }
        }

        Err(Status::internal(
            "failed to fetch all keys from any shard node",
        ))
    }

    async fn watch(
        &self,
        request: Request<WatchRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let _is_proxied = Self::is_proxied_request(&request);
        let req = request.into_inner();

        tracing::debug!(
            partition = &req.partition,
            key_or_prefix_len = req.key_or_prefix.len(),
            is_prefix = req.is_prefix,
            "watch request"
        );

        let shard_id = self
            .router
            .route(&req.partition, &req.key_or_prefix)
            .map_err(|e| Status::internal(format!("routing error: {}", e)))?;

        if !self.owns_shard(&shard_id) {
            // TODO: proxy watch request to shard node
            return Err(Status::failed_precondition(
                "watch must target a node that owns the shard",
            ));
        }

        let buffer = 64;
        let rx = if req.is_prefix {
            self.store
                .watch_prefix(&req.partition, &req.key_or_prefix, buffer)
        } else {
            self.store
                .watch_key(&req.partition, &req.key_or_prefix, buffer)
        };

        let (tx, grpc_rx) = mpsc::channel(buffer);

        tokio::spawn(async move {
            while let Ok(event) = rx.recv().await {
                let msg = watch_event_to_proto(&event);
                if tx.send(Ok(msg)).await.is_err() {
                    break; // client disconnected
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(grpc_rx)))
    }
}

fn watch_event_to_proto(event: &WatchEvent) -> WatchEventMessage {
    match event {
        WatchEvent::Set {
            partition,
            key,
            value,
        } => WatchEventMessage {
            event_type: EventType::Set.into(),
            partition: partition.clone(),
            key: key.clone(),
            value: value.clone(),
        },
        WatchEvent::Delete { partition, key } => WatchEventMessage {
            event_type: EventType::Delete.into(),
            partition: partition.clone(),
            key: key.clone(),
            value: Vec::new(),
        },
    }
}
