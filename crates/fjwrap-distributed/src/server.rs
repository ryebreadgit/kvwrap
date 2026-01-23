use crate::proto::kv_service_server::KvService;
use crate::proto::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, SetRequest, SetResponse,
};
use fjwrap_core::{Error as CoreError, KvStore};
use std::sync::Arc;
use tonic::{Request, Response, Status};

/// gRPC service implementation wrapping any KvStore
pub struct KvServiceImpl<S> {
    db: Arc<S>,
}

impl<S> KvServiceImpl<S> {
    pub fn new(db: Arc<S>) -> Self {
        Self { db }
    }
}

fn core_error_to_status(err: CoreError) -> Status {
    match err {
        CoreError::KeyNotFound => Status::not_found("key not found"),
        CoreError::Storage(e) => Status::internal(format!("storage error: {}", e)),
        CoreError::Io(e) => Status::internal(format!("io error: {}", e)),
        CoreError::SerdeJson(e) => Status::internal(format!("serialization error: {}", e)),
        CoreError::Other(msg) => Status::internal(msg),
    }
}

#[tonic::async_trait]
impl<S> KvService for KvServiceImpl<S>
where
    S: KvStore + Send + Sync + 'static,
{
    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();

        tracing::debug!(
            partition = %req.partition,
            key_len = req.key.len(),
            "get request"
        );

        let result = self
            .db
            .get(&req.partition, &req.key)
            .await
            .map_err(core_error_to_status)?;

        Ok(Response::new(GetResponse { value: result }))
    }

    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();

        tracing::debug!(
            partition = %req.partition,
            key_len = req.key.len(),
            value_len = req.value.len(),
            "set request"
        );

        self.db
            .set(&req.partition, &req.key, &req.value)
            .await
            .map_err(core_error_to_status)?;

        Ok(Response::new(SetResponse {}))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();

        tracing::debug!(
            partition = %req.partition,
            key_len = req.key.len(),
            "delete request"
        );

        self.db
            .delete(&req.partition, &req.key)
            .await
            .map_err(core_error_to_status)?;

        Ok(Response::new(DeleteResponse {}))
    }
}
