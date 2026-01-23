use crate::config::RemoteConfig;
use async_trait::async_trait;
use fjwrap_core::{Error, KvStore, Result};
use fjwrap_proto::{DeleteRequest, GetRequest, SetRequest, kv_service_client::KvServiceClient};
use tonic::transport::Channel;

#[derive(Clone)]
pub struct RemoteStore {
    client: KvServiceClient<Channel>,
}

impl RemoteStore {
    pub async fn connect(config: RemoteConfig) -> Result<Self> {
        let mut endpoint: tonic::transport::Endpoint = Channel::from_shared(config.endpoint)
            .map_err(|e| Error::Other(format!("invalid endpoint: {}", e)))?;

        if let Some(timeout) = config.connect_timeout {
            endpoint = endpoint.connect_timeout(timeout);
        }

        if let Some(timeout) = config.request_timeout {
            endpoint = endpoint.timeout(timeout);
        }

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| Error::Other(format!("connection failed: {}", e)))?;

        Ok(Self {
            client: KvServiceClient::new(channel),
        })
    }

    pub fn connect_lazy(config: RemoteConfig) -> Result<Self> {
        let mut endpoint = Channel::from_shared(config.endpoint)
            .map_err(|e| Error::Other(format!("invalid endpoint: {}", e)))?;

        if let Some(timeout) = config.connect_timeout {
            endpoint = endpoint.connect_timeout(timeout);
        }

        if let Some(timeout) = config.request_timeout {
            endpoint = endpoint.timeout(timeout);
        }

        let channel = endpoint.connect_lazy();

        Ok(Self {
            client: KvServiceClient::new(channel),
        })
    }
}

#[async_trait]
impl KvStore for RemoteStore {
    async fn get(&self, partition: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let request = GetRequest {
            partition: partition.to_string(),
            key: key.to_vec(),
        };
        let mut client = self.client.clone();
        let response = client.get(request).await.map_err(status_to_core_error)?;
        Ok(response.into_inner().value)
    }

    async fn set(&self, partition: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let request = SetRequest {
            partition: partition.to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let mut client = self.client.clone();
        client.set(request).await.map_err(status_to_core_error)?;
        Ok(())
    }

    async fn delete(&self, partition: &str, key: &[u8]) -> Result<()> {
        let request = DeleteRequest {
            partition: partition.to_string(),
            key: key.to_vec(),
        };
        let mut client = self.client.clone();
        client.delete(request).await.map_err(status_to_core_error)?;
        Ok(())
    }
}

fn status_to_core_error(status: tonic::Status) -> Error {
    match status.code() {
        tonic::Code::NotFound => Error::KeyNotFound,
        tonic::Code::Unavailable => Error::Network(status.message().to_string()),
        tonic::Code::DeadlineExceeded => Error::Network("request timed out".to_string()),
        _ => Error::Other(format!(
            "rpc error ({}): {}",
            status.code(),
            status.message()
        )),
    }
}
