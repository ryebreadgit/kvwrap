use crate::config::RemoteConfig;
use async_channel::Receiver;
use async_compat::CompatExt;
use async_trait::async_trait;
use kvwrap_core::{Error, KvStore, Result, WatchEvent};
use kvwrap_proto::{
    DeleteRequest, GetRequest, SetRequest, WatchRequest, kv_service_client::KvServiceClient,
    watch_event_message::EventType,
};
use tonic::{
    Code, Status,
    transport::{Channel, Endpoint},
};

#[derive(Clone)]
pub struct RemoteStore {
    client: KvServiceClient<Channel>,
}

impl RemoteStore {
    pub async fn connect(config: RemoteConfig) -> Result<Self> {
        let mut endpoint: Endpoint = Endpoint::from_shared(config.endpoint)
            .map_err(|e| Error::Other(format!("invalid endpoint: {}", e)))?;

        if let Some(timeout) = config.connect_timeout {
            endpoint = endpoint.connect_timeout(timeout);
        }

        if let Some(timeout) = config.request_timeout {
            endpoint = endpoint.timeout(timeout);
        }

        let channel = endpoint
            .connect()
            .compat()
            .await
            .map_err(|e| Error::Other(format!("connection failed: {}", e)))?;

        Ok(Self {
            client: KvServiceClient::new(channel),
        })
    }

    pub async fn connect_lazy(config: RemoteConfig) -> Result<Self> {
        async {
            let mut endpoint: Endpoint = Endpoint::from_shared(config.endpoint)
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
        .compat()
        .await
    }

    fn start_watch(
        &self,
        partition: &[u8],
        key_or_prefix: &[u8],
        is_prefix: bool,
        buffer: usize,
    ) -> Receiver<WatchEvent> {
        let (tx, rx) = async_channel::bounded(buffer);
        let mut client = self.client.clone();
        let request = WatchRequest {
            partition: partition.to_vec(),
            key_or_prefix: key_or_prefix.to_vec(),
            is_prefix,
        };

        tokio::spawn(async move {
            let stream = match client.watch(request).await {
                Ok(response) => response.into_inner(),
                Err(e) => {
                    tracing::warn!(error = %e, "watch stream failed to start");
                    return;
                }
            };

            Self::run_watch_stream(stream, tx).await;
        });

        rx
    }

    async fn run_watch_stream(
        mut stream: tonic::Streaming<kvwrap_proto::WatchEventMessage>,
        tx: async_channel::Sender<WatchEvent>,
    ) {
        use futures_lite::StreamExt;

        loop {
            match stream.next().await {
                Some(Ok(msg)) => {
                    let event = proto_to_watch_event(msg);
                    if tx.send(event).await.is_err() {
                        break;
                    }
                }
                Some(Err(e)) => {
                    tracing::warn!(error = %e, "watch stream error");
                    break;
                }
                None => break, // stream ended
            }
        }
    }
}

#[async_trait]
impl KvStore for RemoteStore {
    async fn get(&self, partition: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        let request = GetRequest {
            partition: partition.to_vec(),
            key: key.to_vec(),
        };
        let mut client = self.client.clone();
        let response = client
            .get(request)
            .compat()
            .await
            .map_err(status_to_core_error)?;
        Ok(response.into_inner().value)
    }

    async fn set(&self, partition: &[u8], key: &[u8], value: &[u8]) -> Result<()> {
        let request = SetRequest {
            partition: partition.to_vec(),
            key: key.to_vec(),
            value: value.to_vec(),
        };
        let mut client = self.client.clone();
        client
            .set(request)
            .compat()
            .await
            .map_err(status_to_core_error)?;
        Ok(())
    }

    async fn delete(&self, partition: &[u8], key: &[u8]) -> Result<()> {
        let request = DeleteRequest {
            partition: partition.to_vec(),
            key: key.to_vec(),
        };
        let mut client = self.client.clone();
        client
            .delete(request)
            .compat()
            .await
            .map_err(status_to_core_error)?;
        Ok(())
    }

    fn watch_key(&self, partition: &[u8], key: &[u8], buffer: usize) -> Receiver<WatchEvent> {
        self.start_watch(partition, key, false, buffer)
    }

    fn watch_prefix(&self, partition: &[u8], prefix: &[u8], buffer: usize) -> Receiver<WatchEvent> {
        self.start_watch(partition, prefix, true, buffer)
    }
}

fn status_to_core_error(status: Status) -> Error {
    match status.code() {
        Code::NotFound => Error::KeyNotFound,
        Code::Unavailable => Error::Network(status.message().to_string()),
        Code::DeadlineExceeded => Error::Network("request timed out".to_string()),
        _ => Error::Other(format!(
            "rpc error ({}): {}",
            status.code(),
            status.message()
        )),
    }
}

fn proto_to_watch_event(msg: kvwrap_proto::WatchEventMessage) -> WatchEvent {
    match EventType::try_from(msg.event_type) {
        Ok(EventType::Delete) => WatchEvent::Delete {
            partition: msg.partition,
            key: msg.key,
        },
        _ => WatchEvent::Set {
            partition: msg.partition,
            key: msg.key,
            value: msg.value,
        },
    }
}
