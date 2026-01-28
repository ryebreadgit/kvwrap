use crate::{Error, Result};
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};

#[async_trait]
pub trait KvStore: Send + Sync {
    async fn get(&self, partition: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn set(&self, partition: &[u8], key: &[u8], value: &[u8]) -> Result<()>;
    async fn delete(&self, partition: &[u8], key: &[u8]) -> Result<()>;
}

#[async_trait]
pub trait KvStoreExt: KvStore {
    async fn get_json<T>(&self, partition: &[u8], key: &[u8]) -> Result<T>
    where
        T: DeserializeOwned + Send;

    async fn set_json<T>(&self, partition: &[u8], key: &[u8], value: &T) -> Result<()>
    where
        T: Serialize + Sync;
}

#[async_trait]
impl<S: KvStore> KvStoreExt for S {
    async fn get_json<T>(&self, partition: &[u8], key: &[u8]) -> Result<T>
    where
        T: DeserializeOwned + Send,
    {
        match self.get(partition, key).await? {
            Some(bytes) => serde_json::from_slice(&bytes).map_err(Error::SerdeJson),
            None => Err(Error::KeyNotFound),
        }
    }

    async fn set_json<T>(&self, partition: &[u8], key: &[u8], value: &T) -> Result<()>
    where
        T: Serialize + Sync,
    {
        let bytes = serde_json::to_vec(value).map_err(Error::SerdeJson)?;
        self.set(partition, key, &bytes).await
    }
}
