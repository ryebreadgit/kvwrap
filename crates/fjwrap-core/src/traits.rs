use crate::{Error, Result};
use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use std::sync::Arc;

async fn get_json_impl<S: KvStore + ?Sized, T>(store: &S, partition: &[u8], key: &[u8]) -> Result<T>
where
    T: DeserializeOwned + Send,
{
    match store.get(partition, key).await? {
        Some(bytes) => serde_json::from_slice(&bytes).map_err(Error::SerdeJson),
        None => Err(Error::KeyNotFound),
    }
}

async fn set_json_impl<S: KvStore + ?Sized, T>(
    store: &S,
    partition: &[u8],
    key: &[u8],
    value: &T,
) -> Result<()>
where
    T: Serialize + Sync,
{
    let bytes = serde_json::to_vec(value).map_err(Error::SerdeJson)?;
    store.set(partition, key, &bytes).await
}

#[async_trait]
pub trait KvStore: Send + Sync {
    async fn get(&self, partition: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn set(&self, partition: &[u8], key: &[u8], value: &[u8]) -> Result<()>;
    async fn delete(&self, partition: &[u8], key: &[u8]) -> Result<()>;

    async fn get_json<T>(&self, partition: &[u8], key: &[u8]) -> Result<T>
    where
        T: DeserializeOwned + Send,
        Self: Sized,
    {
        get_json_impl(self, partition, key).await
    }

    async fn set_json<T>(&self, partition: &[u8], key: &[u8], value: &T) -> Result<()>
    where
        T: Serialize + Sync,
        Self: Sized,
    {
        set_json_impl(self, partition, key, value).await
    }
}

#[async_trait]
impl KvStore for Arc<dyn KvStore> {
    async fn get(&self, partition: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        (**self).get(partition, key).await
    }

    async fn set(&self, partition: &[u8], key: &[u8], value: &[u8]) -> Result<()> {
        (**self).set(partition, key, value).await
    }

    async fn delete(&self, partition: &[u8], key: &[u8]) -> Result<()> {
        (**self).delete(partition, key).await
    }

    async fn get_json<T>(&self, partition: &[u8], key: &[u8]) -> Result<T>
    where
        T: DeserializeOwned + Send,
    {
        get_json_impl(self.as_ref(), partition, key).await
    }

    async fn set_json<T>(&self, partition: &[u8], key: &[u8], value: &T) -> Result<()>
    where
        T: Serialize + Sync,
    {
        set_json_impl(self.as_ref(), partition, key, value).await
    }
}
