use crate::{Error, KvStore, LocalConfig, Result};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{Arc, RwLock},
};

#[derive(Clone)]
pub struct LocalStore {
    db: Database,
    keyspaces: Arc<RwLock<HashMap<Vec<u8>, Keyspace>>>,
}

impl LocalStore {
    pub fn new(config: LocalConfig) -> Result<Self> {
        let path = Path::new(&config.path);
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        let db = Database::builder(path)
            .cache_size(config.cache_size)
            .open()?;

        Ok(Self {
            db,
            keyspaces: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn get_or_create_keyspace(&self, name: &[u8]) -> Result<Keyspace> {
        {
            let cache = self.keyspaces.read().unwrap();
            if let Some(ks) = cache.get(name) {
                return Ok(ks.clone());
            }
        }

        let mut cache = self.keyspaces.write().unwrap();

        if let Some(ks) = cache.get(name) {
            return Ok(ks.clone());
        }

        let name_str = URL_SAFE_NO_PAD.encode(name); // Encode to make it keyspace name safe
        let ks = self
            .db
            .keyspace(&name_str, || KeyspaceCreateOptions::default())?;
        cache.insert(name.to_vec(), ks.clone());
        Ok(ks)
    }
}

#[async_trait]
impl KvStore for LocalStore {
    async fn get(&self, partition: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        let keyspace = self.get_or_create_keyspace(partition)?;
        let key = key.to_vec();
        blocking::unblock(move || {
            keyspace
                .get(&key)
                .map(|op| op.map(|v| v.to_vec()))
                .map_err(Error::Storage)
        })
        .await
    }
    async fn set(&self, partition: &[u8], key: &[u8], value: &[u8]) -> Result<()> {
        let keyspace = self.get_or_create_keyspace(partition)?;
        let key = key.to_vec();
        let value = value.to_vec();
        blocking::unblock(move || keyspace.insert(&key, &value).map_err(Error::Storage)).await
    }
    async fn delete(&self, partition: &[u8], key: &[u8]) -> Result<()> {
        let keyspace = self.get_or_create_keyspace(partition)?;
        let key = key.to_vec();
        blocking::unblock(move || keyspace.remove(&key).map_err(Error::Storage)).await
    }
}
