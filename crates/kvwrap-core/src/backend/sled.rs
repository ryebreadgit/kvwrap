use crate::{Error, KvStore, LocalConfig, Result, WatchEvent, WatchRegistry};
use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{Arc, RwLock},
};

#[derive(Clone)]
pub struct SledStore {
    db: sled::Db,
    trees: Arc<RwLock<HashMap<Vec<u8>, sled::Tree>>>,
    watchers: WatchRegistry,
}

impl SledStore {
    pub fn new(config: LocalConfig) -> Result<Self> {
        let path = Path::new(&config.path);
        if !path.exists() {
            fs::create_dir_all(path)?;
        }

        let db = sled::Config::new()
            .path(path)
            .cache_capacity(config.cache_size as u64)
            .open()?;

        Ok(Self {
            db,
            trees: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    fn get_or_create_tree(&self, name: &[u8]) -> Result<sled::Tree> {
        {
            let cache = self.trees.read().unwrap();
            if let Some(tree) = cache.get(name) {
                return Ok(tree.clone());
            }
        }

        let mut cache = self.trees.write().unwrap();
        if let Some(tree) = cache.get(name) {
            return Ok(tree.clone());
        }

        let name_str = URL_SAFE_NO_PAD.encode(name);
        let tree = self.db.open_tree(name_str.as_bytes())?;
        cache.insert(name.to_vec(), tree.clone());
        Ok(tree)
    }
}

#[async_trait]
impl KvStore for SledStore {
    async fn get(&self, partition: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>> {
        let tree = self.get_or_create_tree(partition)?;
        let key = key.to_vec();
        blocking::unblock(move || {
            tree.get(&key)
                .map(|op| op.map(|v| v.to_vec()))
                .map_err(Error::Sled)
        })
        .await
    }

    async fn set(&self, partition: &[u8], key: &[u8], value: &[u8]) -> Result<()> {
        let tree = self.get_or_create_tree(partition)?;
        let key = key.to_vec();
        let value = value.to_vec();
        let key_for_notify = key.clone();
        let value_for_notify = value.clone();
        blocking::unblock(move || {
            tree.insert(&key, value.as_slice())
                .map(|_| ())
                .map_err(Error::Sled)
        })
        .await?;
        self.watchers.notify(&WatchEvent::Set {
            partition: partition.to_vec(),
            key: key_for_notify,
            value: value_for_notify,
        });
        Ok(())
    }

    async fn delete(&self, partition: &[u8], key: &[u8]) -> Result<()> {
        let tree = self.get_or_create_tree(partition)?;
        let key = key.to_vec();
        let key_for_notify = key.clone();
        blocking::unblock(move || tree.remove(&key).map(|_| ()).map_err(Error::Sled)).await?;
        self.watchers.notify(&WatchEvent::Delete {
            partition: partition.to_vec(),
            key: key_for_notify,
        });
        Ok(())
    }
}
