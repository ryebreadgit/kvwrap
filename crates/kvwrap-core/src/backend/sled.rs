use crate::{Error, KvStore, LocalConfig, Result, WatchEvent, WatchRegistry};
use async_channel::Receiver;
use async_trait::async_trait;
use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{Arc, RwLock},
    thread,
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
            watchers: WatchRegistry::default(),
        })
    }

    fn get_or_create_tree(&self, name: &str) -> Result<sled::Tree> {
        {
            let cache = self.trees.read().unwrap();
            if let Some(tree) = cache.get(name.as_bytes()) {
                return Ok(tree.clone());
            }
        }

        let mut cache = self.trees.write().unwrap();
        if let Some(tree) = cache.get(name.as_bytes()) {
            return Ok(tree.clone());
        }

        let tree = self.db.open_tree(name.as_bytes())?;
        cache.insert(name.as_bytes().to_vec(), tree.clone());
        Ok(tree)
    }
}

#[async_trait]
impl KvStore for SledStore {
    async fn get(&self, partition: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let tree = self.get_or_create_tree(partition)?;
        let key = key.to_vec();
        blocking::unblock(move || {
            tree.get(&key)
                .map(|op| op.map(|v| v.to_vec()))
                .map_err(Error::Sled)
        })
        .await
    }

    async fn set(&self, partition: &str, key: &[u8], value: &[u8]) -> Result<()> {
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
            partition: partition.to_string(),
            key: key_for_notify,
            value: value_for_notify,
        });
        Ok(())
    }

    async fn delete(&self, partition: &str, key: &[u8]) -> Result<()> {
        let tree = self.get_or_create_tree(partition)?;
        let key = key.to_vec();
        let key_for_notify = key.clone();
        blocking::unblock(move || tree.remove(&key).map(|_| ()).map_err(Error::Sled)).await?;
        self.watchers.notify(&WatchEvent::Delete {
            partition: partition.to_string(),
            key: key_for_notify,
        });
        Ok(())
    }

    fn scan(
        &self,
        partition: &str,
        prefix: Option<&[u8]>,
        buffer: usize,
    ) -> Receiver<Result<(Vec<u8>, Vec<u8>)>> {
        let (tx, rx) = async_channel::bounded(buffer);

        let tree = match self.get_or_create_tree(partition) {
            Ok(t) => t,
            Err(e) => {
                let _ = tx.try_send(Err(e));
                return rx;
            }
        };

        let prefix = prefix.map(|p| p.to_vec());

        thread::spawn(move || {
            let iter = match prefix {
                Some(ref p) => tree.scan_prefix(p),
                None => tree.iter(),
            };

            for res in iter {
                let item = res
                    .map(|(k, v)| (k.to_vec(), v.to_vec()))
                    .map_err(Error::Sled);
                if tx.send_blocking(item).is_err() {
                    break; // Receiver dropped
                }
            }
        });

        rx
    }

    fn watch_key(&self, partition: &str, key: &[u8], buffer: usize) -> Receiver<WatchEvent> {
        self.watchers.subscribe_key(partition, key, buffer)
    }

    fn watch_prefix(&self, partition: &str, prefix: &[u8], buffer: usize) -> Receiver<WatchEvent> {
        self.watchers.subscribe_prefix(partition, prefix, buffer)
    }
}
