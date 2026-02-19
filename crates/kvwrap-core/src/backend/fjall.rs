use crate::{Error, KvStore, LocalConfig, Result, WatchEvent, WatchRegistry};
use async_channel::Receiver;
use async_trait::async_trait;
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use std::{
    collections::HashMap,
    fs,
    path::Path,
    sync::{Arc, RwLock},
    thread,
};

#[derive(Clone)]
pub struct FjallStore {
    db: Database,
    keyspaces: Arc<RwLock<HashMap<String, Keyspace>>>,
    watchers: WatchRegistry,
}

impl FjallStore {
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
            watchers: WatchRegistry::default(),
        })
    }

    fn get_or_create_keyspace(&self, name: &str) -> Result<Keyspace> {
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

        let ks = self
            .db
            .keyspace(name, || KeyspaceCreateOptions::default())?;
        cache.insert(name.to_string(), ks.clone());
        Ok(ks)
    }
}

#[async_trait]
impl KvStore for FjallStore {
    async fn get(&self, partition: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let keyspace = self.get_or_create_keyspace(partition)?;
        let key = key.to_vec();
        blocking::unblock(move || {
            keyspace
                .get(&key)
                .map(|op| op.map(|v| v.to_vec()))
                .map_err(Error::Fjall)
        })
        .await
    }
    async fn set(&self, partition: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let keyspace = self.get_or_create_keyspace(partition)?;
        let key = key.to_vec();
        let value = value.to_vec();
        let key_for_notify = key.clone();
        let value_for_notify = value.clone();
        blocking::unblock({
            let key = key;
            let value = value;
            move || keyspace.insert(&key, &value).map_err(Error::Fjall)
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
        let keyspace = self.get_or_create_keyspace(partition)?;
        let key = key.to_vec();
        let key_for_notify = key.clone();
        blocking::unblock(move || keyspace.remove(&key).map_err(Error::Fjall)).await?;
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

        let keyspace = match self.get_or_create_keyspace(partition) {
            Ok(ks) => ks,
            Err(e) => {
                let _ = tx.try_send(Err(e));
                return rx;
            }
        };

        let prefix = prefix.map(|p| p.to_vec());

        thread::spawn(move || {
            let iter = match prefix {
                Some(ref p) => keyspace.prefix(p),
                None => keyspace.iter(),
            };

            for kv in iter {
                let (key, value) = match kv.into_inner() {
                    Ok(kv) => (kv.0.to_vec(), kv.1.to_vec()),
                    Err(e) => {
                        let _ = tx.send_blocking(Err(Error::Fjall(e)));
                        break;
                    }
                };
                if tx.send_blocking(Ok((key, value))).is_err() {
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
