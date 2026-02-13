use crate::{KvStore, Result};
use async_channel::{Receiver, Sender};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub enum WatchEvent {
    Set {
        partition: Vec<u8>,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        partition: Vec<u8>,
        key: Vec<u8>,
    },
}

impl WatchEvent {
    pub fn partition(&self) -> &[u8] {
        match self {
            WatchEvent::Set { partition, .. } => partition,
            WatchEvent::Delete { partition, .. } => partition,
        }
    }

    pub fn key(&self) -> &[u8] {
        match self {
            WatchEvent::Set { key, .. } => key,
            WatchEvent::Delete { key, .. } => key,
        }
    }
}

struct WatchSubscription {
    partition: Vec<u8>,
    matcher: KeyMatcher,
    sender: Sender<WatchEvent>,
}

enum KeyMatcher {
    Exact(Vec<u8>),
    Prefix(Vec<u8>),
}

impl KeyMatcher {
    fn matches(&self, key: &[u8]) -> bool {
        match self {
            KeyMatcher::Exact(k) => key == k.as_slice(),
            KeyMatcher::Prefix(p) => key.starts_with(p),
        }
    }
}

#[derive(Clone, Default)]
pub struct WatchRegistry {
    subscribers: Arc<Mutex<Vec<WatchSubscription>>>,
}

impl WatchRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn subscribe_key(
        &self,
        partition: &[u8],
        key: &[u8],
        buffer: usize,
    ) -> Receiver<WatchEvent> {
        let (tx, rx) = async_channel::bounded(buffer);
        self.subscribers.lock().unwrap().push(WatchSubscription {
            partition: partition.to_vec(),
            matcher: KeyMatcher::Exact(key.to_vec()),
            sender: tx,
        });
        rx
    }

    pub fn subscribe_prefix(
        &self,
        partition: &[u8],
        prefix: &[u8],
        buffer: usize,
    ) -> Receiver<WatchEvent> {
        let (tx, rx) = async_channel::bounded(buffer);
        self.subscribers.lock().unwrap().push(WatchSubscription {
            partition: partition.to_vec(),
            matcher: KeyMatcher::Prefix(prefix.to_vec()),
            sender: tx,
        });
        rx
    }

    pub fn notify(&self, event: &WatchEvent) {
        let mut subs = self.subscribers.lock().unwrap();
        subs.retain(|sub| {
            if sub.partition != event.partition() || !sub.matcher.matches(event.key()) {
                return true;
            }
            !sub.sender.is_closed() && sub.sender.try_send(event.clone()).is_ok()
        });
    }
}
