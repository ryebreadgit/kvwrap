use crate::routing::{
    error::RouteError,
    traits::{ClusterConfigExt, KeyRangeExt, RouteResult, ShardRouter},
    types::{ClusterConfig, NodeId, NodeInfo, ShardId, ShardStatus},
};
use prost::Message;
use std::sync::Arc;

#[derive(Clone)]
pub struct StaticRouter {
    config: Arc<ClusterConfig>,
}

impl StaticRouter {
    pub fn new(config: ClusterConfig) -> Self {
        StaticRouter {
            config: Arc::new(config),
        }
    }

    pub fn from_json_str(json_str: &str) -> Result<Self, serde_json::Error> {
        let config: ClusterConfig = serde_json::from_str(json_str)?;
        Ok(StaticRouter::new(config))
    }

    pub fn from_proto_bytes(bytes: &[u8]) -> Result<Self, prost::DecodeError> {
        let config = ClusterConfig::decode(bytes)?;
        Ok(StaticRouter::new(config))
    }

    pub fn single_node(node_id: u64, host: String, port: u32) -> Self {
        let config = ClusterConfig::single_node(node_id, host, port);
        StaticRouter::new(config)
    }
}

impl ShardRouter for StaticRouter {
    fn route(&self, partition: &[u8], key: &[u8]) -> RouteResult<ShardId> {
        let shard_key = self.shard_key(partition, key);

        for shard in &self.config.shards {
            if let Some(range) = &shard.range {
                let begin = range.begin.as_ref().map(|b| b.as_slice());
                let end = range.end.as_ref().map(|e| e.as_slice());
                if (begin.is_none() || shard_key.as_slice() >= begin.unwrap())
                    && (end.is_none() || shard_key.as_slice() < end.unwrap())
                {
                    if shard.status() == ShardStatus::Active {
                        return Ok(shard.id.clone().unwrap());
                    }
                }
            }
        }

        Err(RouteError::NoShardForKey)
    }

    fn shard_nodes(&self, shard_id: ShardId) -> RouteResult<Vec<NodeInfo>> {
        let shard = self
            .config
            .get_shard(&shard_id)
            .ok_or(RouteError::ShardNotFound(shard_id.clone()))?;

        let mut nodes = Vec::new();
        for node_id in &shard.replicas {
            if let Some(node) = self.config.get_node(node_id) {
                nodes.push(node.clone());
            }
        }
        if nodes.is_empty() {
            return Err(RouteError::NoNodesAvailable(shard_id));
        }

        Ok(nodes)
    }

    fn node_shard_ids(&self, node_id: &NodeId) -> RouteResult<Vec<ShardId>> {
        let mut shard_ids = Vec::new();
        for shard in &self.config.shards {
            if shard.replicas.iter().any(|n| n == node_id) {
                shard_ids.push(shard.id.clone().unwrap());
            }
        }
        Ok(shard_ids)
    }

    fn config_version(&self) -> RouteResult<u64> {
        Ok(self.config.version)
    }
}
