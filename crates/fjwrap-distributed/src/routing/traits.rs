use crate::routing::{
    error::RouteError,
    types::{ClusterConfig, NodeId, NodeInfo, ShardConfig, ShardId},
};

pub type RouteResult<T> = Result<T, RouteError>;

pub trait KeyRangeExt {
    fn contains(&self, key: &[u8]) -> bool;
    fn overlaps_prefix(&self, prefix: &[u8]) -> bool;
}

pub trait ClusterConfigExt {
    fn get_node(&self, id: &NodeId) -> Option<&NodeInfo>;
    fn get_shard(&self, id: &ShardId) -> Option<&ShardConfig>;
    fn single_node(node_id: u64, host: String, port: u32) -> ClusterConfig;
}

pub trait ShardRouter: Send + Sync {
    fn shard_key(&self, partition: &[u8], key: &[u8]) -> Vec<u8> {
        let mut shard_key = Vec::with_capacity(partition.len() + 1 + key.len());
        shard_key.extend_from_slice(partition);
        shard_key.push(0);
        shard_key.extend_from_slice(key);
        shard_key
    }

    fn route(&self, partition: &[u8], key: &[u8]) -> RouteResult<ShardId>;

    fn shard_nodes(&self, shard_id: ShardId) -> RouteResult<Vec<NodeInfo>>;

    fn node_shard_ids(&self, node_id: &NodeId) -> RouteResult<Vec<ShardId>>;

    fn config_version(&self) -> RouteResult<u64>;
}
