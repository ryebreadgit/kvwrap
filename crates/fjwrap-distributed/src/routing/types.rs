use crate::routing::traits::{ClusterConfigExt, KeyRangeExt};
pub use fjwrap_proto::{
    ClusterConfig, KeyRange, NodeAddress, NodeId, NodeInfo, ShardConfig, ShardId, ShardStatus,
};

impl KeyRangeExt for KeyRange {
    fn contains(&self, key: &[u8]) -> bool {
        if let Some(begin) = &self.begin {
            if key < begin.as_slice() {
                return false;
            }
        }
        if let Some(end) = &self.end {
            if key >= end.as_slice() {
                return false;
            }
        }
        true
    }
    fn overlaps_prefix(&self, prefix: &[u8]) -> bool {
        if let Some(end) = &self.end {
            if prefix >= end.as_slice() {
                return false;
            }
        }
        if let Some(begin) = &self.begin {
            if begin.starts_with(prefix) || prefix >= begin.as_slice() {
                return true;
            }
            return false;
        }
        true
    }
}

impl ClusterConfigExt for ClusterConfig {
    fn get_node(&self, id: &NodeId) -> Option<&NodeInfo> {
        self.nodes.iter().find(|node| node.id.as_ref() == Some(id))
    }

    fn get_shard(&self, id: &ShardId) -> Option<&ShardConfig> {
        self.shards
            .iter()
            .find(|shard| shard.id.as_ref() == Some(id))
    }

    fn single_node(node_id: u64, host: String, port: u32) -> ClusterConfig {
        ClusterConfig {
            version: 1,
            nodes: vec![NodeInfo {
                id: Some(NodeId { id: node_id }),
                address: Some(NodeAddress { host, port }),
            }],
            shards: vec![ShardConfig {
                id: Some(ShardId { id: 0 }),
                range: Some(KeyRange {
                    begin: None,
                    end: None,
                }),
                replicas: vec![NodeId { id: node_id }],
                status: ShardStatus::Active.into(),
                leader: Some(NodeId { id: node_id }),
            }],
        }
    }
}
