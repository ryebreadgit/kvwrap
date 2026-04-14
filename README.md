# kvwrap

A Rust library that provides a unified interface for various key-value stores with optional distributed capabilities.

## Usage

```bash
cargo add kvwrap
```

```rust
use kvwrap::{KvStore, LocalConfig, LocalStore};
use std::sync::Arc;

let store: Arc<dyn KvStore> = Arc::new(
    LocalStore::new(LocalConfig {
        path: "./data/mydb".to_string(),
        cache_size: 1024 * 1024 * 10, // 10MB cache
    })
    .expect("Failed to create store"),
);

// Basic key-value operations
store.set("partition1", b"prefix:key1", b"value1").await?;
let value = store.get("partition1", b"prefix:key1").await?; // Option<Vec<u8>>

// Scan keys by prefix
let rx = store.scan("partition1", Some(b"prefix:"), 10);
while let Ok(result) = rx.recv().await {
    let (key, value) = result?;
}

store.delete("partition1", b"prefix:key1").await?;

// JSON support
store.set_json("partition1", b"prefix:key2", &serde_json::json!({"name": "Alice", "age": 30})).await?;
let json: Option<serde_json::Value> = store.get_json("partition1", b"prefix:key2").await?;

store.delete("partition1", b"prefix:key2").await?;
```

## Features

| Flag | Default | Description |
|------|---------|-------------|
| `fjall` | on | Fjall kvstore backend |
| `sled` | off | Sled kvstore backend |
| `distributed` | off | Distributed kvstore backend for sharding, clustering, and replication |
| `client` | off | Client library for connecting to a remote distributed kvstore - requires protobuf |
| `full` | off | Enables distributed & client features |

## Examples

[See here](./crates/kvwrap/examples/) for practical examples of using kvwrap.

## Todo

- [x] Implement client library for connecting to a remote distributed kvstore
- [X] Implement sharding and clustering capabilities in the distributed backend
- [ ] Implement replication and consensus (Raft) in the distributed backend
- [ ] Add support for more kvstore backends (e.g. RocksDB, LMDB)
- [ ] Add more features to the client library (e.g. connection pooling, retries, etc.)
- [ ] Add more examples and documentation

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](./LICENSE.md)