//! Basic example demonstrating how to use KvStore with a local store.
//! This example shows how to create a local store, set and get key-value pairs, scan for keys with a prefix, and use JSON serialization.
//!
//! Run with:
//!    cargo run --example basic

use kvwrap::{KvStore, LocalConfig, LocalStore};
use std::sync::Arc;

fn main() {
    pollster::block_on(async {
        // Create a store
        let store: Arc<dyn KvStore> = Arc::new(
            LocalStore::new(LocalConfig {
                path: "./data/mydb".to_string(),
                cache_size: 1024 * 1024 * 10, // 10MB cache
            })
            .unwrap_or_else(|e| {
                eprintln!("Failed to create store: {}", e);
                std::process::exit(1);
            }),
        );

        println!("Store opened successfully at ./data/mydb");

        match store.set("partition1", b"prefix:key1", b"value1").await {
            Ok(_) => println!("Data set successfully"),
            Err(e) => eprintln!("Failed to set data: {}", e),
        }

        match store.get("partition1", b"prefix:key1").await {
            Ok(Some(value)) => println!("Retrieved value: {}", String::from_utf8_lossy(&value)),
            Ok(None) => println!("Key not found"),
            Err(e) => eprintln!("Failed to get data: {}", e),
        }

        let rx = store.scan("partition1", Some(b"prefix:"), 10);
        println!("Scanning for keys with prefix 'prefix:' in partition1:");
        while let Ok(result) = rx.recv().await {
            match result {
                Ok((key, value)) => {
                    println!(
                        "Key: {}, Value: {}",
                        String::from_utf8_lossy(&key),
                        String::from_utf8_lossy(&value)
                    );
                }
                Err(e) => eprintln!("Error during scan: {}", e),
            }
        }

        match store.delete("partition1", b"prefix:key1").await {
            Ok(_) => println!("Data deleted successfully"),
            Err(e) => eprintln!("Failed to delete data: {}", e),
        }

        match store
            .set_json(
                "partition1",
                b"prefix:key2",
                &serde_json::json!({"name": "Alice", "age": 30}),
            )
            .await
        {
            Ok(_) => println!("JSON data set successfully"),
            Err(e) => eprintln!("Failed to set JSON data: {}", e),
        }

        match store
            .get_json::<serde_json::Value>("partition1", b"prefix:key2")
            .await
        {
            Ok(Some(json)) => println!("Retrieved JSON value: {}", json),
            Ok(None) => println!("Key not found"),
            Err(e) => eprintln!("Failed to get JSON data: {}", e),
        }

        match store.delete("partition1", b"prefix:key2").await {
            Ok(_) => println!("JSON data deleted successfully"),
            Err(e) => eprintln!("Failed to delete JSON data: {}", e),
        }
    });
}
