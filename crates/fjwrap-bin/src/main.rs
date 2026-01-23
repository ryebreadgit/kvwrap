use anyhow::Error;
use clap::Parser;
use fjwrap_core::{LocalConfig, LocalStore};
use fjwrap_distributed::run_server;
use std::net::SocketAddr;
use std::sync::Arc;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:50051", env = "FJWRAP_LISTEN_ADDR")]
    listen: SocketAddr,

    #[arg(long, default_value = ".fjwrap_data", env = "FJWRAP_DATA_PATH")]
    data_path: String,

    #[arg(long, default_value = "67108864", env = "FJWRAP_CACHE_SIZE")] // 64 MiB
    cache_size: u64,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();

    let config = LocalConfig {
        path: args.data_path,
        cache_size: args.cache_size,
    };

    let store = Arc::new(LocalStore::new(config)?);

    println!("Starting server on {}", args.listen);
    run_server(store, args.listen).await?;

    Ok(())
}
