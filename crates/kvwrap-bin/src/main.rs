use anyhow::Error;
use clap::Parser;
use fracturedjson::Formatter;
use kvwrap::{
    ClusterConfig, KeyRange, LocalConfig, LocalStore, NodeAddress, NodeId, NodeInfo, ShardConfig,
    ShardId, ShardStatus, StaticRouter, run_server,
};
use std::{fs, net::SocketAddr, sync::Arc};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "0.0.0.0:50051", env = "KVWRAP_LISTEN_ADDR")]
    listen: SocketAddr,

    #[arg(long, default_value = "0", env = "KVWRAP_NODE_ID")]
    node_id: u64,

    #[arg(long, default_value = ".kvwrap_data", env = "KVWRAP_DATA_PATH")]
    data_path: String,

    #[arg(long, default_value = "67108864", env = "KVWRAP_CACHE_SIZE")] // 64 MiB
    cache_size: u64,

    #[arg(
        long,
        default_value = "./router_settings.json",
        env = "KVWRAP_ROUTER_CONFIG_PATH"
    )]
    router_config_path: String,

    #[arg(long, default_value = "false", env = "KVWRAP_VERBOSE")]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();

    if args.verbose {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .init();
    }

    if !fs::metadata(&args.router_config_path).is_ok() {
        let mut formatter = Formatter::new();
        formatter.options.indent_spaces = 2;
        formatter.options.simple_bracket_padding = true;
        let default_config = ClusterConfig {
            version: 1,
            nodes: vec![NodeInfo {
                id: Some(NodeId { id: 0 }),
                address: Some(NodeAddress {
                    host: args.listen.ip().to_string(),
                    port: args.listen.port().into(),
                }),
            }],
            shards: vec![ShardConfig {
                id: Some(ShardId { id: 0 }),
                range: Some(KeyRange {
                    begin: None,
                    end: Some(vec![0xFF, 0xFF, 0xFF, 0xFF]),
                }),
                replicas: vec![NodeId { id: 0 }],
                status: ShardStatus::Active.into(),
                leader: Some(NodeId { id: 0 }),
            }],
        };
        let json_str = formatter.serialize(&default_config, 0, 100)?;
        fs::write(&args.router_config_path, json_str)?;
        println!(
            "Default router config written to '{}'",
            &args.router_config_path
        );
    }

    let config = LocalConfig {
        path: args.data_path,
        cache_size: args.cache_size,
    };

    let store = Arc::new(LocalStore::new(config)?);

    let router_config_str = if fs::metadata(&args.router_config_path).is_ok() {
        fs::read_to_string(&args.router_config_path)?
    } else {
        println!(
            "Router config file '{}' not found, using single-node config",
            args.router_config_path
        );
        String::new()
    };

    let router_config = StaticRouter::from_json_str(&router_config_str)?;

    let router = Arc::new(router_config);

    println!("Starting server on {}", args.listen);
    run_server(store, router, args.node_id, args.listen).await?;

    Ok(())
}
