#![allow(missing_docs)]

#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

mod block_ingest;
mod forwarder;
mod serialized;
mod spot_meta;

use block_ingest::BlockIngest;
use clap::{Args, Parser};
use forwarder::EthForwarderApiServer;
use reth::cli::Cli;
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_node_ethereum::EthereumNode;
use tracing::info;

#[derive(Args, Debug, Clone)]
struct HyperliquidExtArgs {
    /// Upstream RPC URL to forward incoming transactions.
    #[arg(long, default_value = "https://rpc.hyperliquid.xyz/evm")]
    pub upstream_rpc_url: String,
}

fn main() {
    reth_cli_util::sigsegv_handler::install();

    // Enable backtraces unless a RUST_BACKTRACE value has already been explicitly provided.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    if let Err(err) = Cli::<EthereumChainSpecParser, HyperliquidExtArgs>::parse().run(
        |builder, ext_args| async move {
            let ingest_dir = builder.config().ingest_dir.clone().expect("ingest dir not set");
            info!(target: "reth::cli", "Launching node");
            let handle = builder
                .node(EthereumNode::default())
                .extend_rpc_modules(move |ctx| {
                    let upstream_rpc_url = ext_args.upstream_rpc_url.clone();
                    let rpc = forwarder::EthForwarderExt::new(upstream_rpc_url).into_rpc();
                    for method_name in rpc.method_names() {
                        ctx.modules.remove_method_from_configured(method_name);
                    }
                    ctx.modules.merge_configured(rpc)?;

                    info!("Transaction forwarder extension enabled");
                    Ok(())
                })
                .launch()
                .await?;

            let ingest = BlockIngest(ingest_dir);
            ingest.run(handle.node).await.unwrap();
            handle.node_exit_future.await
        },
    ) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
