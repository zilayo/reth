#![allow(missing_docs)]

#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

mod block_ingest;
mod serialized;

use std::path::PathBuf;

use block_ingest::BlockIngest;
use clap::{Args, Parser};
use reth::cli::Cli;
use reth_ethereum_cli::chainspec::EthereumChainSpecParser;
use reth_node_ethereum::EthereumNode;
use tracing::info;

#[derive(Args, Debug, Clone)]
struct IngestArgs {
    /// EVM blocks base directory
    #[arg(long, default_value="/tmp/evm-blocks")]
    pub ingest_dir: PathBuf,
}

fn main() {
    reth_cli_util::sigsegv_handler::install();

    // Enable backtraces unless a RUST_BACKTRACE value has already been explicitly provided.
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        std::env::set_var("RUST_BACKTRACE", "1");
    }

    if let Err(err) = Cli::<EthereumChainSpecParser, IngestArgs>::parse().run(|builder, ingest_args| async move {
        info!(target: "reth::cli", "Launching node");
        let handle = builder.launch_node(EthereumNode::default()).await?;

        let ingest_dir = ingest_args.ingest_dir;
        let ingest = BlockIngest(ingest_dir);
        ingest.run(handle.node).await.unwrap();
        handle.node_exit_future.await
    }) {
        eprintln!("Error: {err:?}");
        std::process::exit(1);
    }
}
