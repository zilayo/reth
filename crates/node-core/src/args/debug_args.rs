//! clap [Args](clap::Args) for debugging purposes

use clap::Args;
use reth_primitives::{TxHash, B256};
use std::path::PathBuf;

/// Parameters for debugging purposes
#[derive(Debug, Clone, Args, PartialEq, Default)]
#[clap(next_help_heading = "Debug")]
pub struct DebugArgs {
    /// Prompt the downloader to download blocks one at a time.
    ///
    /// NOTE: This is for testing purposes only.
    #[arg(long = "debug.continuous", help_heading = "Debug", conflicts_with = "tip")]
    pub continuous: bool,

    /// Flag indicating whether the node should be terminated after the pipeline sync.
    #[arg(long = "debug.terminate", help_heading = "Debug")]
    pub terminate: bool,

    /// Set the chain tip manually for testing purposes.
    ///
    /// NOTE: This is a temporary flag
    #[arg(long = "debug.tip", help_heading = "Debug", conflicts_with = "continuous")]
    pub tip: Option<B256>,

    /// Runs the sync only up to the specified block.
    #[arg(long = "debug.max-block", help_heading = "Debug")]
    pub max_block: Option<u64>,

    /// Print opcode level traces directly to console during execution.
    #[arg(long = "debug.print-inspector", help_heading = "Debug")]
    pub print_inspector: bool,

    /// Hook on a specific block during execution.
    #[arg(
        long = "debug.hook-block",
        help_heading = "Debug",
        conflicts_with = "hook_transaction",
        conflicts_with = "hook_all"
    )]
    pub hook_block: Option<u64>,

    /// Hook on a specific transaction during execution.
    #[arg(
        long = "debug.hook-transaction",
        help_heading = "Debug",
        conflicts_with = "hook_block",
        conflicts_with = "hook_all"
    )]
    pub hook_transaction: Option<TxHash>,

    /// Hook on every transaction in a block.
    #[arg(
        long = "debug.hook-all",
        help_heading = "Debug",
        conflicts_with = "hook_block",
        conflicts_with = "hook_transaction"
    )]
    pub hook_all: bool,

    /// The path to store engine API messages at.
    /// If specified, all of the intercepted engine API messages
    /// will be written to specified location.
    #[arg(long = "debug.engine-api-store", help_heading = "Debug", value_name = "PATH")]
    pub engine_api_store: Option<PathBuf>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// A helper type to parse Args more easily
    #[derive(Parser)]
    struct CommandParser<T: Args> {
        #[clap(flatten)]
        args: T,
    }

    #[test]
    fn test_parse_database_args() {
        let default_args = DebugArgs::default();
        let args = CommandParser::<DebugArgs>::parse_from(["reth"]).args;
        assert_eq!(args, default_args);
    }
}
