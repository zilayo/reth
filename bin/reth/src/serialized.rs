use alloy_primitives::{Address, Bytes, Log};
use reth_primitives::{SealedBlock, Transaction};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct BlockAndReceipts {
    pub(crate) block: EvmBlock,
    pub(crate) receipts: Vec<LegacyReceipt>,
    #[serde(default)]
    pub(crate) system_txs: Vec<SystemTx>,
    #[serde(default)]
    pub(crate) read_precompile_calls:
        Vec<(Address, Vec<(ReadPrecompileInput, ReadPrecompileResult)>)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum EvmBlock {
    Reth115(SealedBlock),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct LegacyReceipt {
    tx_type: LegacyTxType,
    success: bool,
    cumulative_gas_used: u64,
    logs: Vec<Log>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum LegacyTxType {
    Legacy = 0,
    Eip2930 = 1,
    Eip1559 = 2,
    Eip4844 = 3,
    Eip7702 = 4,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct SystemTx {
    pub(crate) tx: Transaction,
    pub(crate) receipt: Option<LegacyReceipt>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub(crate) struct ReadPrecompileInput {
    pub(crate) input: Bytes,
    pub(crate) gas_limit: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) enum ReadPrecompileResult {
    Ok { gas_used: u64, bytes: Bytes },
    OutOfGas,
    Error,
    UnexpectedError,
}
