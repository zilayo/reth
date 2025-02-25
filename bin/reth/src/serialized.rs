use alloy_consensus::{TxEip1559, TxEip2930, TxLegacy};
use alloy_rpc_types::Log;
use reth_primitives::{SealedBlock, Transaction};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SerializedTransaction {
    pub transaction: TypedTransaction,
    pub signature: SerializedSignature,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SerializedSignature {
    pub r: [u8; 32],
    pub s: [u8; 32],
    pub v: [u8; 8],
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum BlockInner {
    Reth115(SealedBlock),
}

/// A raw transaction.
///
/// Transaction types were introduced in [EIP-2718](https://eips.ethereum.org/EIPS/eip-2718).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) enum TypedTransaction {
    /// Legacy transaction (type `0x0`).
    ///
    /// Traditional Ethereum transactions, containing parameters `nonce`, `gasPrice`, `gasLimit`,
    /// `to`, `value`, `data`, `v`, `r`, and `s`.
    ///
    /// These transactions do not utilize access lists nor do they incorporate EIP-1559 fee market
    /// changes.
    Legacy(TxLegacy),
    /// Transaction with an [`AccessList`] ([EIP-2930](https://eips.ethereum.org/EIPS/eip-2930)), type `0x1`.
    ///
    /// The `accessList` specifies an array of addresses and storage keys that the transaction
    /// plans to access, enabling gas savings on cross-contract calls by pre-declaring the accessed
    /// contract and storage slots.
    Eip2930(TxEip2930),
    /// A transaction with a priority fee ([EIP-1559](https://eips.ethereum.org/EIPS/eip-1559)), type `0x2`.
    ///
    /// Unlike traditional transactions, EIP-1559 transactions use an in-protocol, dynamically
    /// changing base fee per gas, adjusted at each block to manage network congestion.
    ///
    /// - `maxPriorityFeePerGas`, specifying the maximum fee above the base fee the sender is
    ///   willing to pay
    /// - `maxFeePerGas`, setting the maximum total fee the sender is willing to pay.
    ///
    /// The base fee is burned, while the priority fee is paid to the miner who includes the
    /// transaction, incentivizing miners to include transactions with higher priority fees per
    /// gas.
    Eip1559(TxEip1559),
}

impl TypedTransaction {
    pub(crate) fn to_reth(self) -> Transaction {
        match self {
            Self::Legacy(tx) => Transaction::Legacy(tx),
            Self::Eip2930(tx) => Transaction::Eip2930(tx),
            Self::Eip1559(tx) => Transaction::Eip1559(tx),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum TxType {
    /// Legacy transaction type.
    Legacy,
    /// EIP-2930 transaction type.
    Eip2930,
    /// EIP-1559 transaction type.
    Eip1559,
    /// EIP-4844 transaction type.
    Eip4844,
    /// EIP-7702 transaction type.
    Eip7702,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Receipt {
    /// Receipt type.
    pub tx_type: TxType,
    /// If transaction is executed successfully.
    ///
    /// This is the `statusCode`
    pub success: bool,
    /// Gas used
    pub cumulative_gas_used: u64,
    /// Log send from contracts.
    pub logs: Vec<Log>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SystemTransaction {
    pub receipt: Receipt,
    pub tx: TypedTransaction,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Block {
    pub block: BlockInner,
    pub system_txs: Vec<SystemTransaction>,
}
