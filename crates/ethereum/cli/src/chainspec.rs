extern crate alloc;

use alloy_primitives::{b256, Address, Bytes, B256, B64, U256};
use once_cell::sync::Lazy;
use reth_chainspec::{ChainSpec, DEV, DEV_HARDFORKS, HOLESKY, SEPOLIA};
use reth_cli::chainspec::{parse_genesis, ChainSpecParser};
use reth_primitives::{Header, SealedHeader};
use std::sync::Arc;

/// Chains supported by reth. First value should be used as the default.
pub const SUPPORTED_CHAINS: &[&str] = &["mainnet", "sepolia", "holesky", "dev"];

static GENESIS_HASH: B256 =
    b256!("d8fcc13b6a195b88b7b2da3722ff6cad767b13a8c1e9ffb1c73aa9d216d895f0");

/// The Hyperliqiud Mainnet spec
pub static HL_MAINNET: Lazy<alloc::sync::Arc<ChainSpec>> = Lazy::new(|| {
    ChainSpec {
        chain: alloy_chains::Chain::from_id(999),
        // genesis contains empty alloc field because state at first bedrock block is imported
        // manually from trusted source
        genesis: serde_json::from_str(r#"{
    "nonce": "0x0",
    "timestamp": "0x6490fdd2",
    "extraData": "0x",
    "gasLimit": "0x1c9c380",
    "difficulty": "0x0",
    "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "coinbase": "0x0000000000000000000000000000000000000000",
    "stateRoot": "0x5eb6e371a698b8d68f665192350ffcecbbbf322916f4b51bd79bb6887da3f494",
    "alloc": {
        "0x2222222222222222222222222222222222222222": {
            "nonce": 0,
            "balance": "0x33b2e3c9fd0803ce8000000",
            "code": "0x608060405236603f5760405134815233907f88a5966d370b9919b20f3e2c13ff65706f196a4e32cc2c12bf57088f885258749060200160405180910390a2005b600080fdfea2646970667358221220ca425db50898ac19f9e4676e86e8ebed9853baa048942f6306fe8a86b8d4abb964736f6c63430008090033",
            "storage": {}
        },
        "0x5555555555555555555555555555555555555555": {
            "nonce": 0,
            "balance": "0x0",
            "code": "0x6080604052600436106100bc5760003560e01c8063313ce56711610074578063a9059cbb1161004e578063a9059cbb146102cb578063d0e30db0146100bc578063dd62ed3e14610311576100bc565b8063313ce5671461024b57806370a082311461027657806395d89b41146102b6576100bc565b806318160ddd116100a557806318160ddd146101aa57806323b872dd146101d15780632e1a7d4d14610221576100bc565b806306fdde03146100c6578063095ea7b314610150575b6100c4610359565b005b3480156100d257600080fd5b506100db6103a8565b6040805160208082528351818301528351919283929083019185019080838360005b838110156101155781810151838201526020016100fd565b50505050905090810190601f1680156101425780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b34801561015c57600080fd5b506101966004803603604081101561017357600080fd5b5073ffffffffffffffffffffffffffffffffffffffff8135169060200135610454565b604080519115158252519081900360200190f35b3480156101b657600080fd5b506101bf6104c7565b60408051918252519081900360200190f35b3480156101dd57600080fd5b50610196600480360360608110156101f457600080fd5b5073ffffffffffffffffffffffffffffffffffffffff8135811691602081013590911690604001356104cb565b34801561022d57600080fd5b506100c46004803603602081101561024457600080fd5b503561066b565b34801561025757600080fd5b50610260610700565b6040805160ff9092168252519081900360200190f35b34801561028257600080fd5b506101bf6004803603602081101561029957600080fd5b503573ffffffffffffffffffffffffffffffffffffffff16610709565b3480156102c257600080fd5b506100db61071b565b3480156102d757600080fd5b50610196600480360360408110156102ee57600080fd5b5073ffffffffffffffffffffffffffffffffffffffff8135169060200135610793565b34801561031d57600080fd5b506101bf6004803603604081101561033457600080fd5b5073ffffffffffffffffffffffffffffffffffffffff813581169160200135166107a7565b33600081815260036020908152604091829020805434908101909155825190815291517fe1fffcc4923d04b559f4d29a8bfc6cda04eb5b0d3c460751c2402c5c5cc9109c9281900390910190a2565b6000805460408051602060026001851615610100027fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0190941693909304601f8101849004840282018401909252818152929183018282801561044c5780601f106104215761010080835404028352916020019161044c565b820191906000526020600020905b81548152906001019060200180831161042f57829003601f168201915b505050505081565b33600081815260046020908152604080832073ffffffffffffffffffffffffffffffffffffffff8716808552908352818420869055815186815291519394909390927f8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925928290030190a350600192915050565b4790565b73ffffffffffffffffffffffffffffffffffffffff83166000908152600360205260408120548211156104fd57600080fd5b73ffffffffffffffffffffffffffffffffffffffff84163314801590610573575073ffffffffffffffffffffffffffffffffffffffff841660009081526004602090815260408083203384529091529020547fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff14155b156105ed5773ffffffffffffffffffffffffffffffffffffffff841660009081526004602090815260408083203384529091529020548211156105b557600080fd5b73ffffffffffffffffffffffffffffffffffffffff841660009081526004602090815260408083203384529091529020805483900390555b73ffffffffffffffffffffffffffffffffffffffff808516600081815260036020908152604080832080548890039055938716808352918490208054870190558351868152935191937fddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef929081900390910190a35060019392505050565b3360009081526003602052604090205481111561068757600080fd5b33600081815260036020526040808220805485900390555183156108fc0291849190818181858888f193505050501580156106c6573d6000803e3d6000fd5b5060408051828152905133917f7fcf532c15f0a6db0bd6d0e038bea71d30d808c7d98cb3bf7268a95bf5081b65919081900360200190a250565b60025460ff1681565b60036020526000908152604090205481565b60018054604080516020600284861615610100027fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff0190941693909304601f8101849004840282018401909252818152929183018282801561044c5780601f106104215761010080835404028352916020019161044c565b60006107a03384846104cb565b9392505050565b60046020908152600092835260408084209091529082529020548156fea265627a7a72315820e87684b404839c5657b1e7820bfa5ac4539ac8c83c21e28ec1086123db902cfe64736f6c63430005110032",
            "storage": {
                "0x0000000000000000000000000000000000000000000000000000000000000000": "0x5772617070656420485950450000000000000000000000000000000000000018",
                "0x0000000000000000000000000000000000000000000000000000000000000001": "0x574859504500000000000000000000000000000000000000000000000000000a",
                "0x0000000000000000000000000000000000000000000000000000000000000002": "0x0000000000000000000000000000000000000000000000000000000000000012"
            }
        }
    },
    "number": "0x0",
    "gasUsed": "0x0",
    "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "parentBeaconBlockRoot": "0x0000000000000000000000000000000000000000000000000000000000000000"
}"#)
                .expect("Can't deserialize Hyperliquid Mainnet genesis json"),
            genesis_header: SealedHeader::new(
                Header {
                    parent_hash: B256::ZERO,
                    number: 0,
                    timestamp: 0,
                    transactions_root: B256::ZERO,
                    receipts_root: B256::ZERO,
                    state_root: B256::ZERO,
                    gas_used: 0,
                    gas_limit: 0x1c9c380,
                    difficulty: U256::ZERO,
                    mix_hash: B256::ZERO,
                    extra_data: Bytes::new(),
                    nonce: B64::ZERO,
                    ommers_hash: B256::ZERO,
                    beneficiary: Address::ZERO,
                    logs_bloom: Default::default(),
                    base_fee_per_gas: Some(0),
                    withdrawals_root: Some(B256::ZERO),
                    blob_gas_used: Some(0),
                    excess_blob_gas: Some(0),
                    parent_beacon_block_root: Some(B256::ZERO),
                    requests_hash: Some(B256::ZERO),
                },
                GENESIS_HASH,
            ),
            paris_block_and_final_difficulty: Some((0, U256::from(0))),
            hardforks: DEV_HARDFORKS.clone(),
            prune_delete_limit: 10000,
            ..Default::default()
        }.into()
});

/// Clap value parser for [`ChainSpec`]s.
///
/// The value parser matches either a known chain, the path
/// to a json file, or a json formatted string in-memory. The json needs to be a Genesis struct.
pub fn chain_value_parser(s: &str) -> eyre::Result<Arc<ChainSpec>, eyre::Error> {
    Ok(match s {
        "mainnet" => HL_MAINNET.clone(),
        "sepolia" => SEPOLIA.clone(),
        "holesky" => HOLESKY.clone(),
        "dev" => DEV.clone(),
        _ => Arc::new(parse_genesis(s)?.into()),
    })
}

/// Ethereum chain specification parser.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct EthereumChainSpecParser;

impl ChainSpecParser for EthereumChainSpecParser {
    type ChainSpec = ChainSpec;

    const SUPPORTED_CHAINS: &'static [&'static str] = SUPPORTED_CHAINS;

    fn parse(s: &str) -> eyre::Result<Arc<ChainSpec>> {
        chain_value_parser(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reth_chainspec::EthereumHardforks;

    #[test]
    fn parse_known_chain_spec() {
        for &chain in EthereumChainSpecParser::SUPPORTED_CHAINS {
            assert!(<EthereumChainSpecParser as ChainSpecParser>::parse(chain).is_ok());
        }
    }

    #[test]
    fn parse_raw_chainspec_hardforks() {
        let s = r#"{
  "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
  "uncleHash": "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
  "coinbase": "0x0000000000000000000000000000000000000000",
  "stateRoot": "0x76f118cb05a8bc558388df9e3b4ad66ae1f17ef656e5308cb8f600717251b509",
  "transactionsTrie": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
  "receiptTrie": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
  "bloom": "0x000...000",
  "difficulty": "0x00",
  "number": "0x00",
  "gasLimit": "0x016345785d8a0000",
  "gasUsed": "0x00",
  "timestamp": "0x01",
  "extraData": "0x00",
  "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
  "nonce": "0x0000000000000000",
  "baseFeePerGas": "0x07",
  "withdrawalsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
  "blobGasUsed": "0x00",
  "excessBlobGas": "0x00",
  "parentBeaconBlockRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
  "requestsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
  "hash": "0xc20e1a771553139cdc77e6c3d5f64a7165d972d327eee9632c9c7d0fe839ded4",
  "alloc": {},
  "config": {
    "ethash": {},
    "chainId": 1,
    "homesteadBlock": 0,
    "daoForkSupport": true,
    "eip150Block": 0,
    "eip155Block": 0,
    "eip158Block": 0,
    "byzantiumBlock": 0,
    "constantinopleBlock": 0,
    "petersburgBlock": 0,
    "istanbulBlock": 0,
    "berlinBlock": 0,
    "londonBlock": 0,
    "terminalTotalDifficulty": 0,
    "shanghaiTime": 0,
    "cancunTime": 0,
    "pragueTime": 0,
    "osakaTime": 0
  }
}"#;

        let spec = <EthereumChainSpecParser as ChainSpecParser>::parse(s).unwrap();
        assert!(spec.is_shanghai_active_at_timestamp(0));
        assert!(spec.is_cancun_active_at_timestamp(0));
        assert!(spec.is_prague_active_at_timestamp(0));
        assert!(spec.is_osaka_active_at_timestamp(0));
    }
}
