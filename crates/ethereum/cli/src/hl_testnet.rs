use alloy_consensus::Header;
use alloy_genesis::{ChainConfig, Genesis};
use alloy_primitives::U256;
use alloy_rlp::Decodable;
use reqwest::blocking::get;
use reth_chainspec::{ChainSpec, DEV_HARDFORKS};
use reth_primitives::SealedHeader;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};

pub(crate) fn load_hl_testnet() -> ChainSpec {
    const TESTNET_GENESIS_URL: &str = "https://raw.githubusercontent.com/sprites0/hl-testnet-genesis/main/19386700.rlp";

    fn download_testnet_genesis() -> Result<&'static str, Box<dyn std::error::Error>> {
        let path = "/tmp/hl_testnet.rmp.lz4";
        println!("Downloading testnet genesis");
        let mut response = get(TESTNET_GENESIS_URL)?;
        if let Some(length) = response.content_length() {
            // Check if the file exists
            if let Ok(metadata) = std::fs::metadata(path) {
                if metadata.len() == length {
                    println!("Already downloaded");
                    return Ok(path);
                }
            }
        }
        let mut file = File::create(path)?;
        let mut downloaded = 0;
        let total_size = response.content_length().unwrap_or(0);
        let mut buffer = vec![0; 0x100000];

        loop {
            let size = response.read(buffer.as_mut_slice())?;
            if size == 0 {
                break;
            }
            file.write_all(&buffer[..size])?;
            downloaded += size as u64;
            println!(
                "Downloaded {} of {} bytes ({}%)",
                downloaded,
                total_size,
                (downloaded as f64 / total_size as f64 * 100.0).round()
            );
        }
        Ok(path)
    }

    let path = download_testnet_genesis().expect("Failed to download testnet genesis");
    let mut file = File::open(path).expect("Failed to open testnet genesis");
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).expect("Failed to read testnet genesis");
    let mut header = Header::decode(&mut &buffer[..]).expect("Failed to decode testnet genesis");

    let config = ChainConfig {
        chain_id: 998,
        homestead_block: Some(0),
        dao_fork_block: Some(0),
        dao_fork_support: false,
        eip150_block: Some(0),
        eip155_block: Some(0),
        eip158_block: Some(0),
        byzantium_block: Some(0),
        constantinople_block: Some(0),
        petersburg_block: Some(0),
        istanbul_block: Some(0),
        muir_glacier_block: Some(0),
        berlin_block: Some(0),
        london_block: Some(0),
        arrow_glacier_block: Some(0),
        gray_glacier_block: Some(0),
        merge_netsplit_block: Some(0),
        shanghai_time: Some(0),
        cancun_time: Some(0),
        prague_time: Some(0),
        osaka_time: Some(0),
        terminal_total_difficulty: Some(U256::ZERO),
        terminal_total_difficulty_passed: true,
        ethash: None,
        clique: None,
        parlia: None,
        extra_fields: Default::default(),
        deposit_contract_address: None,
        blob_schedule: Default::default(),
    };
    header.number = 0;
    let genesis_header = SealedHeader::new(header.clone(), header.hash_slow());
    let genesis = Genesis {
        config,
        nonce: header.nonce.into(),
        timestamp: header.timestamp,
        extra_data: header.extra_data,
        gas_limit: header.gas_limit,
        difficulty: header.difficulty,
        mix_hash: header.mix_hash,
        coinbase: header.beneficiary,
        alloc: BTreeMap::default(),
        base_fee_per_gas: header.base_fee_per_gas.map(|x| x.into()),
        excess_blob_gas: header.excess_blob_gas,
        blob_gas_used: header.blob_gas_used,
        number: None,
    };

    ChainSpec {
        chain: alloy_chains::Chain::from_id(998),
        genesis: genesis.into(),
        genesis_header,
        hardforks: DEV_HARDFORKS.clone(),
        prune_delete_limit: 10000,
        ..Default::default()
    }
}
