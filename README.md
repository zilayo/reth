# nanoreth

Hyperliquid archive node based on [reth](https://github.com/paradigmxyz/reth).

## System Transactions Appear as Pseudo Transactions

Deposit transactions from `0x222..22` to user addresses are intentionally recorded as pseudo transactions.
This change simplifies block explorers, making it easier to track deposit timestamps.
Ensure careful handling when indexing.

## How to run (mainnet)

```sh
# Fetch EVM blocks
$ aws s3 sync s3://hl-mainnet-evm-blocks/ ~/evm-blocks --request-payer requester # one-time
$ goofys --region=ap-northeast-1 --requester-pays hl-mainnet-evm-blocks evm-blocks-bak # realtime

# Run node
$ make install
$ reth node --http --http.addr 0.0.0.0 --http.api eth,ots,net,web3 \
    --ws --ws.addr 0.0.0.0 --ws.origins '*' --ws.api eth,ots,net,web3 --ingest-dir ~/evm-blocks --ws.port 8545
```

## How to run (testnet)

Testnet is supported since block 21043587.

```sh
# Get testnet genesis at block 21043587
$ cd ~
$ git clone https://github.com/sprites0/hl-testnet-genesis
$ zstd --rm -d ~/hl-testnet-genesis/*.zst

# Init node
$ make install
$ reth init-state --without-evm --chain testnet --header ~/hl-testnet-genesis/21043587.rlp \
  --header-hash 0x2404e9a38b87e9028295df91e922c2e804c3ca75b550289cf5b353a9c61c34ea \
  ~/hl-testnet-genesis/21043587.jsonl --total-difficulty 0 

# Run node
$ reth node --http --http.addr 0.0.0.0 --http.api eth,ots,net,web3 \
    --ws --ws.addr 0.0.0.0 --ws.origins '*' --ws.api eth,ots,net,web3 --ingest-dir ~/evm-blocks --ws.port 8546
```
