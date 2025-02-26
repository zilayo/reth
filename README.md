# nanoreth

Hyperliquid archive node based on [reth](https://github.com/paradigmxyz/reth).

## How to run

```sh
# Fetch EVM blocks
$ aws s3 sync s3://hl-mainnet-evm-blocks/ ~/evm-blocks --request-payer requester # one-time
$ goofys --region=ap-northeast-1 --requester-pays hl-mainnet-evm-blocks evm-blocks-bak # realtime

# Run node
$ make install
$ reth node --http --http.addr 0.0.0.0 --http.api eth,ots,net,web3 --ws --ws.addr 0.0.0.0 --ws.origins '*' --ws.api eth,ots,net,web3 --ingest-dir ~/evm-blocks --ws.port 8545
```

## System Transactions Appear as Pseudo Transactions

Deposit transactions from `0x222..22` to user addresses are intentionally recorded as pseudo transactions.
This change simplifies block explorers, making it easier to track deposit timestamps.
Ensure careful handling when indexing.
