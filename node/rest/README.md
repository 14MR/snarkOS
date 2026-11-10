# snarkos-node-rest

[![Crates.io](https://img.shields.io/crates/v/snarkos-node-rest.svg?color=neon)](https://crates.io/crates/snarkos-node-rest)
[![Authors](https://img.shields.io/badge/authors-Aleo-orange.svg)](https://aleo.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](./LICENSE.md)

The `snarkos-node-rest` crate provides a REST API for the `snarkos` node.

REST responses support gzip compression when the client sends `Accept-Encoding: gzip`.
Responses smaller than 32 bytes are not compressed.

The v1 API and the unversioned routes are deprecated. See [Migrating from the v1 REST API to v2](./MIGRATING_TO_V2.md).

## Selected program history

Run a client or validator with `--history-programs=shield_swap.aleo` to record per-key history for that program after each committed block.
Use a comma-separated list or repeat the flag to select more programs:

```sh
snarkos start --client --history-programs=shield_swap.aleo,shield_swap_router.aleo
```

Dependencies must be selected explicitly. The flag works without `--history-json` and can be combined with it to also write the legacy credits snapshots.
It cannot be combined with `--history-compat-mode`.

Selected key versions and deletion markers are stored in the same ledger RocksDB database. Recording starts with a baseline at the first indexed block, then records changes per committed block. History is available only across contiguous indexed coverage. Gaps are unavailable. Existing history requires replay from genesis or an earlier valid checkpoint; enabling the flag does not backfill a synced ledger.

Read history through `/v2/mainnet`:

```text
GET /program/{program}/mapping/{mapping}/{key}/history/{height}
GET /program/{program}/mapping/{mapping}/history/{height}?keys=key1,key2
```

A missing key in covered history returns `null`. Missing coverage or mapping returns `404`.
This mode does not evaluate historical views or expose state between transactions in the same block.
