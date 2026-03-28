# client_bench

A dedicated client-only benchmark node for `nvmeof_raft` and `goraft` clusters.

Unlike `multi_goraft` / `multi_nvmeof` where the leader also generates load, this binary runs on a **separate machine** that is not part of the Raft cluster. It connects to the leader over RDMA (or TCP) and calls `Server.HandleClientApply` remotely, so the leader's CPU and NVMe are free to handle only Raft duties.

Each `--threads` goroutine opens its own connection to the leader (avoids send-side serialization in `net/rpc`).

## Build

From the project root (`~/nvmeof_raft`):

```console
GOPATH=~/go

# RDMA version
go build -tags raft -o bench_client ./cmd/benchmarks/client_bench/

# TCP version
go build -tags raft_tcp -o bench_client_tcp ./cmd/benchmarks/client_bench/
```

The client binary does **not** need a block device or metadata directory — it carries no Raft state.

## Run

### 3-node cluster (eternity4 / eternity5 / eternity6)

Start the three Raft nodes first (without `--bench`), let them elect a leader, then run the client on a separate machine (e.g. `eternity2`):

**Raft nodes (eternity4 / eternity5 / eternity6):**
#### nvmeof
```console
# eternity4 (node 0) — no --bench, just participates
sudo ./bench_multi_nvmeof --id=0 \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --device=/dev/nvme0n1 --partition-offset=1048576

# eternity5 (node 1)
sudo ./bench_multi_nvmeof --id=1 \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --device=/dev/nvme2n1 --partition-offset=10738466816

# eternity6 (node 2)
sudo ./bench_multi_nvmeof --id=2 \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --device=/dev/nvme0n1 --partition-offset=21475885056
```

#### goraft
```console
# node 0 — eternity4 (benchmark driver)
sudo ./bench_multi_goraft \
  --id=0 \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft

# node 1 — eternity5 (follower)
sudo ./bench_multi_goraft \
  --id=1 \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft

# node 2 — eternity6 (follower)
sudo ./bench_multi_goraft \
  --id=2 \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft

```

**Client node (eternity2 or any other machine with IB connectivity):**
```console
./bench_client \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --entries=100000 --batch=16 --payload=65536 --threads=8
```

The client auto-discovers the leader by probing each peer.

### 5-node cluster (eternity4 / eternity5 / eternity6 / eternity2 / eternity7)

**Raft nodes:**
```console
# eternity4 (node 0)
sudo ./bench_multi_nvmeof --id=0 \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022,eternity2:4023,eternity7:4024 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --device=/dev/nvme1n1 --partition-offset=1048576

# eternity5 (node 1)
sudo ./bench_multi_nvmeof --id=1 \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022,eternity2:4023,eternity7:4024 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --device=/dev/nvme2n1 --partition-offset=53688139776

# eternity6 (node 2)
sudo ./bench_multi_nvmeof --id=2 \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022,eternity2:4023,eternity7:4024 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --device=/dev/nvme0n1 --partition-offset=107375230976

# eternity2 (node 3)
sudo ./bench_multi_nvmeof --id=3 \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022,eternity2:4023,eternity7:4024 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --device=/dev/nvme2n1 --partition-offset=161062322176

# eternity7 (node 4)
sudo ./bench_multi_nvmeof --id=4 \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022,eternity2:4023,eternity7:4024 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --device=/dev/nvme2n1 --partition-offset=214749413376
```

**Client node (any machine not in the cluster, e.g. eternity8):**
```console
./bench_client \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022,eternity2:4023,eternity7:4024 \
  --entries=100000 --batch=16 --payload=65536 --threads=8
```

### TCP version

```console
./bench_client_tcp \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022 \
  --entries=100000 --batch=256 --payload=8192 --threads=8
```

## Flags

| Flag          | Default  | Description                                                     |
|---------------|----------|-----------------------------------------------------------------|
| `--peers`     | *(required)* | Comma-separated `host:port` list of Raft nodes              |
| `--entries`   | `10000`  | Total entries to submit (split evenly across threads)          |
| `--batch`     | `256`    | Commands per `Apply()` call per thread                         |
| `--payload`   | `8192`   | Payload size in bytes                                          |
| `--threads`   | `1`      | Concurrent goroutines; each opens its own RDMA/TCP connection  |

## Expected output

```
Searching for leader among: [10.0.0.4:4020 10.0.0.5:4021 10.0.0.6:4022]
  10.0.0.4:4020: follower
  10.0.0.5:4021: follower
Leader: 10.0.0.6:4022
Benchmark: 100000 entries, batch=16, payload=65536, threads=8

======= Experimental Parameters =======
100000 entries, Batch: 16, Payload: 65536, Threads: 8
=== client bench (remote Apply) ===
  Total time   : Xs
  Throughput   : Y entries/s
  Latency avg  : Zms
  Latency min  : ...
  Latency p50  : ...
  Latency p99  : ...
  Latency max  : ...
```

## Architecture

```
client machine (eternity2)
  bench_client
    thread 0 ──RDMA──► Server.HandleClientApply ──► Apply() ──► AppendEntries ──► followers
    thread 1 ──RDMA──► Server.HandleClientApply ──► Apply()         (PBA copy)
    thread 2 ──RDMA──► ...
    ...

  no NVMe, no Raft state, no election participation
```

The leader handles only Raft duties (no payload generation), giving a clean measurement of the cluster's replication throughput.

## Why separate connections per thread

`net/rpc.Client` serializes sends with an internal mutex. With a shared single connection, N threads contend on that mutex and effectively serialize at the transport layer. Per-thread connections map each goroutine to its own RDMA QP (Queue Pair), allowing true parallel in-flight RPCs.
