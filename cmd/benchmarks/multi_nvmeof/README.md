# multi_nvmeof

Multi-threaded benchmark for the NVMe-oF optimized `nvmeof_raft` implementation.

Extends `bench_nvmeof` with a `--threads` flag that spawns concurrent client goroutines, each calling `Apply()` independently on the leader. The key replication path remains PBA-based: the leader sends physical block addresses to followers, which copy data directly from the leader's NVMe device using O_DIRECT — no log data crosses the network.

Defaults: 10,000 entries, 256 per batch, 8 KB payload, 1 thread.

## Requirements

- Each node must have a dedicated NVMe(-oF) block device
- Every node must have NVMe-oF access to every other node's device (for cross-device PBA copy)
- The `--metadata-dir` must reside on the **same NVMe partition** as the node's own device so that `FIEMAP` ioctl can resolve physical block addresses
- Root or `disk` group access to the block device (O_DIRECT writes require `sudo`)

## Build

From the project root (`~/nvmeof_raft`):

```console
GOPATH=~/go
# RDMA version
go build -tags raft -o bench_multi_nvmeof ./cmd/benchmarks/multi_nvmeof/
# TCP version
go build -tags raft_tcp -o bench_multi_nvmeof_tcp ./cmd/benchmarks/multi_nvmeof/
```

## Run

### 3-node cluster (eternity4 / eternity5 / eternity6)

Each node's device path is embedded in the `--peers` string as `host:port:device`.
The mapping is: eternity4→`/dev/nvme0n1`, eternity5→`/dev/nvme1n1`, eternity6→`/dev/nvme2n1`.
All nodes must receive the identical `--peers` string.

Start followers first so the cluster is stable before the benchmark driver joins.
Get the partition offset with: `sudo fdisk -l /dev/nvme<X>n1` — multiply the "Start" sector by 512.

```console
# node 0 — eternity4 (benchmark driver, /dev/nvme0n1)
sudo ./bench_multi_nvmeof \
  --id=0 \
  --peers=10.0.0.4:4020:/dev/nvme0n1,10.0.0.5:4020:/dev/nvme1n1,10.0.0.6:4020:/dev/nvme2n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576 \
  --entries=100000 --batch=16 --payload=65536 --threads=4 \
  --heartbeat-ms=100

# node 1 — eternity5 (follower, /dev/nvme1n1)
sudo ./bench_multi_nvmeof \
  --id=1 \
  --peers=10.0.0.4:4020:/dev/nvme3n1,10.0.0.5:4020:/dev/nvme2n1,10.0.0.6:4020:/dev/nvme4n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576

# node 2 — eternity6 (follower, /dev/nvme2n1)
sudo ./bench_multi_nvmeof \
  --id=2 \
  --peers=10.0.0.4:4020:/dev/nvme1n1,10.0.0.5:4020:/dev/nvme2n1,10.0.0.6:4020:/dev/nvme0n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576
```

### TCP version

Replace `bench_multi_nvmeof` with `bench_multi_nvmeof_tcp`:

```console
# node 0 — eternity4 (benchmark driver)
sudo ./bench_multi_nvmeof_tcp \
  --id=0 \
  --peers=eternity4:4020:/dev/nvme0n1,eternity5:4020:/dev/nvme1n1,eternity6:4020:/dev/nvme2n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576 \
  --entries=100000 --batch=256 --payload=8192 --threads=4 \
  --bench

# node 1 — eternity5 (follower)
sudo ./bench_multi_nvmeof_tcp \
  --id=1 \
  --peers=eternity4:4020:/dev/nvme0n1,eternity5:4020:/dev/nvme1n1,eternity6:4020:/dev/nvme2n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576

# node 2 — eternity6 (follower)
sudo ./bench_multi_nvmeof_tcp \
  --id=2 \
  --peers=eternity4:4020:/dev/nvme0n1,eternity5:4020:/dev/nvme1n1,eternity6:4020:/dev/nvme2n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576
```

## Flags

| Flag                  | Default               | Description                                                       |
|-----------------------|-----------------------|-------------------------------------------------------------------|
| `--id`                | `0`                   | 0-based index into `--peers` list (this node)                     |
| `--peers`             | *(required)*          | Comma-separated `host:port:device` list of all nodes              |
| `--metadata-dir`      | `./bench_nvmeof_data` | Directory for ring buffer `.dat` files (must be on NVMe)          |
| `--partition-offset`  | `0`                   | Partition start offset in bytes (`sector_start × 512`)            |
| `--entries`           | `10000`               | Total number of entries to submit (split across threads)          |
| `--batch`             | `256`                 | Commands per `Apply()` call per thread                            |
| `--payload`           | `8192`                | Payload size in bytes                                             |
| `--threads`           | `1`                   | Number of concurrent client goroutines                            |
| `--bench`             | `false`               | Run the benchmark when this node becomes leader                   |
| `--heartbeat-ms`      | `300`                 | Heartbeat interval in ms (lower wins elections faster)            |
| `--debug`             | `false`               | Verbose Raft debug logging                                        |

## Expected output

```
[node 1] started at eternity4:4020
[node 1] waiting to become stable leader...
[node 1] is stable leader — starting benchmark (10000 entries, batch=256, payload=8192, threads=4)

======= Experimental Parameters ======
10000 entries, Batch: 256, Payload: 8192
=== nvmeof_raft (PBA-based replication) ===
  Total time   : Xs
  Throughput   : Y entries/s
  Latency avg  : Zms
  Latency min  : ...
  Latency p50  : ...
  Latency p99  : ...
  Latency max  : ...
```

Only the leader node prints the result. Followers block until killed after the benchmark finishes.

## How it differs from bench_nvmeof

| Aspect      | bench_nvmeof        | multi_nvmeof                              |
|-------------|---------------------|-------------------------------------------|
| Concurrency | 1 goroutine         | `--threads` goroutines calling `Apply()`  |
| Entry split | Sequential          | Entries divided evenly across threads     |
| Use case    | Baseline latency    | Throughput scaling under parallel load    |

## Replication path

The network carries only RPC metadata (PBAs and term numbers), not log entry data. Followers copy data directly from the leader's NVMe device using O_DIRECT, bypassing the page cache and filesystem entirely. Since every server has NVMe-oF access to every device, the follower reads from the leader's device path and writes to its own device path.

```
Leader                            Follower
  Apply(batch)
    → FIEMAP: logicalOffset → PBA (on leader's device)
    → AppendEntries RPC (PBA + leaderDevicePath, no data)
                                    → O_DIRECT read from leader's device (leaderPbaSrc)
                                    → O_DIRECT write to follower's own device (followerPbaDst)
    ← quorum ack
  return
```
