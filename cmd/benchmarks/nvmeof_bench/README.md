# bench_nvmeof

Benchmarks the NVMe-oF optimized Raft implementation (`nvmeof_raft`).
It is the performance counterpart to `bench_goraft` and measures the
speedup achieved by PBA-based (Physical Block Address) log replication
over the file-based baseline.

The benchmark runs a 3-node Raft cluster, elects a leader, and measures
throughput/latency for replicating entries. The number of entries, batch size,
and payload size are configurable via flags (defaults: 10,000 entries, 256 per
batch, 8 KB each).

## Requirements

- 3 nodes, each with a dedicated NVMe(-oF) block device
- Root or `disk` group access to the block device (O_DIRECT writes)
- The metadata directory must reside on the **same NVMe partition** as the
  block device so that `FIEMAP` ioctl can resolve physical block addresses
- `sudo` is required to open the block device with O_DIRECT

## Build

The source uses the `raft` build tag to compile the NVMe-oF path.
Run from the project root (`~/nvmeof_raft`):ß

```console
GOPATH=~/go
# RDMA version
go build -tags raft -o bench_nvmeof ./cmd/benchmarks/nvmeof_bench/
# TCP version
go build -tags raft_tcp -o bench_nvmeof_tcp ./cmd/benchmarks/nvmeof_bench/
```

The binary is placed in the current directory.

## Run

### Distributed cluster (intended use)

Each node's device path is embedded in the `--peers` string as `host:port:device`.
Every node receives the full peer list, so followers know which device to read
from when doing cross-device PBA block copy.

Pass `--bench` on one or all nodes — whichever wins the leader election
runs the benchmark; the others serve as followers.

The partition offset is `sector_start × 512` (bytes). Retrieve it with:

```console
sudo fdisk -l /dev/nvme<X>n1
# note the "Start" sector of the target partition, then multiply by 512
```

Use `--entries`, `--batch`, and `--payload` to override the benchmark parameters.

```console
# node 0  (eternity4) — drives benchmark  (/dev/nvme0n1)
sudo ./bench_nvmeof \
  --id=0 \
  --peers=10.0.0.4:4020:/dev/nvme0n1,10.0.0.5:4020:/dev/nvme1n1,10.0.0.6:4020:/dev/nvme2n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576 \
  --entries=100000 --batch=16 --payload=65536 \
  --bench

# node 1  (eternity5) — follower  (/dev/nvme1n1)
sudo ./bench_nvmeof \
  --id=1 \
  --peers=10.0.0.4:4020:/dev/nvme3n1,10.0.0.5:4020:/dev/nvme2n1,10.0.0.6:4020:/dev/nvme4n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576

# node 2  (eternity6) — follower  (/dev/nvme2n1)
sudo ./bench_nvmeof \
  --id=2 \
  --peers=10.0.0.4:4020:/dev/nvme1n1,10.0.0.5:4020:/dev/nvme2n1,10.0.0.6:4020:/dev/nvme0n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576
```

#### TCP Version

```console
# node 0  (eternity4) — drives benchmark
sudo ./bench_nvmeof_tcp \
  --id=0 \
  --peers=eternity4:4020:/dev/nvme0n1,eternity5:4020:/dev/nvme1n1,eternity6:4020:/dev/nvme2n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576 \
  --entries=100000 --batch=256 --payload=8192 \
  --bench

# node 1  (eternity5)
sudo ./bench_nvmeof_tcp \
  --id=1 \
  --peers=eternity4:4020:/dev/nvme0n1,eternity5:4020:/dev/nvme1n1,eternity6:4020:/dev/nvme2n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576

# node 2  (eternity6)
sudo ./bench_nvmeof_tcp \
  --id=2 \
  --peers=eternity4:4020:/dev/nvme0n1,eternity5:4020:/dev/nvme1n1,eternity6:4020:/dev/nvme2n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576
```

#### TCP Version with IPoIB

```console
# node 0  (eternity4) — drives benchmark
sudo ./bench_nvmeof_tcp \
  --id=0 \
  --peers=10.0.0.4:4020:/dev/nvme0n1,10.0.0.5:4020:/dev/nvme1n1,10.0.0.6:4020:/dev/nvme2n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576 \
  --entries=100000 --batch=256 --payload=8192 \
  --bench

# node 1  (eternity5)
sudo ./bench_nvmeof_tcp \
  --id=1 \
  --peers=10.0.0.4:4020:/dev/nvme0n1,10.0.0.5:4020:/dev/nvme1n1,10.0.0.6:4020:/dev/nvme2n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576

# node 2  (eternity6)
sudo ./bench_nvmeof_tcp \
  --id=2 \
  --peers=10.0.0.4:4020:/dev/nvme0n1,10.0.0.5:4020:/dev/nvme1n1,10.0.0.6:4020:/dev/nvme2n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576
```

## Flags

| Flag                 | Default                | Description                                                  |
|----------------------|------------------------|--------------------------------------------------------------|
| `--id`               | `0`                    | 0-based index into `--peers` list (this node)                |
| `--peers`            | *(required)*           | Comma-separated `host:port:device` list of all nodes         |
| `--metadata-dir`     | `./bench_nvmeof_data`  | Directory for ring buffer `.dat` files (must be on NVMe)     |
| `--partition-offset` | `0`                    | Partition start offset in bytes (`sector_start × 512`)       |
| `--entries`          | `10000`                | Number of entries to submit                                  |
| `--batch`            | `256`                  | Commands per `Apply()` call                                  |
| `--payload`          | `8192`                 | Payload size in bytes                                        |
| `--bench`            | `false`                | Run the benchmark when this node becomes leader              |
| `--debug`            | `false`                | Verbose Raft debug logging                                   |

## Expected output

```
[node 1] started at eternity4:4020
[node 1] waiting to become stable leader...
[node 1] is stable leader — starting benchmark (10000 entries, batch=256, payload=8192)

=== nvmeof_raft (PBA-based replication) ===
  Entries      : 10000
  Total time   : Xs
  Throughput   : Y entries/s
  Latency avg  : Zms
  Latency min  : ...
  Latency p50  : ...
  Latency p99  : ...
  Latency max  : ...
```

## How it differs from bench_goraft

| Aspect              | bench_goraft                  | bench_nvmeof                        |
|---------------------|-------------------------------|-------------------------------------|
| Replication path    | Serialize entry → TCP → write | FIEMAP PBA lookup → O_DIRECT copy   |
| Log storage         | Single `.dat` file per node   | Ring buffer `.dat` + NVMe partition |
| Device requirement  | Any writable directory        | NVMe block device + disk group      |
| Build tag           | *(none)*                      | `-tags raft`                        |


## 5-node test command

```console
# node 0 (eternity4) — drives benchmark
sudo ./bench_nvmeof \
  --id=0 \
  --peers=eternity4:4020:/dev/nvme0n1,eternity5:4020:/dev/nvme1n1,eternity6:4020:/dev/nvme2n1,eternity2:4020:/dev/nvme0n1,eternity7:4020:/dev/nvme1n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576 \
  --entries=100000 --batch=16 --payload=65536 \
  --bench

# node 1 (eternity5)
sudo ./bench_nvmeof \
  --id=1 \
  --peers=eternity4:4020:/dev/nvme0n1,eternity5:4020:/dev/nvme1n1,eternity6:4020:/dev/nvme2n1,eternity2:4020:/dev/nvme0n1,eternity7:4020:/dev/nvme1n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576

# node 2 (eternity6)
sudo ./bench_nvmeof \
  --id=2 \
  --peers=eternity4:4020:/dev/nvme0n1,eternity5:4020:/dev/nvme1n1,eternity6:4020:/dev/nvme2n1,eternity2:4020:/dev/nvme0n1,eternity7:4020:/dev/nvme1n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576

# node 3 (eternity2)
sudo ./bench_nvmeof \
  --id=3 \
  --peers=eternity4:4020:/dev/nvme0n1,eternity5:4020:/dev/nvme1n1,eternity6:4020:/dev/nvme2n1,eternity2:4020:/dev/nvme0n1,eternity7:4020:/dev/nvme1n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576

# node 4 (eternity7)
sudo ./bench_nvmeof \
  --id=4 \
  --peers=eternity4:4020:/dev/nvme0n1,eternity5:4020:/dev/nvme1n1,eternity6:4020:/dev/nvme2n1,eternity2:4020:/dev/nvme0n1,eternity7:4020:/dev/nvme1n1 \
  --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
  --partition-offset=1048576
```
