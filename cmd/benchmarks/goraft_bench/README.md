# bench_goraft

Benchmarks the reference [`eatonphil/goraft`](https://github.com/eatonphil/goraft) implementation.
This serves as the baseline comparison target for `bench_nvmeof`.

The benchmark runs a 3-node Raft cluster, elects a leader, and measures
throughput/latency for replicating entries. The number of entries, batch size,
and payload size are configurable via flags (defaults: 10,000 entries, 256 per
batch, 8 KB each).

## Build

From the project root (`~/nvmeof_raft`):

```console
GOPATH=~/go

# rmda RPC version
go build -o bench_goraft -tags raft ./cmd/benchmarks/goraft_bench/

# tcp/ip RPC version
go build -o bench_goraft_tcp -tags raft_tcp ./cmd/benchmarks/goraft_bench/
```

The binary is placed in the current directory.

## Run

### Distributed cluster (intended use)

Run one instance per node. Pass `--bench` on the node that should drive the
benchmark — whichever node wins the leader election runs it.

Use `--entries`, `--batch`, and `--payload` to override the benchmark parameters.

```console
# node 0  (eternity4)
sudo ./bench_goraft --id=0 \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft \
  --entries=100000 --batch=16 --payload=65536 \
  --bench

# node 1  (eternity5)
sudo ./bench_goraft --id=1 \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft

# node 2  (eternity6)
sudo ./bench_goraft --id=2 \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft
```

#### TCP version

```console
# node 0  (eternity4)
sudo ./bench_goraft_tcp --id=0 \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft \
  --entries=100000 --batch=256 --payload=8192 \
  --bench

# node 1  (eternity5)
sudo ./bench_goraft_tcp --id=1 \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft

# node 2  (eternity6)
sudo ./bench_goraft_tcp --id=2 \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft
```

#### TCP version with IPoIB

```console
# node 0  (eternity4)
sudo ./bench_goraft_tcp --id=0 \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft \
  --entries=100000 --batch=256 --payload=256 \
  --bench

# node 1  (eternity5)
sudo ./bench_goraft_tcp --id=1 \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft

# node 2  (eternity6)
sudo ./bench_goraft_tcp --id=2 \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft
```

### Local single-machine test

Use a RAM-backed filesystem (`/dev/shm`) as the metadata directory.
The log file is opened with `O_SYNC`, so each write is flushed to the
underlying storage device. On a spinning HDD (`/tmp`) a 256-entry
batch persist takes ~7 s and repeatedly triggers re-elections, so the
benchmark stalls. On `/dev/shm` (tmpfs) the same persist takes < 1 ms.

```console
PEERS="localhost:4020,localhost:4021,localhost:4022"

./bench_goraft --id=0 --peers=$PEERS --metadata-dir=/dev/shm/goraft0 \
  --entries=10000 --batch=256 --payload=8192 --bench &
./bench_goraft --id=1 --peers=$PEERS --metadata-dir=/dev/shm/goraft1 &
./bench_goraft --id=2 --peers=$PEERS --metadata-dir=/dev/shm/goraft2 &
wait
```

Only the leader node prints the result. The other two nodes block until
killed after the leader finishes.

## Flags

| Flag             | Default               | Description                                      |
|------------------|-----------------------|--------------------------------------------------|
| `--id`           | `0`                   | 0-based index into `--peers` list (this node)    |
| `--peers`        | *(required)*          | Comma-separated `host:port` list of all 3 nodes  |
| `--metadata-dir` | `./bench_goraft_data` | Directory for the Raft log `.dat` file           |
| `--entries`      | `10000`               | Number of entries to submit                      |
| `--batch`        | `256`                 | Commands per `Apply()` call                      |
| `--payload`      | `8192`                | Payload size in bytes                            |
| `--bench`        | `false`               | Run the benchmark when this node becomes leader  |
| `--debug`        | `false`               | Verbose Raft debug logging                       |

## Expected output

```
[node 1] started at localhost:4020
[node 1] waiting to become stable leader...
[node 1] is stable leader — starting benchmark (10000 entries, batch=256, payload=8192)

======= Experimental Parameter ========
100000 entires, Batch: 256, Payload: 8196
=== goraft (file-based replication) ===
  Total time   : 1m36.900981163s
  Throughput   : 1031.98 entries/s
  Latency avg  : 162.559039ms
  Latency min  : 99.141961ms
  Latency p50  : 161.284558ms
  Latency p99  : 185.819097ms
  Latency max  : 199.88593ms
```

> Numbers above are from a local `/dev/shm` run.
> On a real distributed NVMe-oF cluster the numbers will differ.
