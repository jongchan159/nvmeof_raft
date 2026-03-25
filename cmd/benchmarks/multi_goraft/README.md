# multi_goraft

Multi-threaded benchmark for the reference [`eatonphil/goraft`](https://github.com/eatonphil/goraft) file-based Raft implementation.

Extends `bench_goraft` with a `--threads` flag that spawns concurrent client goroutines, each calling `Apply()` independently on the leader. Use this to measure throughput scaling under parallel load.

Defaults: 10,000 entries, 256 per batch, 8 KB payload, 1 thread.

## Build

From the project root (`~/nvmeof_raft`):

```console
GOPATH=~/go
go build -tags raft -o bench_multi_goraft ./cmd/benchmarks/multi_goraft/
go build -tags raft_tcp -o bench_multi_goraft_tcp ./cmd/benchmarks/multi_goraft/
```

## Run

### 3-node cluster (eternity4 / eternity5 / eternity6)

Start followers first so the leader is stable before the benchmark begins.

```console
# node 0 — eternity4 (benchmark driver)
sudo ./bench_multi_goraft \
  --id=0 \
  --peers=10.0.0.4:4020,10.0.0.5:4021,10.0.0.6:4022 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft \
  --entries=100000 --batch=256 --payload=8192 --threads=8 \
  --bench

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

### 5-node cluster (eternity4 / eternity5 / eternity6 / eternity2 / eternity7)

Start the four followers first, then the benchmark driver.

```console
# node 0 — eternity4 (benchmark driver)
sudo ./bench_multi_goraft \
  --id=0 \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022,eternity2:4023,eternity7:4024 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft \
  --entries=100000 --batch=256 --payload=8192 --threads=8 \
  --bench

# node 1 — eternity5 (follower)
sudo ./bench_multi_goraft \
  --id=1 \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022,eternity2:4023,eternity7:4024 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft

# node 2 — eternity6 (follower)
sudo ./bench_multi_goraft \
  --id=2 \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022,eternity2:4023,eternity7:4024 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft

# node 3 — eternity2 (follower)
sudo ./bench_multi_goraft \
  --id=3 \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022,eternity2:4023,eternity7:4024 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft

# node 4 — eternity7 (follower)
sudo ./bench_multi_goraft \
  --id=4 \
  --peers=eternity4:4020,eternity5:4021,eternity6:4022,eternity2:4023,eternity7:4024 \
  --metadata-dir=/mnt/nvmeof_raft/bench_goraft

```

### Local single-machine test

```console
PEERS="localhost:4020,localhost:4021,localhost:4022"

./bench_multi_goraft --id=1 --peers=$PEERS --metadata-dir=/dev/shm/goraft1 &
./bench_multi_goraft --id=2 --peers=$PEERS --metadata-dir=/dev/shm/goraft2 &
./bench_multi_goraft --id=0 --peers=$PEERS --metadata-dir=/dev/shm/goraft0 \
  --entries=10000 --batch=256 --payload=8192 --threads=2 --bench &
wait
```

Use `/dev/shm` (tmpfs) to avoid O_SYNC stalls on spinning disks. On `/tmp` a 256-entry batch persist takes ~7 s and causes repeated re-elections.

## Flags

| Flag             | Default               | Description                                              |
|------------------|-----------------------|----------------------------------------------------------|
| `--id`           | `0`                   | 0-based index into `--peers` list (this node)           |
| `--peers`        | *(required)*          | Comma-separated `host:port` list of all nodes           |
| `--metadata-dir` | `./bench_goraft_data` | Directory for the Raft log `.dat` file                  |
| `--entries`      | `10000`               | Total number of entries to submit (split across threads)|
| `--batch`        | `256`                 | Commands per `Apply()` call per thread                  |
| `--payload`      | `8192`                | Payload size in bytes                                   |
| `--threads`      | `1`                   | Number of concurrent client goroutines                  |
| `--bench`        | `false`               | Run the benchmark when this node becomes leader         |
| `--debug`        | `false`               | Verbose Raft debug logging                              |

## Expected output

```
[node 1] started at eternity4:4020
[node 1] waiting to become stable leader...
[node 1] is stable leader — starting benchmark (10000 entries, batch=256, payload=8192, threads=4)

======= Experimental Parametesr =======
10000 entires, Batch: 256, Payload: 8192
=== goraft (file-based replication) ===
  Total time   : 4.2s
  Throughput   : 2380.00 entries/s
  Latency avg  : 20ms
  Latency min  : 3ms
  Latency p50  : 20ms
  Latency p99  : 47ms
  Latency max  : 47ms
```

Only the leader node prints the result. Followers block until killed after the benchmark finishes.

## How it differs from bench_goraft

| Aspect      | bench_goraft        | multi_goraft                              |
|-------------|---------------------|-------------------------------------------|
| Concurrency | 1 goroutine         | `--threads` goroutines calling `Apply()`  |
| Entry split | Sequential          | Entries divided evenly across threads     |
| Use case    | Baseline latency    | Throughput scaling under parallel load    |
