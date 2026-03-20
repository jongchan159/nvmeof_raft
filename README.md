## Configurance
1. System Configurance
Ubuntu 18.04.5 LTS


2. Run Raft Cluster
- setting

eternitystorage: storage node

eternity4,5,6: computing nodes (raft server)

## BLOCKCOPY
### Test Code
1. TestGetPBA & TestBlkCp
```console
cd ~/nvmeof_raft/blockcopy

#TestGetPBA
go test -v -run TestGetPBA_Complete

#TestBlkCp
sudo -E go test -v -run TestBlkCp_Complete
```

2. Test step-by-step
```console
#TestGetPBA
go test -v -run TestGetPBA_Step1
go test -v -run TestGetPBA_Step2
go test -v -run TestGetPBA_Step3

#TestBlkCp
sudo -E go test -v -run TestBlkCp_Step1
sudo -E go test -v -run TestBlkCp_Step2
sudo -E go test -v -run TestBlkCp_Step3
sudo -E go test -v -run TestBlkCp_Step4
```

4. cleanup
```console
go test -v -run TestGetPBA_Cleanup
go test -v -run TestBlkCp_Cleanup
```

### Running Raft
#### Raft Server Build
1) Move to main directory
cd ~/nvmeof_raft

2) Build (It must has `cmd/main.go`)

2-1. remote test

go build -tags raft -o raft_server ./cmd/remotetest

2-2. metadata parsing

go build -o mdparse ./cmd/mdparse

2-3 stress tesst

go build -tags raft -o pbastress ./cmd/benchmarks/pbastress/main.go
sudo ./pbastress

2-4. bench test
go build -o bench_nvmeof -tags raft  ./cmd/benchmarks/bench_nvmeof/main.go
go build -o bench_goraft  ./cmd/benchmarks/bench_goraft/main.go
\

# eternity4
sudo ./bench_goraft --id=0 \
--peers=eternity4:4020,eternity5:4021,eternity6:4022 \
--metadata-dir=/mnt/nvmeof_raft/bench_goraft --bench

# eternity5
sudo ./bench_goraft --id=1 \
--peers=eternity4:4020,eternity5:4021,eternity6:4022 \
--metadata-dir=/mnt/nvmeof_raft/bench_goraft --bench

# eternity6
sudo ./bench_goraft --id=2 \
--peers=eternity4:4020,eternity5:4021,eternity6:4022 \
--metadata-dir=/mnt/nvmeof_raft/bench_goraft --bench


bench_nvmeof (distributed)

# eternity4 (node 0) — drives benchmark
sudo ./bench_nvmeof --id=0 --peers=eternity4:4020,eternity5:4021,eternity6:4022 \
    --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
    --device=/dev/nvme1n1 --partition-offset=1048576 --bench

# eternity5 (node 1)
sudo ./bench_nvmeof --id=1 --peers=eternity4:4020,eternity5:4021,eternity6:4022 \
    --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
    --device=/dev/nvme2n1 --partition-offset=10738466816

# eternity6 (node 2)
sudo ./bench_nvmeof --id=2 --peers=eternity4:4020,eternity5:4021,eternity6:4022 \
    --metadata-dir=/mnt/nvmeof_raft/bench_nvmeof \
    --device=/dev/nvme0n1 --partition-offset=21475885056