## How to Set NVMe-overRDMA
### If initiator server was rebooted

Reload RDMA/IB modules                                                                 
                                                                                        
```console
sudo modprobe ib_core
sudo modprobe ib_uverbs                                                                   
sudo modprobe rdma_cm                                                                     
sudo modprobe nvme-rdma
``` 

if ib0 is down

```console
sudo nvme discover -t rdma -a 10.0.0.90 -s 4420
sudo ip link set ib0 up
sudo ip addr add 10.0.0.4/24 dev ib0
sudo nvme connect -t rdma -a 10.0.0.90 -s 4420 -n raft
```

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

go build -tags raft -o pbastress_sv ./cmd/benchmarks/pbastress/main.go
sudo ./pbastress_sv

2-4. bench test

go build -o bench_nvmeof -tags raft  ./cmd/benchmarks/bench_nvmeof/main.go

go build -o bench_goraft  ./cmd/benchmarks/bench_goraft/main.go