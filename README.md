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
1. Raft Server Build
1) Move to main directory
cd ~/nvmeof_raft

2) Build (It must has `cmd/main.go`)
2-1. remote test
go build -tags raft -o raft_server ./cmd/remotetest

2-2. 

