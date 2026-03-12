## Command
### Test Code
1. TestGetPBA
cd ~/nvmeof_raft/blockcopy
go test -v -run TestGetPBA_Complete

2. TestBlkCp
sudo -E go test -v -run TestBlkCp_Complete

3. Test step-by-step
go test -v -run TestGetPBA_Step1
go test -v -run TestGetPBA_Step2
go test -v -run TestGetPBA_Step3

sudo -E go test -v -run TestBlkCp_Step1
sudo -E go test -v -run TestBlkCp_Step2
sudo -E go test -v -run TestBlkCp_Step3
sudo -E go test -v -run TestBlkCp_Step4

4. cleanup
go test -v -run TestGetPBA_Cleanup
go test -v -run TestBlkCp_Cleanup

### Running Raft
1. Raft Server Build
1) Move to main directory
cd ~/nvmeof_raft

2) Build (It must has `cmd/main.go`)
go build -o raft_server ./cmd

 or directry excute
`go build -o raft_server`

2. Run Raft Cluster
- setting
	eternity4: storage node
	eternity5,6: computing nodes (raft server)

- Raft Configurance
	Leader: eternity5
	Followers: eternity6
```
./raft_server \
  --id=5 \
  --port=7005 \
  --peers=eternity5:7005,eternity6:7006,eternity4:7004 \
  --data-dir=/mnt/nvme0n1/jongc/nvmeof_raft/node5
```

3. Raft Test
# 리더에게 요청 전송
curl -X POST http://eternity5:7005/write \
  -H "Content-Type: application/json" \
  -d '{"key":"test", "value":"hello"}'

# 상태 확인
curl http://eternity5:7005/status

# 로그 확인
curl http://eternity5:7005/logs
