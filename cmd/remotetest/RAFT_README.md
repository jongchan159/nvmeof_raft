# NVMe-oF Optimized Raft Server

## 빌드

```bash
cd ~/nvmeof_raft

# Go 모듈 초기화 (처음 한 번만)
go mod init nvmeof_raft
go mod tidy

# main.go를 cmd/ 디렉토리에 배치
mkdir -p cmd
cp main.go cmd/

# 빌드
go build -o raft_server ./cmd

# 또는 직접 빌드
go build -o raft_server main.go
```

## 3노드 클러스터 실행

### Node 1 (eternity4)
```bash
sudo ./raft_server --id=4 --address=eternity4:7004 \
  --peers=eternity4:7004,eternity5:7005,eternity6:7006 \
  --metadata-dir=/mnt/nvmeof_raft/metadata4 \
  --device=/dev/nvme1n1 \
  --partition-offset=1048576 \
  --debug
```

### Node 2 (eternity5)
```bash
sudo ./raft_server --id=5 --address=eternity5:7005 \
  --peers=eternity4:7004,eternity5:7005,eternity6:7006 \
  --metadata-dir=/mnt/nvmeof_raft/metadata5 \
  --device=/dev/nvme2n1 \
  --partition-offset=10738466816 \
  --debug
```

### Node 3 (eternity6)
```bash
sudo ./raft_server --id=6 --address=eternity6:7006 \
  --peers=eternity4:7004,eternity5:7005,eternity6:7006 \
  --metadata-dir=/mnt/nvmeof_raft/metadata6 \
  --device=/dev/nvme0n1 \
  --partition-offset=21475885056 \
  --debug
```

## 명령어 플래그

| Flag | 설명 | 예시 | 필수 |
|------|------|------|------|
| `--id` | 노드 ID (0이 아닌 숫자) | `--id=5` | ✅ |
| `--address` | 이 노드의 주소 | `--address=localhost:7001` | ✅ |
| `--peers` | 클러스터의 모든 노드 (쉼표로 구분) | `--peers=node1:7001,node2:7002` | ✅ |
| `--metadata-dir` | 메타데이터 저장 디렉토리 | `--metadata-dir=./data` | ❌ (기본: ./metadata) |
| `--debug` | 디버그 로그 활성화 | `--debug` | ❌ |

## State Machine

현재 구현된 State Machine은 간단한 Key-Value 스토어입니다:

- `SET key value` - 키에 값 저장
- `GET key` - 키의 값 조회

### 명령 전송 (Go 코드에서)

```go
// Client 코드 예시
results, err := server.Apply([][]byte{
    []byte("SET name alice"),
    []byte("SET age 30"),
})

result, err := server.Apply([][]byte{
    []byte("GET name"),
})
// result: []byte("alice")
```

## 로그 확인

```bash
# 메타데이터 파일 확인
ls -la /mnt/nvme0n1/jongc/nvmeof_raft/metadata5/

# Raft 로그 파일
# md_5.dat - Node 5의 persistent state
```

## 종료

```
Ctrl+C
```

## 트러블슈팅

### 문제 1: "bind: address already in use"
```bash
# 포트가 이미 사용 중
lsof -i :7005
kill <PID>
```

### 문제 2: "permission denied" (metadata directory)
```bash
# 디렉토리 권한 확인
sudo chown -R $USER:$USER /mnt/nvme0n1/jongc/nvmeof_raft/
```

### 문제 3: 노드가 서로 연결 안 됨
```bash
# 방화벽 확인
sudo ufw status

# 포트 허용
sudo ufw allow 7004:7006/tcp

# 네트워크 확인
ping eternity5
telnet eternity5 7005
```

## 다음 단계

1. **Client 코드 작성**: Raft 클러스터에 명령 전송
2. **Blockcopy 통합**: NVMe-oF 블록 복사 활성화
3. **성능 테스트**: 표준 Raft vs 최적화된 Raft 비교
4. **장애 테스트**: 노드 장애 시나리오

## 참고

- Raft 논문: https://raft.github.io/
- 이 프로젝트는 NVMe-oF를 활용한 로그 복제 최적화 구현
