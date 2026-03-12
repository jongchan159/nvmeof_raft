package main

import (
	"fmt"
	"net/rpc"
	"time"

	"nvmeof_raft"
)

type DummySM struct{}

func (d DummySM) Apply(cmd []byte) ([]byte, error) { return []byte("ok"), nil }

func main() {
	cluster := []nvmeof_raft.ClusterMember{
		{Id: 1, Address: "127.0.0.1:9001"},
		{Id: 2, Address: "127.0.0.1:9002"},
	}

	// metadataDir는 아무 폴더나 (로컬 테스트용)
	l_md := "/home/jongc/nvmeof_raft/cmd/localtest"
	f_md := "/home/jongc/nvmeof_raft/cmd/localtest"
	sm := DummySM{}

	leader := nvmeof_raft.NewServer(cluster, sm, l_md, 0)
	follower := nvmeof_raft.NewServer(cluster, sm, f_md, 1)

	leader.Debug = true
	follower.Debug = true

	leader.Start()
	follower.Start()

	// 서버 뜰 시간 조금 주기
	time.Sleep(300 * time.Millisecond)

	// RPC dummy test
	client, err := rpc.DialHTTP("tcp", "127.0.0.1:9002")
	if err != nil {
		fmt.Println("dial err:", err)
		return
	}
	req := nvmeof_raft.AppendEntriesRequest{
		RPCMessage:   nvmeof_raft.RPCMessage{Term: 1},
		LeaderId:     1,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		// Entries:       nil,
		LeaderCommit:   0,
		LeaderPbaSrc:   1234,
		LogBlockLength: 7,
	}
	var rsp nvmeof_raft.AppendEntriesResponse
	err = client.Call("Server.HandleAppendEntriesRequest", req, &rsp)
	fmt.Println("call err:", err, "rsp:", rsp)

	// 프로세스가 바로 종료되면 로그 보기 어려우니 잠깐 잡아둠
	//time.Sleep(2 * time.Second)

	// 여기서부턴 raft test
	// ✅ 여기서 바로 강제 리더
	leader.ForceLeaderForTest()

	// ✅ follower는 절대 election 안 하게 electionTimeout을 아주 멀리 보내버리기(임시)
	time.Sleep(10 * time.Millisecond)
	follower.ForceFollowerNoElectionForTest() // 아래에 추가
	leader.TriggerAppendEntriesOnceForTest()
	// -> 두 번의 RECV AppendEntries가 나와야 함

	time.Sleep(2 * time.Second)

	fmt.Println("Ready. Now trigger test RPC ...")
	time.Sleep(10 * time.Second)
}
