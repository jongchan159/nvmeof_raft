package main

import (
	"bytes"
	crypto "crypto/rand"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"flag"
	"time"
	"github.com/thanhphu/raftbench/util"
	"github.com/eatonphil/goraft"
)

type statemachine struct {
	db     *sync.Map
	server int
}

type commandKind uint8

const (
	setCommand commandKind = iota
	getCommand
)

type command struct {
	kind  commandKind
	key   string
	value string
}

func encodeCommand(c command) []byte {
	msg := bytes.NewBuffer(nil)
	err := msg.WriteByte(uint8(c.kind))
	if err != nil {
		panic(err)
	}

	err = binary.Write(msg, binary.LittleEndian, uint64(len(c.key)))
	if err != nil {
		panic(err)
	}

	msg.WriteString(c.key)

	err = binary.Write(msg, binary.LittleEndian, uint64(len(c.value)))
	if err != nil {
		panic(err)
	}

	msg.WriteString(c.value)

	return msg.Bytes()
}
func benchWrite(s *goraft.Server, key string, v int) bool {
    c := command{
        kind:  setCommand,
        key:   key,
        value: fmt.Sprintf("%d", v),
    }
    _, err := s.Apply([][]byte{encodeCommand(c)})
    return err == nil
}

func benchRead(s *goraft.Server, key string) bool {
    c := command{
        kind: getCommand,
        key:  key,
    }
    _, err := s.Apply([][]byte{encodeCommand(c)})
    return err == nil
}

func decodeCommand(msg []byte) command {
	var c command
	c.kind = commandKind(msg[0])

	keyLen := binary.LittleEndian.Uint64(msg[1:9])
	c.key = string(msg[9 : 9+keyLen])

	if c.kind == setCommand {
		valLen := binary.LittleEndian.Uint64(msg[9+keyLen : 9+keyLen+8])
		c.value = string(msg[9+keyLen+8 : 9+keyLen+8+valLen])
	}

	return c
}


func (s *statemachine) Apply(cmd []byte) ([]byte, error) {
    c := decodeCommand(cmd)

    switch c.kind {
    case setCommand:
        s.db.Store(c.key, c.value)
        fmt.Printf("[APPLY] SET key=%q value=%s\n", c.key, c.value) // ✅ 추가
    case getCommand:
        value, ok := s.db.Load(c.key)
        if !ok {
            fmt.Printf("[APPLY] GET key=%q -> MISS\n", c.key) // ✅ 추가
            return nil, fmt.Errorf("Key not found")
        }
        fmt.Printf("[APPLY] GET key=%q -> %s\n", c.key, value.(string)) // ✅ 추가
        return []byte(value.(string)), nil
    default:
        return nil, fmt.Errorf("Unknown command: %x", cmd)
    }

    return nil, nil
}

type httpServer struct {
	raft *goraft.Server
	db   *sync.Map
}

// Example:
//
//	curl http://localhost:2020/set?key=x&value=1
func (hs httpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	var c command
	c.kind = setCommand
	c.key = r.URL.Query().Get("key")
	c.value = r.URL.Query().Get("value")

	_, err := hs.raft.Apply([][]byte{encodeCommand(c)})
	if err != nil {
		log.Printf("Could not write key-value: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
}

// Example:
//
//	curl http://localhost:2020/get?key=x
//	1
//	curl http://localhost:2020/get?key=x&relaxed=true # Skips consensus for the read.
//	1
func (hs httpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	var c command
	c.kind = getCommand
	c.key = r.URL.Query().Get("key")

	var value []byte
	var err error
	if r.URL.Query().Get("relaxed") == "true" {
		v, ok := hs.db.Load(c.key)
		if !ok {
			err = fmt.Errorf("Key not found")
		} else {
			value = []byte(v.(string))
		}
	} else {
		var results []goraft.ApplyResult
		results, err = hs.raft.Apply([][]byte{encodeCommand(c)})
		if err == nil {
			if len(results) != 1 {
				err = fmt.Errorf("Expected single response from Raft, got: %d.", len(results))
			} else if results[0].Error != nil {
				err = results[0].Error
			} else {
				value = results[0].Result
			}

		}
	}

	if err != nil {
		log.Printf("Could not encode key-value in http response: %s", err)
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}

	written := 0
	for written < len(value) {
		n, err := w.Write(value[written:])
		if err != nil {
			log.Printf("Could not encode key-value in http response: %s", err)
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		written += n
	}
}

type config struct {
	cluster []goraft.ClusterMember
	index   int
	id      string
	address string
	http    string
}

func getConfig() config {
	cfg := config{}
	var node string
	for i, arg := range os.Args[1:] {
		if arg == "--node" {
			var err error
			node = os.Args[i+2]
			cfg.index, err = strconv.Atoi(node)
			if err != nil {
				log.Fatal("Expected $value to be a valid integer in `--node $value`, got: %s", node)
			}
			i++
			continue
		}

		if arg == "--http" {
			cfg.http = os.Args[i+2]
			i++
			continue
		}

		if arg == "--cluster" {
			cluster := os.Args[i+2]
			var clusterEntry goraft.ClusterMember
			for _, part := range strings.Split(cluster, ";") {
				idAddress := strings.Split(part, ",")
				var err error
				clusterEntry.Id, err = strconv.ParseUint(idAddress[0], 10, 64)
				if err != nil {
					log.Fatal("Expected $id to be a valid integer in `--cluster $id,$ip`, got: %s", idAddress[0])
				}

				clusterEntry.Address = idAddress[1]
				cfg.cluster = append(cfg.cluster, clusterEntry)
			}

			i++
			continue
		}
	}

	if node == "" {
		log.Fatal("Missing required parameter: --node $index")
	}

	if cfg.http == "" {
		log.Fatal("Missing required parameter: --http $address")
	}

	if len(cfg.cluster) == 0 {
		log.Fatal("Missing required parameter: --cluster $node1Id,$node1Address;...;$nodeNId,$nodeNAddress")
	}

	return cfg
}

func main() {
    // ---- 1) 플래그 ----
    id := flag.Int("id", 0, "node ID (unique per node)")
    cluster := flag.String("cluster",
        "eternity4:2020,eternity6:2021,eternity9:2022",
        "comma-separated cluster addresses (order must match ids 4,6,9)")
    httpAddr := flag.String("http", ":8080", "HTTP service address")
    dataDir := flag.String("data", "./data", "data directory for this node")

    bench := flag.Bool("bench", false, "run benchmark using util.Bench")
    numKeys := flag.Int("numKeys", 10, "number of distinct keys")
    mil := flag.Int("mil", 10000, "loops per key (total ops = numKeys * mil)")
    runs := flag.Int("runs", 3, "number of benchmark runs")
    wait := flag.Int("wait", 2000, "wait(ms) between phases and runs)")
    firstWait := flag.Int("firstWait", 5000, "warm-up wait(ms) before starting benchmark")
    step := flag.Int("step", 100, "backoff step (ms) on retry")
    maxTries := flag.Int("maxTries", 10, "max retries per op")
    logFile := flag.String("logfile", "result.csv", "csv log file path")
    flag.Parse()

    // ---- 2) 랜덤 시드 ----
    var b [8]byte
    if _, err := crypto.Read(b[:]); err != nil {
        panic("cannot seed math/rand package with cryptographically secure random number generator")
    }
    rand.Seed(int64(binary.LittleEndian.Uint64(b[:])))

    // ---- 3) 클러스터 구성 (주소→멤버). ID는 4,6,9로 고정 매핑 ----
    addrs := strings.Split(*cluster, ",")
    ids := []uint64{4, 6, 9} // 필요하면 여기 바꿔
    if len(addrs) != len(ids) {
      log.Fatalf("len(cluster)=%d != len(ids)=%d", len(addrs), len(ids))
    }
    var clusterConfig []goraft.ClusterMember
    for i, addr := range addrs {
        clusterConfig = append(clusterConfig, goraft.ClusterMember{
            Id:      ids[i],
            Address: addr,
        })
    }
    // 내 id의 인덱스(0-based) 찾기
    clusterIndex := -1
    for i, uid := range ids {
        if uid == uint64(*id) {
            clusterIndex = i
            break
        }
    }
    if clusterIndex == -1 {
        log.Fatalf("--id=%d not in ids=%v (cluster=%v)", *id, ids, addrs)
    }

    // ---- 4) 서버/HTTP 시작 ----
    var db sync.Map
    sm := &statemachine{db: &db, server: *id}

    s := goraft.NewServer(clusterConfig, sm, *dataDir, clusterIndex)
    s.Debug = true
    go s.Start()

    hs := httpServer{s, &db}
    http.HandleFunc("/set", hs.setHandler)
    http.HandleFunc("/get", hs.getHandler)
    go func() {
        if err := http.ListenAndServe(*httpAddr, nil); err != nil {
            log.Fatalf("http server error: %v", err)
        }
    }()

    // ---- 5) 리더가 되면 벤치 실행 ----
    if *bench {
        log.Println("[INFO] Waiting for leader election...")
        for {
            if s.IsLeader() { // goraft에 IsLeader() 이미 추가되어 있어야 함
                log.Println("[INFO] Node became leader, starting benchmark!")
                break
            }
            time.Sleep(1 * time.Second)
        }

        test := util.TestParams{
            NumKeys:   *numKeys,
            Mil:       *mil,
            Runs:      *runs,
            Wait:      time.Duration(*wait) * time.Millisecond,
            FirstWait: time.Duration(*firstWait) * time.Millisecond,
            Step:      time.Duration(*step) * time.Millisecond,
            MaxTries:  *maxTries,
            Enabled:   true,
            LogFile:   *logFile,
        }
util.Bench(test,
    func(k string) bool {
        return benchRead(s, k)
    },
    func(k, v string) bool {
        return benchWrite(s, k, rand.Int())
    },
)
    }

    // 종료 방지
    select {}
}

