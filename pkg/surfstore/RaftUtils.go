package surfstore

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"sync"

	"google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TODO Any initialization you need here

	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	server := RaftSurfstore{

		isLeader:      false,
		isLeaderMutex: &isLeaderMutex,

		// Persistent state on all servers
		term: 0,
		log:  make([]*UpdateOperation, 0),

		// state machine
		metaStore: NewMetaStore(config.BlockAddrs),

		// Added for discussion
		id:             id,
		peers:          config.RaftAddrs,
		pendingCommits: make([]*chan bool, 0),

		// Volatile state on all servers
		commitIndex: -1,
		lastApplied: -1,

		// // Volatile state on leaders
		// nextIndex:  make([]int64, len(config.RaftAddrs)),
		// matchIndex: make([]int64, len(config.RaftAddrs)),

		/*--------------- Chaos Monkey --------------*/
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
func ServeRaftServer(server *RaftSurfstore) error {
	grpcServer := grpc.NewServer()

	RegisterRaftSurfstoreServer(grpcServer, server)

	l, e := net.Listen("tcp", server.peers[server.id])
	if e != nil {
		return e
	}

	return grpcServer.Serve(l)
}
