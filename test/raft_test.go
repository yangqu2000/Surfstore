package SurfTest

import (
	"cse224/proj5/pkg/surfstore"
	"fmt"
	"strings"
	"testing"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {

		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})

		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Fatalf("Server %d should be in term %d", idx, 1)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(2) {
			t.Fatalf("Server should be in term %d", 2)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
}

func TestRaftFollowersGetUpdates(t *testing.T) {
	t.Log("leader1 gets a request")
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// here node 0 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	// update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// one final call to sendheartbeat (from spec)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// check that all the nodes are in the right state
	// leader and all followers should have the entry in the log
	// everything should be term 1
	// entry should be applied to all metastores
	// only node 0 should be leader
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta.Filename] = filemeta

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta,
	})

	term := int64(1)
	var leader bool
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

func TestRaftFollowersGetUpdatesTwice(t *testing.T) {
	t.Log("leader1 gets a request")
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// here node 0 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	// update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// one final call to sendheartbeat (from spec)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// check that all the nodes are in the right state
	// leader and all followers should have the entry in the log
	// everything should be term 1
	// entry should be applied to all metastores
	// only node 0 should be leader
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta.Filename] = filemeta

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta,
	})

	term := int64(1)
	var leader bool
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}

	// update a file on node 0 twice
	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       2,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta2)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// one final call to sendheartbeat (from spec)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// check that all the nodes are in the right state
	// leader and all followers should have the entry in the log
	// everything should be term 1
	// entry should be applied to all metastores
	// only node 0 should be leader
	goldenMeta2 := make(map[string]*surfstore.FileMetaData)
	goldenMeta2[filemeta.Filename] = filemeta2

	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta2,
	})

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta2, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

func TestRaftServerIsCrashable(t *testing.T) {
	t.Log("leader1 gets a request")
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// crash the leader
	test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})

	leaderIdx = 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// here node 0 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	// update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	_, err := test.Clients[0].UpdateFile(test.Context, filemeta)
	fmt.Println(fmt.Sprint(err), surfstore.ERR_SERVER_CRASHED.Error())

	if err == nil || strings.Split(err.Error(), "=")[2][1:] != surfstore.ERR_SERVER_CRASHED.Error() {
		t.Fatalf("Server should return ERR_SERVER_CRASHED")
	}

}

func TestRaftLogsConsistent(t *testing.T) {
	t.Log("leader1 gets a request")
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// crash the minority of the cluster
	crashedIdx := 2
	test.Clients[crashedIdx].Crash(test.Context, &emptypb.Empty{})

	// here node 0 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	// update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	_, err := test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
	if err != nil {
		fmt.Println("Error:", err)
	}

	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// one final call to sendheartbeat (from spec)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// check that all the nodes are in the right state
	// leader and all followers should have the entry in the log
	// everything should be term 1
	// entry should be applied to all metastores
	// only node 0 should be leader
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta.Filename] = filemeta

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta,
	})

	term := int64(1)
	var leader bool
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)

		if err != nil {
			// t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
			t.Logf("Error checking state for server %d: %s", idx, err.Error())
		}
	}

	t.Log("leader1 crashes")
	test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})

	t.Log("The other crashed nodes are restored")
	test.Clients[crashedIdx].Restore(test.Context, &emptypb.Empty{})

	t.Log("leader2 gets a request")
	leaderIdx = 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// update a file on node 0
	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       2,
		BlockHashList: nil,
	}

	_, err = test.Clients[leaderIdx].UpdateFile(test.Context, filemeta2)
	if err != nil {
		fmt.Println("Error:", err)
	}
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	t.Log("leader1 is restored")
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})

	// one final call to sendheartbeat (from spec)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// check that all the nodes are in the right state
	// leader and all followers should have the entry in the log
	// everything should be term 2
	// entry should be applied to all metastores
	// only node 0 should be leader
	term = int64(2)
	goldenMeta2 := make(map[string]*surfstore.FileMetaData)
	goldenMeta2[filemeta.Filename] = filemeta2

	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: filemeta2,
	})

	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta2, server, test.Context)

		if err != nil {
			t.Fatalf("2 Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

func TestRaftBlockWhenMajorityDown(t *testing.T) {
	t.Log("leader1 gets a request")
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	// here node 0 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	// update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	_, err := test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
	if strings.Split(err.Error(), "=")[2][1:] != "context deadline exceeded" {
		t.Fatalf("Request did not block")
	}
}

func TestRaftLogsCorrectlyOverwritten(t *testing.T) {

	t.Log("leader1 gets several requests while all other nodes are crashed. leader1 crashes. all other nodes are restored. leader2 gets a request. leader1 is restored.")

	t.Log("set leader 1 at leaderIdx = 0")
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	t.Log("all other nodes are crashed")
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	// here node 0 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check

	t.Log("leader1 gets a request")

	// update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	t.Log("leader1 crashed")
	test.Clients[0].Crash(test.Context, &emptypb.Empty{})

	t.Log("All other node are restored")
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

	t.Log("Leader 2 win election with leaderIdx = 1")
	leaderIdx = 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	t.Log("leader2 gets a request")

	// update a file on node 0
	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       2,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta2)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	t.Log("Leader 1 restored")
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})

	// one final call to sendheartbeat (from spec)
	t.Log("one final call to sendheartbeat")
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// check that all the nodes are in the right state
	// leader and all followers should have the entry in the log
	// everything should be term 1
	// entry should be applied to all metastores
	// only node 0 should be leader
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta.Filename] = filemeta2

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta,
	})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         2,
		FileMetaData: filemeta2,
	})

	term := int64(2)
	var leader bool
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}

}

// func TestRaftRecoverable(t *testing.T) {
// 	t.Log("leader1 gets a request")
// 	//Setup
// 	cfgPath := "./config_files/3nodes.txt"
// 	test := InitTest(cfgPath)
// 	defer EndTest(test)

// 	// set node 0 to be the leader
// 	// TEST
// 	leaderIdx := 0
// 	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
// 	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

// 	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
// 	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

// 	// here node 0 should be the leader, all other nodes should be followers
// 	// all logs and metastores should be empty
// 	// ^ TODO check

// 	// update a file on node 0
// 	filemeta := &surfstore.FileMetaData{
// 		Filename:      "testfile",
// 		Version:       1,
// 		BlockHashList: nil,
// 	}

// 	_, err := test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
// 	fmt.Println("uf", err)
// 	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

// 	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
// 	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

// 	// one final call to sendheartbeat (from spec)
// 	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

// 	// check that all the nodes are in the right state
// 	// leader and all followers should have the entry in the log
// 	// everything should be term 1
// 	// entry should be applied to all metastores
// 	// only node 0 should be leader
// 	goldenMeta := make(map[string]*surfstore.FileMetaData)
// 	goldenMeta[filemeta.Filename] = filemeta

// 	goldenLog := make([]*surfstore.UpdateOperation, 0)
// 	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
// 		Term:         1,
// 		FileMetaData: filemeta,
// 	})

// 	term := int64(1)
// 	var leader bool
// 	for idx, server := range test.Clients {
// 		if idx == leaderIdx {
// 			leader = bool(true)
// 		} else {
// 			leader = bool(false)
// 		}

// 		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)

// 		if err != nil {
// 			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
// 		}
// 	}
// }
