package surfstore

import (
	context "context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader      bool
	isLeaderMutex *sync.RWMutex

	// Persistent state on all servers
	term int64
	log  []*UpdateOperation

	// state machine
	metaStore *MetaStore

	// Added for discussion
	id             int64
	peers          []string
	pendingCommits []*chan bool

	// Volatile state on all servers
	commitIndex int64
	lastApplied int64

	// Volatile state on leaders
	// nextIndex  []int64
	// matchIndex []int64

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) isMajorityCrashed(ctx context.Context, empty *emptypb.Empty) (bool, error) {

	totalCrashed := 0
	if s.isCrashed {
		totalCrashed++
	}

	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		// TODO check all errors
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return false, err
		}

		client := NewRaftSurfstoreClient(conn)

		_, err = client.AppendEntries(ctx, &AppendEntryInput{
			Term: s.term,
			// TODO put the right values
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		})

		if err != nil {
			if convertRPCError(err.Error()) == ERR_SERVER_CRASHED.Error() {
				totalCrashed++
			} else {
				return false, err
			}
		}

	}

	if totalCrashed > len(s.peers)/2 {
		return true, nil
	}

	return false, nil
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {

	if !s.isLeader {
		if s.isCrashed {
			return nil, ERR_SERVER_CRASHED
		}
		return nil, ERR_NOT_LEADER
	}

	isBlock, err := s.isMajorityCrashed(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	for isBlock {
		isBlock, err = s.isMajorityCrashed(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
	}

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	return s.metaStore.GetFileInfoMap(ctx, &emptypb.Empty{})
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {

	if !s.isLeader {
		if s.isCrashed {
			return nil, ERR_SERVER_CRASHED
		}
		return nil, ERR_NOT_LEADER
	}

	isBlock, err := s.isMajorityCrashed(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	for isBlock {
		isBlock, err = s.isMajorityCrashed(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
	}

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {

	if !s.isLeader {
		if s.isCrashed {
			return nil, ERR_SERVER_CRASHED
		}
		return nil, ERR_NOT_LEADER
	}

	isBlock, err := s.isMajorityCrashed(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	for isBlock {
		isBlock, err = s.isMajorityCrashed(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}
	}

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	return s.metaStore.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {

	if !s.isLeader {
		if s.isCrashed {
			return nil, ERR_SERVER_CRASHED
		}
		return nil, ERR_NOT_LEADER
	}

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// append entry to our log
	s.log = append(s.log, &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	})

	commitChan := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &commitChan)

	// send entry to all followers in parallel
	go s.sendToAllFollowersInParallel(ctx)

	isBlock, err := s.isMajorityCrashed(ctx, &emptypb.Empty{})
	if err != nil {
		return nil, err
	}
	for isBlock {
		isBlock, err = s.isMajorityCrashed(ctx, &emptypb.Empty{})
		if err != nil {
			return nil, err
		}

		// send entry to all followers in parallel
		go s.sendToAllFollowersInParallel(ctx)
	}

	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		// TODO check all errors
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}

		client := NewRaftSurfstoreClient(conn)

		output, err := client.AppendEntries(ctx, &AppendEntryInput{
			Term: s.term,
			// TODO put the right values
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		})

		if err != nil {
			if convertRPCError(err.Error()) == ERR_SERVER_CRASHED.Error() {
				continue
			} else {
				return nil, err
			}
		}

		if output.Term > s.term {
			s.isLeaderMutex.Lock()
			defer s.isLeaderMutex.Unlock()
			s.isLeader = false
			return nil, ERR_NOT_LEADER
		}

	}

	// commit the entry once majority of followers have it in their log
	commit := <-commitChan

	// once committed, apply to the state machine
	if commit {
		return s.metaStore.UpdateFile(ctx, filemeta)
	}

	return nil, nil
}

func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	// send entry to all my followers and count the replies

	responses := make(chan bool, len(s.peers)-1)
	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		go s.sendToFollower(ctx, addr, responses)
	}

	totalResponses := 1
	totalAppends := 1

	// wait in loop for responses
	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if totalResponses == len(s.peers) {
			break
		}
	}

	if totalAppends > len(s.peers)/2 {
		// TODO put on correct channel
		*s.pendingCommits[0] <- true
		s.pendingCommits = s.pendingCommits[1:]

		// TODO update commit Index correctly
		s.commitIndex++
	}

}

func (s *RaftSurfstore) sendToFollower(ctx context.Context, addr string, responses chan bool) {

	dummyAppendEntriesInput := AppendEntryInput{
		Term: s.term,
		// TODO put the right values
		PrevLogTerm:  -1,
		PrevLogIndex: -1,
		Entries:      s.log,
		LeaderCommit: s.commitIndex,
	}

	// TODO check all errors
	conn, err := grpc.Dial(addr, grpc.WithInsecure())

	if err != nil {
		responses <- false
	} else {
		client := NewRaftSurfstoreClient(conn)

		output, err := client.AppendEntries(ctx, &dummyAppendEntriesInput)
		if err != nil {
			responses <- false
		} else {
			responses <- output.Success
		}

	}

}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	if s.term < input.Term {
		s.isLeaderMutex.Lock()
		defer s.isLeaderMutex.Unlock()
		s.isLeader = false
		s.term = input.Term
	}

	// TODO actually check entries
	if !s.isLeader {
		s.log = input.Entries
		for s.lastApplied < input.LeaderCommit {
			entry := s.log[s.lastApplied+1]
			_, err := s.metaStore.UpdateFile(ctx, entry.FileMetaData)
			if err != nil {
				return nil, err
			}
			s.lastApplied++
		}

		s.commitIndex = input.LeaderCommit

		appendEntryOutput := &AppendEntryOutput{
			ServerId:     s.id,
			Term:         s.term,
			Success:      true,
			MatchedIndex: -1,
		}

		return appendEntryOutput, nil

	}

	return &AppendEntryOutput{
		ServerId:     s.id,
		Term:         s.term,
		Success:      true,
		MatchedIndex: -1,
	}, nil

}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	s.isLeaderMutex.Lock()
	defer s.isLeaderMutex.Unlock()
	s.isLeader = true
	s.term++

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {

	fmt.Println("send heatbeat...")

	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	}

	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}

	// contact all the follower, send some AppendEntries call
	for idx, addr := range s.peers {
		if int64(idx) == s.id {
			continue
		}

		// TODO check all errors
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}

		client := NewRaftSurfstoreClient(conn)

		output, err := client.AppendEntries(ctx, &AppendEntryInput{
			Term: s.term,
			// TODO put the right values
			PrevLogTerm:  -1,
			PrevLogIndex: -1,
			Entries:      s.log,
			LeaderCommit: s.commitIndex,
		})

		fmt.Println(output, err)

	}

	return &Success{Flag: true}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log,
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()

	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
