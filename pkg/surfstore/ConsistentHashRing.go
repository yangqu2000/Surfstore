package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap   map[string]string
	hashInOrder []string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {

	// find where the input block belongs to

	// 2. find the first server with larger hash value than blockHash
	for _, hash := range c.hashInOrder {
		if hash > blockId {
			return c.ServerMap[hash]
		}
	}

	return c.ServerMap[c.hashInOrder[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	consistentHashRing := &ConsistentHashRing{
		ServerMap:   make(map[string]string),
		hashInOrder: []string{},
	}

	for _, serverAddr := range serverAddrs {
		serverHash := consistentHashRing.Hash("blockstore" + serverAddr)
		consistentHashRing.ServerMap[serverHash] = serverAddr
	}

	for h := range consistentHashRing.ServerMap {
		consistentHashRing.hashInOrder = append(consistentHashRing.hashInOrder, h)
	}
	sort.Strings(consistentHashRing.hashInOrder)

	return consistentHashRing
}
