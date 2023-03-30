package surfstore

import (
	context "context"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {

	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil

	// panic("todo")
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {

	// file already exist
	for filename, cloudFileMetaData := range m.FileMetaMap {
		if filename == fileMetaData.Filename {
			// no race condition
			if cloudFileMetaData.Version+1 == fileMetaData.Version {
				m.FileMetaMap[filename] = fileMetaData
				return &Version{Version: fileMetaData.Version}, nil
			} else {
				return &Version{Version: int32(-1)}, nil
			}
		}
	}

	// file does not exist (to be create)
	m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	return &Version{Version: fileMetaData.Version}, nil

}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := make(map[string]*BlockHashes)

	for _, blockId := range blockHashesIn.Hashes {
		serverAddr := m.ConsistentHashRing.GetResponsibleServer(blockId)
		if blockStoreMap[serverAddr] == nil {
			blockStoreMap[serverAddr] = &BlockHashes{Hashes: make([]string, 0)}
		}
		blockStoreMap[serverAddr].Hashes = append(blockStoreMap[serverAddr].Hashes, blockId)
	}

	return &BlockStoreMap{BlockStoreMap: blockStoreMap}, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
