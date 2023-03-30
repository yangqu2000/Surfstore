package surfstore

import (
	context "context"
	"errors"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {

	_, found := bs.BlockMap[blockHash.Hash]
	if found {
		return bs.BlockMap[blockHash.Hash], nil
	}

	return nil, errors.New("Block not found")
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {

	hashes := []string{}
	for hash := range bs.BlockMap {
		hashes = append(hashes, hash)
	}

	return &BlockHashes{Hashes: hashes}, nil
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {

	bs.BlockMap[GetBlockHashString(block.BlockData)] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	hashesOut := []string{}

	for _, hash := range blockHashesIn.Hashes {

		_, found := bs.BlockMap[hash]
		if found {
			hashesOut = append(hashesOut, hash)
		}

	}

	return &BlockHashes{Hashes: hashesOut}, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
