package surfstore

import (
	context "context"
	"fmt"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {

	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashes = b.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	*succ = success.Flag

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	blockHashes, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashesOut = blockHashes.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {

	// connect to the server
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())

		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		fileInfoMap, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})

		if err != nil {
			conn.Close()
			if convertRPCError(err.Error()) == ERR_NOT_LEADER.Error() || convertRPCError(err.Error()) == ERR_SERVER_CRASHED.Error() {
				continue
			} else {
				return err
			}
		}

		// is leader and not crashed
		*serverFileInfoMap = fileInfoMap.FileInfoMap

		// close the connection
		return conn.Close()
	}

	return fmt.Errorf("ERR_NO_NODE_AVALIABLE")
}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {

	// connect to the server
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())

		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		v, err := c.UpdateFile(ctx, fileMetaData)

		if err != nil {
			conn.Close()
			if convertRPCError(err.Error()) == ERR_NOT_LEADER.Error() || convertRPCError(err.Error()) == ERR_SERVER_CRASHED.Error() {
				continue
			} else {
				return err
			}
		}

		*latestVersion = v.Version
		// close the connection
		return conn.Close()
	}

	return fmt.Errorf("ERR_NO_NODE_AVALIABLE")
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {

	// connect to the server
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())

		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		bsMap, err := c.GetBlockStoreMap(ctx, &BlockHashes{Hashes: blockHashesIn})
		if err != nil {
			conn.Close()
			if convertRPCError(err.Error()) == ERR_NOT_LEADER.Error() || convertRPCError(err.Error()) == ERR_SERVER_CRASHED.Error() {
				continue
			} else {
				return err
			}
		}

		newMap := make(map[string][]string)
		for hash := range bsMap.BlockStoreMap {
			newMap[hash] = bsMap.BlockStoreMap[hash].Hashes
		}
		*blockStoreMap = newMap

		// close the connection
		return conn.Close()
	}

	return fmt.Errorf("ERR_NO_NODE_AVALIABLE")
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {

	// connect to the server
	for _, metaStoreAddr := range surfClient.MetaStoreAddrs {

		conn, err := grpc.Dial(metaStoreAddr, grpc.WithInsecure())

		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		addrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})
		if err != nil {
			conn.Close()
			if convertRPCError(err.Error()) == ERR_NOT_LEADER.Error() || convertRPCError(err.Error()) == ERR_SERVER_CRASHED.Error() {
				continue
			} else {
				return err
			}
		}

		*blockStoreAddrs = addrs.GetBlockStoreAddrs()

		// close the connection
		return conn.Close()
	}

	return fmt.Errorf("ERR_NO_NODE_AVALIABLE")
}

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
