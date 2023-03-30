package surfstore

import (
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

func convertRPCError(err string) string {
	return strings.Split(err, "=")[2][1:]
}

func isFileModified(refBlockHashes []string, varBlockHashes []string) bool {

	if len(refBlockHashes) != len(varBlockHashes) {
		return true
	} else {
		for idx, refHash := range refBlockHashes {
			if refHash != varBlockHashes[idx] {
				return true
			}
		}
	}

	return false
}

func isFileDeleted(fileInfo *FileMetaData) bool {
	if len(fileInfo.BlockHashList) == 1 && fileInfo.BlockHashList[0] == "0" {
		return true
	}
	return false
}

func (client *RPCClient) downloadFileFromCloud(filePath string, fileInfo *FileMetaData) {

	if isFileDeleted(fileInfo) {
		os.Remove(filePath)
		return
	}

	// download file from server
	f, err := os.Create(filePath)
	if err != nil {
		log.Fatal("Error when creating file: ", err)
	}

	defer f.Close()

	blockStoreMap := make(map[string][]string)
	err = client.GetBlockStoreMap(fileInfo.BlockHashList, &blockStoreMap)

	// convert server -> list of block hashs to hash -> server mapping
	blockServerMap := make(map[string]string)
	for server := range blockStoreMap {
		hashes := blockStoreMap[server]
		for _, hash := range hashes {
			blockServerMap[hash] = server
		}
	}

	block := &Block{}
	for _, blockHash := range fileInfo.BlockHashList {

		blockStoreAddr := ""
		for hash := range blockServerMap {
			if hash == blockHash {
				blockStoreAddr = blockServerMap[hash]
				break
			}
		}
		if blockStoreAddr == "" {
			log.Fatal("Error finding block in block servers: ", err)
		}

		err = client.GetBlock(blockHash, blockStoreAddr, block)
		if err != nil {
			log.Fatal("Error downloading file block from cloud: ", err)
		}

		_, err = f.Write(block.BlockData)
		if err != nil {
			log.Fatal("Error writing file block: ", err)
		}
	}
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {

	baseDir := client.BaseDir
	fmt.Println("Client base directory: ", baseDir)

	localFileInfoMap, err := LoadMetaFromMetaFile(baseDir)
	if err != nil {
		log.Fatal("Error loading Local Index from Meta File: ", err)
	}
	fmt.Println("begin local file info map")
	PrintMetaMap(localFileInfoMap)

	baseFileHashesMap := make(map[string][]string)
	modifiedFileInfoMap := make(map[string]*FileMetaData)

	// scan the base dir
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		log.Fatal("Error when reading file from base directory:", err)
	}

	hasIndexDB := false
	for _, entry := range entries {
		if !entry.IsDir() {

			fileName := entry.Name()

			if fileName == "index.db" {
				hasIndexDB = true
				continue
			}

			filePath := ConcatPath(baseDir, fileName)

			fileStats, err := os.Stat(filePath)
			if err != nil {
				log.Fatal("Error when reading file's statistics: ", err)
			}

			blockHashes := []string{}
			if fileStats.Size() == 0 { // empty file
				blockHashes = append(blockHashes, "-1")

			} else {
				f, err := os.Open(filePath)
				if err != nil {
					log.Fatalf("unable to read file: %v", err)
				}

				defer f.Close()

				buf := make([]byte, client.BlockSize)
				for {
					size, err := f.Read(buf)
					if err == io.EOF {
						break
					}

					if err != nil {
						log.Fatal("Error reading the file content: ", err)
						continue
					}

					block := buf[:size]

					hashValue := GetBlockHashString(block)

					blockHashes = append(blockHashes, hashValue)
				}
			}

			baseFileHashesMap[fileName] = blockHashes

			_, found := localFileInfoMap[fileName]
			if !found { // file in base directory, not in local index
				modifiedFileInfoMap[fileName] = &FileMetaData{
					Filename:      fileName,
					Version:       int32(1),
					BlockHashList: blockHashes,
				}
			} else {
				// file has uncommited change in base directory
				if isFileModified(localFileInfoMap[fileName].BlockHashList, blockHashes) {
					modifiedFileInfoMap[fileName] = &FileMetaData{
						Filename:      fileName,
						Version:       localFileInfoMap[fileName].Version + 1,
						BlockHashList: blockHashes,
					}
				}
			}

		}
	}

	for fileName, localFileInfo := range localFileInfoMap {
		_, found := baseFileHashesMap[fileName]

		// file deleted in the base directory
		if !found {
			modifiedFileInfoMap[fileName] = &FileMetaData{
				Filename:      fileName,
				Version:       localFileInfo.Version + 1,
				BlockHashList: []string{"0"},
			}
		}
	}

	fmt.Println("modified file info map BEFORE checking base dir with local changes")
	PrintMetaMap(modifiedFileInfoMap)

	if !hasIndexDB {
		indexDB, err := os.Create(ConcatPath(baseDir, "index.db"))
		if err != nil {
			log.Fatal("Error creating the index.db file: ", err)
		}
		defer indexDB.Close()
	}

	// connect to the server and download an updated FileInfoMap
	remoteFileInfoMap := make(map[string]*FileMetaData)

	err = client.GetFileInfoMap(&remoteFileInfoMap)
	if err != nil {
		fmt.Println(err)
		log.Fatal("Error downloading remote fileInfoMap from Meta store server: ", err)
	}
	fmt.Println("remote file info map")
	PrintMetaMap(remoteFileInfoMap)

	/**
	compare the local index with the remote index
	1) remote index does not match => download file from block -> reconstitute that file into base dir -> add the updated FileInfo to local index
	2) local index does not
	**/

	// get the block store address
	var blockStoreAddrs []string
	err = client.GetBlockStoreAddrs(&blockStoreAddrs)
	if err != nil {
		log.Fatal("Error getting block store address: ", err)
	}

	for fileName, remoteFileInfo := range remoteFileInfoMap {

		filePath := ConcatPath(baseDir, fileName)

		localFileInfo, found := localFileInfoMap[fileName]

		// file in remote server but not presents in local index
		if !found {

			_, found := modifiedFileInfoMap[fileName]
			if found {
				delete(modifiedFileInfoMap, fileName)
			}

			client.downloadFileFromCloud(filePath, remoteFileInfo)
			localFileInfoMap[fileName] = remoteFileInfo

		} else { // file prensents in both server and local index

			if isFileModified(remoteFileInfo.BlockHashList, localFileInfo.BlockHashList) {
				// whoever syncsd to the cloud first wins, discard local uncommitted change
				modifiedFileInfoMap[fileName] = localFileInfo

			} else {

				if isFileDeleted(remoteFileInfo) && isFileDeleted(modifiedFileInfoMap[fileName]) {
					delete(modifiedFileInfoMap, fileName)
				}

			}

		}

	}

	fmt.Println("modified file info map AFTER checking remote")
	PrintMetaMap(modifiedFileInfoMap)

	for fileName, modifiedFileInfo := range modifiedFileInfoMap {

		success := false
		filePath := ConcatPath(baseDir, fileName)

		if isFileDeleted(modifiedFileInfo) {
			success = true

		} else {

			f, err := os.Open(filePath)

			if err != nil {
				log.Fatal("Error reading the file: ", err)
			}

			defer f.Close()

			buf := make([]byte, client.BlockSize)
			for {
				size, err := f.Read(buf)
				if err == io.EOF {
					break
				}

				if err != nil {
					log.Fatal("Error reading the file content: ", err)
					continue
				}

				block := &Block{
					BlockData: buf[:size],
					BlockSize: int32(size),
				}

				blockStoreMap := make(map[string][]string)
				client.GetBlockStoreMap([]string{GetBlockHashString(block.BlockData)}, &blockStoreMap)

				blockStoreAddr := ""
				for key := range blockStoreMap {
					blockStoreAddr = key
				}

				client.PutBlock(block, blockStoreAddr, &success)
				if !success {
					break
				}
			}

		}

		if success {
			var latestVersion int32
			err := client.UpdateFile(modifiedFileInfo, &latestVersion)
			if err != nil {
				log.Fatal("Error update the file: ", err)
			}

			// check if race condition happends
			if latestVersion == -1 {
				client.downloadFileFromCloud(filePath, remoteFileInfoMap[fileName])
				localFileInfoMap[fileName] = remoteFileInfoMap[fileName]
			} else {

				err = client.GetFileInfoMap(&remoteFileInfoMap)
				if err != nil {
					log.Fatal("Error getting the fileInfoMap from the meta stroe: ", err)
				}

				localFileInfoMap[fileName] = modifiedFileInfo
			}

		}

	}

	err = WriteMetaFile(localFileInfoMap, baseDir)
	if err != nil {
		log.Fatal("Error writing local index file: ", err)
	}

	fmt.Println("final local file info map")
	PrintMetaMap(localFileInfoMap)

	fmt.Println("final remote file info map")
	PrintMetaMap(remoteFileInfoMap)
}
