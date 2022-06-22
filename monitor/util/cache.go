package util

import (
	"github.com/ethereum/go-ethereum/common"
	"sync"
)

type BlockHashMap map[uint64]common.Hash

type BlockNumberAndHash struct {
	Number uint64
	Hash   common.Hash
}

var (
	originalBlockHashCache = sync.Map{}
	taintedBlockHashCache = make(BlockHashMap)
	originalBlockHashCacheInitialized = false
	taintedBlockHashCacheInitialized = false
	originalBlockHashCacheDumper func(blockNumber uint64, hash common.Hash) = nil
	taintedBlockHashCacheDumper func(blockNumber uint64, hash common.Hash) = nil
)

// InitOriginalBlockHashCache WARNING: thread unsafe
func InitOriginalBlockHashCache(blockNumberAndHashes []BlockNumberAndHash, originalBlockHashCacheDumperFunc func(blockNumber uint64, hash common.Hash)) {
	if originalBlockHashCacheInitialized {
		panic("originalBlockHashCache already initialized")
	}
	originalBlockHashCacheDumper = originalBlockHashCacheDumperFunc
	for _, blockNumberAndHash := range blockNumberAndHashes {
		originalBlockHashCache.Store(blockNumberAndHash.Number, blockNumberAndHash.Hash)
	}
	originalBlockHashCacheInitialized = true
}

func LoadOriginalBlockHash(blockNumber uint64) (hash common.Hash, exist bool) {
	value, ok := originalBlockHashCache.Load(blockNumber)
	return value.(common.Hash), ok
}

func SaveOriginalBlockHash(blockNumber uint64, hash common.Hash) {
	if _, exist := originalBlockHashCache.Load(blockNumber); !exist {
		originalBlockHashCache.Store(blockNumber, hash)
		if originalBlockHashCacheDumper != nil {
			originalBlockHashCacheDumper(blockNumber, hash)
		}
	}
}

// InitTaintedBlockHashCache WARNING: thread unsafe
func InitTaintedBlockHashCache(blockNumberAndHashes []BlockNumberAndHash, taintedBlockHashCacheDumperFunc func(blockNumber uint64, hash common.Hash)) {
	if taintedBlockHashCacheInitialized {
		panic("taintedBlockHashCache already initialized")
	}
	taintedBlockHashCacheDumper = taintedBlockHashCacheDumperFunc
	for _, blockNumberAndHash := range blockNumberAndHashes {
		taintedBlockHashCache[blockNumberAndHash.Number] = blockNumberAndHash.Hash
	}
	taintedBlockHashCacheInitialized = true
}

// LoadTaintedBlockHash WARNING: thread unsafe
func LoadTaintedBlockHash(blockNumber uint64) (hash common.Hash, exist bool) {
	value, ok := taintedBlockHashCache[blockNumber]
	return value, ok
}

// SaveTaintedBlockHash WARNING: thread unsafe
func SaveTaintedBlockHash(blockNumber uint64, hash common.Hash) {
	if _, exist := taintedBlockHashCache[blockNumber]; !exist {
		taintedBlockHashCache[blockNumber] = hash
		if taintedBlockHashCacheDumper != nil {
			taintedBlockHashCacheDumper(blockNumber, hash)
		}
	}
}
