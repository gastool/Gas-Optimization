// Copyright 2014 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

// Package utils contains internal helper functions for go-ethereum commands.
package utils

import (
	"compress/gzip"
	"fmt"
	"github.com/ethereum/go-ethereum/monitor/typing"
	"github.com/ethereum/go-ethereum/monitor/util"
	"io"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/internal/debug"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/rlp"
)

const (
	importBatchSize = 2500
)

// Fatalf formats a message to standard error and exits the program.
// The message is also printed to standard output if standard error
// is redirected to a different file.
func Fatalf(format string, args ...interface{}) {
	w := io.MultiWriter(os.Stdout, os.Stderr)
	if runtime.GOOS == "windows" {
		// The SameFile check below doesn't work on Windows.
		// stdout is unlikely to get redirected though, so just print there.
		w = os.Stdout
	} else {
		outf, _ := os.Stdout.Stat()
		errf, _ := os.Stderr.Stat()
		if outf != nil && errf != nil && os.SameFile(outf, errf) {
			w = os.Stderr
		}
	}
	fmt.Fprintf(w, "Fatal: "+format+"\n", args...)
	os.Exit(1)
}

func StartNode(stack *node.Node) {
	if err := stack.Start(); err != nil {
		Fatalf("Error starting protocol stack: %v", err)
	}
	go func() {
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigc)
		<-sigc
		log.Info("Got interrupt, shutting down...")
		go stack.Close()
		for i := 10; i > 0; i-- {
			<-sigc
			if i > 1 {
				log.Warn("Already shutting down, interrupt more to panic.", "times", i-1)
			}
		}
		debug.Exit() // ensure trace and CPU profile data is flushed.
		debug.LoudPanic("boom")
	}()
}

// dynamically update TransactionsOriginalGasUsed, CallChainOriginalEvmErrors, CallChainOriginalGasSiteInfo
// should be called in a separated goroutine
func dynamicallyUpdateDependencyInfo(monitorConfig *typing.MonitorConfig) {
	log.Warn("[dynamic update pre] Begin reading and truncating file with Flock")
	start := time.Now()
	var (
		originalGasUsedUpdateCount int
		originalEvmErrorUpdateCount int
		originalGasSiteInfoUpdateCount int
		evmErrorLatestBlockNumber uint64
		gasSiteInfoLatestBlockNumber uint64
		waitGroup sync.WaitGroup
	)
	waitGroup.Add(3)
	go func() {
		defer waitGroup.Done()
		originalGasUsedUpdateCount = util.LoadTransactionsOriginalGasUsedAndTruncateFile(
			monitorConfig.CleanGasBriefSnippetFilename, monitorConfig.TransactionsOriginalGasUsed,
			monitorConfig.TransactionsOriginalGasUsedLock, monitorConfig.TransactionsOriginalGasUsedSnippetFileLock,
			monitorConfig.CleanGasBriefSnippetFileTruncateSize)
	}()
	go func() {
		defer waitGroup.Done()
		originalEvmErrorUpdateCount, evmErrorLatestBlockNumber = util.LoadCallChainOriginalEvmErrorsAndTruncateFile(
			monitorConfig.CallChainEvmErrorSnippetFilename, monitorConfig.CallChainOriginalErrorCallInfo,
			monitorConfig.CallChainOriginalErrorCallInfoLock, monitorConfig.CallChainOriginalErrorCallInfoSnippetFileLock,
			monitorConfig.CallChainEvmErrorSnippetFileTruncateSize)
	}()
	go func() {
		defer waitGroup.Done()
		originalGasSiteInfoUpdateCount, gasSiteInfoLatestBlockNumber = util.LoadCallChainOriginalGasSiteInfoAndTruncateFile(
			monitorConfig.CallChainGasSiteInfoSnippetFilename, monitorConfig.CallChainOriginalGasSiteInfo,
			monitorConfig.CallChainOriginalGasSiteInfoLock, monitorConfig.CallChainOriginalGasSiteInfoSnippetFileLock,
			monitorConfig.CallChainGasSiteInfoSnippetFileTruncateSize)
	}()
	waitGroup.Wait()

	var minBlockNumber uint64
	if evmErrorLatestBlockNumber < gasSiteInfoLatestBlockNumber {
		minBlockNumber = evmErrorLatestBlockNumber
	} else {
		minBlockNumber = gasSiteInfoLatestBlockNumber
	}
	if monitorConfig.DependencyInfoLatestBlockNumber < minBlockNumber {
		monitorConfig.DependencyInfoLatestBlockNumber = minBlockNumber
	}

	elapsed := time.Since(start)

	if originalGasUsedUpdateCount != 0 || originalEvmErrorUpdateCount != 0 || originalGasSiteInfoUpdateCount != 0 {
		log.Warn("[dynamic update] Updated " + strconv.Itoa(originalGasUsedUpdateCount) + " originalGasUsed entries, " +
			strconv.Itoa(originalEvmErrorUpdateCount) + " originalEvmError entries, " +
			strconv.Itoa(originalGasSiteInfoUpdateCount) + " originalGasSiteInfo entries in " +
			strconv.FormatFloat(elapsed.Seconds(), 'f', 4, 64) + "s")
	} else {
		log.Warn("[dynamic update] Updated 0 entry")
	}
}

func ImportChain(chain *core.BlockChain, fn string) error {
	// Watch for Ctrl-C while the import is running.
	// If a signal is received, the import will stop at the next batch.
	interrupt := make(chan os.Signal, 1)
	stop := make(chan struct{})
	signal.Notify(interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(interrupt)
	defer close(interrupt)
	go func() {
		if _, ok := <-interrupt; ok {
			log.Info("Interrupted during import, stopping at next batch")
		}
		close(stop)
	}()
	checkInterrupt := func() bool {
		select {
		case <-stop:
			return true
		default:
			return false
		}
	}

	log.Info("Importing blockchain", "file", fn)

	// Open the file handle and potentially unwrap the gzip stream
	fh, err := os.Open(fn)
	if err != nil {
		return err
	}
	defer fh.Close()

	var reader io.Reader = fh
	if strings.HasSuffix(fn, ".gz") {
		if reader, err = gzip.NewReader(reader); err != nil {
			return err
		}
	}
	stream := rlp.NewStream(reader, 0)

	// Run actual the import.
	blocks := make(types.Blocks, importBatchSize)
	n := 0
	for batch := 0; ; batch++ {
		// Load a batch of RLP blocks.
		if checkInterrupt() {
			// flush csv file buffers
			log.Warn("Flushing all csv files ...")
			util.FlushAllCsvFileBuffersAndSnippetBuffers(chain.GetVMConfig().MonitorConfig)
			log.Warn("Flush complete")

			return fmt.Errorf("interrupted")
		}
		i := 0
		for ; i < importBatchSize; i++ {
			var b types.Block
			if err := stream.Decode(&b); err == io.EOF {
				break
			} else if err != nil {
				return fmt.Errorf("at block %d: %v", n, err)
			}
			// don't import first block
			if b.NumberU64() == 0 {
				i--
				continue
			}
			blocks[i] = &b
			n++
		}
		if i == 0 {
			break
		}
		// Import the batch.
		if checkInterrupt() {
			// flush csv file buffers
			log.Warn("Flushing all csv files ...")
			util.FlushAllCsvFileBuffersAndSnippetBuffers(chain.GetVMConfig().MonitorConfig)
			log.Warn("Flush complete")

			return fmt.Errorf("interrupted")
		}
		missing := missingBlocks(chain, blocks[:i])
		if len(missing) == 0 {
			log.Info("Skipping batch as all blocks present", "batch", batch, "first", blocks[0].Hash(), "last", blocks[i-1].Hash())
			continue
		}

		monitorConfig := chain.GetVMConfig().MonitorConfig

		// dynamically update TransactionsOriginalGasUsed, CallChainOriginalEvmErrors, CallChainOriginalGasSiteInfo (with lock)
		if monitorConfig.ReplaceMonitoredContractCode && monitorConfig.DynamicallyUpdateDependencyInfo &&
			(batch % monitorConfig.DynamicallyUpdateDependencyInfoPerBatches == 0) {
			go dynamicallyUpdateDependencyInfo(monitorConfig)
			// block if no sufficient dependency info is present and wait for another process
			if len(missing) > 0 {
				for monitorConfig.DependencyInfoLatestBlockNumber < missing[0].NumberU64() {
					log.Warn("Insufficient dependency info, sleeping ...")
					time.Sleep(time.Second * 20)

					// check if interrupted
					if checkInterrupt() {
						// flush csv file buffers
						log.Warn("Flushing all csv files ...")
						util.FlushAllCsvFileBuffersAndSnippetBuffers(chain.GetVMConfig().MonitorConfig)
						log.Warn("Flush complete")

						return fmt.Errorf("interrupted")
					}

					dynamicallyUpdateDependencyInfo(monitorConfig) // sync, block
				}
			}
		}

		if _, err := chain.InsertChain(missing); err != nil {
			// flush csv file buffers
			log.Warn("Flushing all csv files ...")
			util.FlushAllCsvFileBuffersAndSnippetBuffers(chain.GetVMConfig().MonitorConfig)
			log.Warn("Flush complete")

			errString := fmt.Sprintf("invalid block %d: %v", n, err)
			panic("Invalid block guard triggered; state update prevented. Error: " + errString)
			return fmt.Errorf("invalid block %d: %v", n, err)
		}

		// force flush important files
		if !monitorConfig.ReplaceMonitoredContractCode && (batch % monitorConfig.ForceFlushKeyCsvFileBufferPerBatches == 0) {
			log.Warn("[auto-flush-more pre] Begin flushing with Flock")
			start := time.Now()
			cleanGasBriefSnippetSize := util.FlushSnippetCsvFileBuffer(
				monitorConfig.CleanGasBriefSnippetFilename, monitorConfig.TransactionsOriginalGasUsedSnippetFileLock)
			callChainEvmErrorSnippetSize := util.FlushSnippetCsvFileBuffer(
				monitorConfig.CallChainEvmErrorSnippetFilename, monitorConfig.CallChainOriginalErrorCallInfoSnippetFileLock)
			callChainGasSiteInfoSnippetSize := util.FlushSnippetCsvFileBuffer(
				monitorConfig.CallChainGasSiteInfoSnippetFilename, monitorConfig.CallChainOriginalGasSiteInfoSnippetFileLock)
			elapsed := time.Since(start)
			if cleanGasBriefSnippetSize > 0 || callChainEvmErrorSnippetSize > 0 || callChainGasSiteInfoSnippetSize > 0 {
				log.Warn("[auto-flush-more] Flushed " +
					strconv.Itoa(cleanGasBriefSnippetSize) + " CleanGasBriefSnippet entries, " +
					strconv.Itoa(callChainEvmErrorSnippetSize) + " CallChainEvmErrorSnippet entries, " +
					strconv.Itoa(callChainGasSiteInfoSnippetSize) + " CallChainGasSiteInfoSnippet entries, in " +
					strconv.FormatFloat(elapsed.Seconds(), 'f', 4, 64) + "s")
			}
		}

		// force flush less important files
		if batch % monitorConfig.ForceFlushLessKeyCsvFileBufferPerBatches == 0 {
			//log.Warn("[auto-flush-less] Begin flushing")
			start := time.Now()
			if !monitorConfig.ReplaceMonitoredContractCode {
				cleanResultFileSize := util.FlushCsvFileBuffer(monitorConfig.MonitorCleanResultFile)
				cleanGasBriefFileSize := util.FlushCsvFileBuffer(monitorConfig.MonitorCleanGasBriefFile)
				callChainEvmErrorFileSize := util.FlushCsvFileBuffer(monitorConfig.MonitorCallChainEvmErrorFile)
				callChainGasSiteInfoFileSize := util.FlushCsvFileBuffer(monitorConfig.MonitorCallChainGasSiteInfoFile)
				opCodeStatisticsFileSize := util.FlushCsvFileBuffer(monitorConfig.MonitorOpCodeStatisticsFile)

				if cleanResultFileSize > 0 || cleanGasBriefFileSize > 0 || callChainEvmErrorFileSize > 0 || callChainGasSiteInfoFileSize > 0 || opCodeStatisticsFileSize > 0 {
					var builder strings.Builder
					builder.WriteString("[auto-flush-less] Flushed ")
					builder.WriteString(strconv.Itoa(cleanResultFileSize))
					builder.WriteString(" CleanResultFile entries, ")
					builder.WriteString(strconv.Itoa(cleanGasBriefFileSize))
					builder.WriteString(" CleanGasBriefFile entries, ")
					builder.WriteString(strconv.Itoa(callChainEvmErrorFileSize))
					builder.WriteString(" CallChainEvmErrorFile entries, ")
					builder.WriteString(strconv.Itoa(callChainGasSiteInfoFileSize))
					builder.WriteString(" CallChainGasSiteInfoFile entries, ")
					builder.WriteString(strconv.Itoa(opCodeStatisticsFileSize))
					builder.WriteString(" OpCodeStatisticsFile entries, ")
					elapsed := time.Since(start)
					builder.WriteString("in ")
					builder.WriteString(strconv.FormatFloat(elapsed.Seconds(), 'f', 4, 64) + "s")
					log.Warn(builder.String())
				}
			} else {
				dirtyResultFileSize := util.FlushCsvFileBuffer(monitorConfig.MonitorDirtyResultFile)
				dirtyGasBriefFileSize := util.FlushCsvFileBuffer(monitorConfig.MonitorDirtyGasBriefFile)
				errorLogFileSize := util.FlushCsvFileBuffer(monitorConfig.MonitorErrorLogFile)
				opCodeStatisticsFileSize := util.FlushCsvFileBuffer(monitorConfig.MonitorOpCodeStatisticsFile)

				if dirtyResultFileSize > 0 || dirtyGasBriefFileSize > 0 || errorLogFileSize > 0 || opCodeStatisticsFileSize > 0 {
					var builder strings.Builder
					builder.WriteString("[auto-flush-less] Flushed ")
					builder.WriteString(strconv.Itoa(dirtyResultFileSize))
					builder.WriteString(" DirtyResultFile entries, ")
					builder.WriteString(strconv.Itoa(dirtyGasBriefFileSize))
					builder.WriteString(" DirtyGasBriefFile entries, ")
					builder.WriteString(strconv.Itoa(errorLogFileSize))
					builder.WriteString(" ErrorLogFile entries, ")
					builder.WriteString(strconv.Itoa(opCodeStatisticsFileSize))
					builder.WriteString(" OpCodeStatisticsFile entries, ")
					elapsed := time.Since(start)
					builder.WriteString("in ")
					builder.WriteString(strconv.FormatFloat(elapsed.Seconds(), 'f', 4, 64) + "s")
					log.Warn(builder.String())
				}
			}
		}

		// gc TransactionsOriginalGasUsed, CallChainOriginalErrorCallInfo, CallChainOriginalGasSiteInfo
		if monitorConfig.ReplaceMonitoredContractCode && monitorConfig.DependencyInfoGC && (batch % monitorConfig.DependencyInfoGCPerBatches == 0) {
			//go func() {
				var waitGroup sync.WaitGroup
				waitGroup.Add(3)
				start := time.Now()
				transactionsOriginalGasUsedGcCount := 0
				callChainOriginalErrorCallInfoGcCount := 0
				callChainOriginalGasSiteInfoGcCount := 0
				transactionsOriginalGasUsedIsFullGc := false
				callChainOriginalErrorCallInfoIsFullGc := false
				callChainOriginalGasSiteInfoIsFullGc := false

				shouldRenewMap := func(currentSize int, junkSize int, peakSize int) bool {
					return junkSize > 0 && junkSize >= (peakSize / 2)
				}

				// gc TransactionsOriginalGasUsed
				go func() {
					defer waitGroup.Done()
					monitorConfig.TransactionsOriginalGasUsedLock.Lock()
					defer monitorConfig.TransactionsOriginalGasUsedLock.Unlock()

					// record current size, junk size, peak size
					currentSize := len(monitorConfig.TransactionsOriginalGasUsed)
					if currentSize > monitorConfig.TransactionsOriginalGasUsedPeakSize {
						monitorConfig.TransactionsOriginalGasUsedPeakSize = currentSize
					}
					monitorConfig.TransactionsOriginalGasUsedJunkSize += len(monitorConfig.TransactionsOriginalGasUsedGcList)
					// remove dead entries
					for _, txHashString := range monitorConfig.TransactionsOriginalGasUsedGcList {
						delete(monitorConfig.TransactionsOriginalGasUsed, txHashString)
						transactionsOriginalGasUsedGcCount += 1
					}
					// create a new map if necessary, to avoid Go's internal memory leak
					if shouldRenewMap(currentSize, monitorConfig.TransactionsOriginalGasUsedJunkSize, monitorConfig.TransactionsOriginalGasUsedPeakSize) {
						newMap := make(map[string]uint64)
						for key, value := range monitorConfig.TransactionsOriginalGasUsed {
							newMap[key] = value
						}
						monitorConfig.TransactionsOriginalGasUsed = newMap
						monitorConfig.TransactionsOriginalGasUsedJunkSize = 0
						monitorConfig.TransactionsOriginalGasUsedPeakSize = 0
						transactionsOriginalGasUsedIsFullGc = true
					}
					// clear gc list
					monitorConfig.TransactionsOriginalGasUsedGcList = []string{}
				}()

				// gc CallChainOriginalErrorCallInfo
				go func() {
					defer waitGroup.Done()
					monitorConfig.CallChainOriginalErrorCallInfoLock.Lock()
					defer monitorConfig.CallChainOriginalErrorCallInfoLock.Unlock()

					// record current size, junk size, peak size
					currentSize := len(monitorConfig.CallChainOriginalErrorCallInfo)
					if currentSize > monitorConfig.CallChainOriginalErrorCallInfoPeakSize {
						monitorConfig.CallChainOriginalErrorCallInfoPeakSize = currentSize
					}
					// remove dead entries
					for _, callLocator := range monitorConfig.CallChainOriginalErrorCallInfoGcList {
						callLocatorValue := *callLocator
						// CallChainOriginalErrorCallInfoGcList may contain duplicated elements
						// must check in advance to avoid deleting twice
						if _, exist := monitorConfig.CallChainOriginalErrorCallInfo[callLocatorValue]; exist {
							delete(monitorConfig.CallChainOriginalErrorCallInfo, callLocatorValue)
							callChainOriginalErrorCallInfoGcCount += 1
						}
					}
					// create a new map if necessary, to avoid Go's internal memory leak
					monitorConfig.CallChainOriginalErrorCallInfoJunkSize += callChainOriginalErrorCallInfoGcCount
					if shouldRenewMap(currentSize, monitorConfig.CallChainOriginalErrorCallInfoJunkSize, monitorConfig.CallChainOriginalErrorCallInfoPeakSize) {
						newMap := make(map[typing.CallLocator]*typing.CallInfo)
						for key, value := range monitorConfig.CallChainOriginalErrorCallInfo {
							newMap[key] = value
						}
						monitorConfig.CallChainOriginalErrorCallInfo = newMap
						monitorConfig.CallChainOriginalErrorCallInfoJunkSize = 0
						monitorConfig.CallChainOriginalErrorCallInfoPeakSize = 0
						callChainOriginalErrorCallInfoIsFullGc = true
					}
					// clear gc list
					monitorConfig.CallChainOriginalErrorCallInfoGcList = []*typing.CallLocator{}
				}()

				// gc CallChainOriginalGasSiteInfo
				go func() {
					defer waitGroup.Done()
					monitorConfig.CallChainOriginalGasSiteInfoLock.Lock()
					defer monitorConfig.CallChainOriginalGasSiteInfoLock.Unlock()

					// record current size, junk size, peak size
					currentSize := len(monitorConfig.CallChainOriginalGasSiteInfo)
					if currentSize > monitorConfig.CallChainOriginalGasSiteInfoPeakSize {
						monitorConfig.CallChainOriginalGasSiteInfoPeakSize = currentSize
					}
					monitorConfig.CallChainOriginalGasSiteInfoJunkSize += len(monitorConfig.CallChainOriginalGasSiteInfoGcList)
					// remove dead entries
					for _, callLocator := range monitorConfig.CallChainOriginalGasSiteInfoGcList {
						delete(monitorConfig.CallChainOriginalGasSiteInfo, *callLocator)
						callChainOriginalGasSiteInfoGcCount += 1
					}
					// create a new map if necessary, to avoid Go's internal memory leak
					if shouldRenewMap(currentSize, monitorConfig.CallChainOriginalGasSiteInfoJunkSize, monitorConfig.CallChainOriginalGasSiteInfoPeakSize) {
						newMap := make(map[typing.CallLocator]*typing.GasSiteInfo)
						for key, value := range monitorConfig.CallChainOriginalGasSiteInfo {
							newMap[key] = value
						}
						monitorConfig.CallChainOriginalGasSiteInfo = newMap
						monitorConfig.CallChainOriginalGasSiteInfoJunkSize = 0
						monitorConfig.CallChainOriginalGasSiteInfoPeakSize = 0
						callChainOriginalGasSiteInfoIsFullGc = true
					}
					// clear gc list
					monitorConfig.CallChainOriginalGasSiteInfoGcList = []*typing.CallLocator{}
				}()

				waitGroup.Wait()
				elapsed := time.Since(start)

				//log.Warn("[gc] Dependency Info GC: removed " +
				//	strconv.Itoa(transactionsOriginalGasUsedGcCount) + " transactionGas entries, " +
				//	strconv.Itoa(callChainOriginalErrorCallInfoGcCount) + " callChainError entries, " +
				//	strconv.Itoa(callChainOriginalGasSiteInfoGcCount) + " callChainGas entries, in " +
				//	strconv.FormatFloat(elapsed.Seconds(), 'f', 4, 64) + "s")

				if transactionsOriginalGasUsedGcCount > 0 || callChainOriginalErrorCallInfoGcCount > 0 || callChainOriginalGasSiteInfoGcCount > 0 {
					var builder strings.Builder
					builder.WriteString("[gc] Dependency Info GC: removed ")
					builder.WriteString(strconv.Itoa(transactionsOriginalGasUsedGcCount))
					builder.WriteString(" transactionGas entries (")
					builder.WriteString(strconv.Itoa(monitorConfig.TransactionsOriginalGasUsedJunkSize))
					builder.WriteString("/")
					builder.WriteString(strconv.Itoa(monitorConfig.TransactionsOriginalGasUsedPeakSize))
					builder.WriteString(")")
					if transactionsOriginalGasUsedIsFullGc {
						builder.WriteString(" (full)")
					}
					builder.WriteString(", ")
					builder.WriteString(strconv.Itoa(callChainOriginalErrorCallInfoGcCount))
					builder.WriteString(" callChainError entries (")
					builder.WriteString(strconv.Itoa(monitorConfig.CallChainOriginalErrorCallInfoJunkSize))
					builder.WriteString("/")
					builder.WriteString(strconv.Itoa(monitorConfig.CallChainOriginalErrorCallInfoPeakSize))
					builder.WriteString(")")
					if callChainOriginalErrorCallInfoIsFullGc {
						builder.WriteString(" (full)")
					}
					builder.WriteString(", ")
					builder.WriteString(strconv.Itoa(callChainOriginalGasSiteInfoGcCount))
					builder.WriteString(" callChainGas entries (")
					builder.WriteString(strconv.Itoa(monitorConfig.CallChainOriginalGasSiteInfoJunkSize))
					builder.WriteString("/")
					builder.WriteString(strconv.Itoa(monitorConfig.CallChainOriginalGasSiteInfoPeakSize))
					builder.WriteString(")")
					if callChainOriginalGasSiteInfoIsFullGc {
						builder.WriteString(" (full)")
					}
					builder.WriteString(", in ")
					builder.WriteString(strconv.FormatFloat(elapsed.Seconds(), 'f', 4, 64))
					builder.WriteString("s")
					log.Warn(builder.String())
				}

			//}()
		}

	}
	// flush csv file buffers
	log.Warn("Flushing all csv files ...")
	util.FlushAllCsvFileBuffersAndSnippetBuffers(chain.GetVMConfig().MonitorConfig)
	log.Warn("Flush complete")

	return nil
}

func missingBlocks(chain *core.BlockChain, blocks []*types.Block) []*types.Block {
	head := chain.CurrentBlock()
	for i, block := range blocks {
		// If we're behind the chain head, only check block, state is available at head
		if head.NumberU64() > block.NumberU64() {
			if !chain.HasBlock(block.Hash(), block.NumberU64()) {
				return blocks[i:]
			}
			continue
		}
		// If we're above the chain head, state availability is a must
		if !chain.HasBlockAndState(block.Hash(), block.NumberU64()) {
			return blocks[i:]
		}
	}
	return nil
}

// ExportChain exports a blockchain into the specified file, truncating any data
// already present in the file.
func ExportChain(blockchain *core.BlockChain, fn string) error {
	log.Info("Exporting blockchain", "file", fn)

	// Open the file handle and potentially wrap with a gzip stream
	fh, err := os.OpenFile(fn, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer fh.Close()

	var writer io.Writer = fh
	if strings.HasSuffix(fn, ".gz") {
		writer = gzip.NewWriter(writer)
		defer writer.(*gzip.Writer).Close()
	}
	// Iterate over the blocks and export them
	if err := blockchain.Export(writer); err != nil {
		return err
	}
	log.Info("Exported blockchain", "file", fn)

	return nil
}

// ExportAppendChain exports a blockchain into the specified file, appending to
// the file if data already exists in it.
func ExportAppendChain(blockchain *core.BlockChain, fn string, first uint64, last uint64) error {
	log.Info("Exporting blockchain", "file", fn)

	// Open the file handle and potentially wrap with a gzip stream
	fh, err := os.OpenFile(fn, os.O_CREATE|os.O_APPEND|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer fh.Close()

	var writer io.Writer = fh
	if strings.HasSuffix(fn, ".gz") {
		writer = gzip.NewWriter(writer)
		defer writer.(*gzip.Writer).Close()
	}
	// Iterate over the blocks and export them
	if err := blockchain.ExportN(writer, first, last); err != nil {
		return err
	}
	log.Info("Exported blockchain to", "file", fn)
	return nil
}

// ImportPreimages imports a batch of exported hash preimages into the database.
func ImportPreimages(db ethdb.Database, fn string) error {
	log.Info("Importing preimages", "file", fn)

	// Open the file handle and potentially unwrap the gzip stream
	fh, err := os.Open(fn)
	if err != nil {
		return err
	}
	defer fh.Close()

	var reader io.Reader = fh
	if strings.HasSuffix(fn, ".gz") {
		if reader, err = gzip.NewReader(reader); err != nil {
			return err
		}
	}
	stream := rlp.NewStream(reader, 0)

	// Import the preimages in batches to prevent disk trashing
	preimages := make(map[common.Hash][]byte)

	for {
		// Read the next entry and ensure it's not junk
		var blob []byte

		if err := stream.Decode(&blob); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// Accumulate the preimages and flush when enough ws gathered
		preimages[crypto.Keccak256Hash(blob)] = common.CopyBytes(blob)
		if len(preimages) > 1024 {
			rawdb.WritePreimages(db, preimages)
			preimages = make(map[common.Hash][]byte)
		}
	}
	// Flush the last batch preimage data
	if len(preimages) > 0 {
		rawdb.WritePreimages(db, preimages)
	}
	return nil
}

// ExportPreimages exports all known hash preimages into the specified file,
// truncating any data already present in the file.
func ExportPreimages(db ethdb.Database, fn string) error {
	log.Info("Exporting preimages", "file", fn)

	// Open the file handle and potentially wrap with a gzip stream
	fh, err := os.OpenFile(fn, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, os.ModePerm)
	if err != nil {
		return err
	}
	defer fh.Close()

	var writer io.Writer = fh
	if strings.HasSuffix(fn, ".gz") {
		writer = gzip.NewWriter(writer)
		defer writer.(*gzip.Writer).Close()
	}
	// Iterate over the preimages and export them
	it := db.NewIterator([]byte("secure-key-"), nil)
	defer it.Release()

	for it.Next() {
		if err := rlp.Encode(writer, it.Value()); err != nil {
			return err
		}
	}
	log.Info("Exported preimages", "file", fn)
	return nil
}
