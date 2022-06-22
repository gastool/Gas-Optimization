package util

import (
	"github.com/ethereum/go-ethereum/monitor/typing"
	"github.com/gofrs/flock"
	"os"
	"strconv"
	"strings"
	"sync"
)

var (
	transactionsOriginalGasUsedLoadCount int
	callChainOriginalEvmErrorsLineLoadCount int
	callChainOriginalGasSiteInfoLineLoadCount int
)

// LoadTransactionsOriginalGasUsed WARNING: thread-unsafe; lock should be used
func LoadTransactionsOriginalGasUsed(cleanGasBriefFilename string, transactionsOriginalGasUsed map[string]uint64, lock *sync.RWMutex) int {
	//errOutOfGas := "out of gas"
	//errCodeStoreOutOfGas := "contract creation code storage out of gas"
	//errGasUintOverflow := "gas uint64 overflow"
	//errInvalidJump := "invalid jump destination"

	loadCount := 0
	locked := false
	ReadCsvFileByLineWithSkip(cleanGasBriefFilename, transactionsOriginalGasUsedLoadCount, func() {
		if lock != nil {
			lock.Lock()
			locked = true
		}
	}, func(record []string) {
		transactionHash := strings.ToLower(record[0])
		gasUsedString := record[1]
		gasUsed, err := strconv.ParseUint(gasUsedString, 10, 64)
		PanicOnError(err)
		//vmErr := record[2]
		// record gas only if vmErr is not gas-related error
		//if vmErr != errOutOfGas && vmErr != errCodeStoreOutOfGas && vmErr != errGasUintOverflow {
			if originalGasUsed, exist := transactionsOriginalGasUsed[transactionHash]; exist {
				if originalGasUsed != gasUsed {
					panic("LoadTransactionsOriginalGasUsed: Inconsistent data detected: newGasUsed != oldGasUsed, txHash = " + 
						transactionHash + ", newGasUsed = " + gasUsedString + ", oldGasUsed = " + strconv.FormatUint(originalGasUsed, 10))
				}
			} else {
				transactionsOriginalGasUsed[transactionHash] = gasUsed
				loadCount += 1
			}
		//}
	}, func() {
		if locked {
			lock.Unlock()
		}
	})
	transactionsOriginalGasUsedLoadCount += loadCount
	return loadCount
}

// LoadCallChainOriginalEvmErrors WARNING: thread-unsafe; lock should be used
func LoadCallChainOriginalEvmErrors(callChainEvmErrorFilename string, callChainOriginalErrorCallInfo map[typing.CallLocator]*typing.CallInfo, lock *sync.RWMutex)  (/* loadCount */ int, /* latestBlockNumber */ uint64) {
	loadCount := 0
	locked := false
	var maxBlockNumber uint64 = 0
	ReadCsvFileByLineWithSkip(callChainEvmErrorFilename, callChainOriginalEvmErrorsLineLoadCount, func() {
		if lock != nil {
			lock.Lock()
			locked = true
		}
	},
	func(record []string) {
		blockNumberString := record[0]
		blockNumber, err := strconv.ParseUint(blockNumberString, 10, 64)
		PanicOnError(err)
		transactionIndexString := record[1]
		transactionIndex, err := strconv.ParseUint(transactionIndexString, 10, 64)
		PanicOnError(err)
		callChainIndexesString := record[2]
		callChainEvmErrorMessage := record[3]
		callChainCallTypeString := record[4]
		callChainCallType := typing.ParseCallType(callChainCallTypeString)
		callChainToAddressTail := record[5]
		callChainValue := record[6]

		if blockNumber > maxBlockNumber {
			maxBlockNumber = blockNumber
		}

		callLocator := typing.CallLocator{
			BlockNumber:      blockNumber,
			TransactionIndex: transactionIndex,
			CallChainIndexes: callChainIndexesString,
		}
		if originalErrorCallInfo, exist := callChainOriginalErrorCallInfo[callLocator]; exist {
			if originalErrorCallInfo.ErrorMessage != callChainEvmErrorMessage {
				panic("LoadCallChainOriginalEvmErrors: Inconsistent data detected: newEvmError != oldEvmError, blockNumber = " +
					blockNumberString + ", transactionIndex = " + transactionIndexString + ", newEvmError = " + callChainEvmErrorMessage +
					", oldEvmError = " + originalErrorCallInfo.ErrorMessage)
			}
		} else {
			callChainOriginalErrorCallInfo[callLocator] = &typing.CallInfo{
				ErrorMessage:  callChainEvmErrorMessage,
				ToAddressTail: callChainToAddressTail,
				Value:         callChainValue,
				Type:          callChainCallType,
			}
			loadCount += 1
		}
	}, func() {
		if locked {
			lock.Unlock()
		}
	})
	callChainOriginalEvmErrorsLineLoadCount += loadCount

	var maxNumber uint64
	if maxBlockNumber == 0 {
		maxNumber = 0
	} else {
		maxNumber = maxBlockNumber - 1
	}
	return loadCount, maxNumber
}

// LoadCallChainOriginalGasSiteInfo WARNING: thread-unsafe; lock should be used
func LoadCallChainOriginalGasSiteInfo(callChainOriginalGasSiteInfoFilename string, callChainOriginalGasSiteInfo map[typing.CallLocator]*typing.GasSiteInfo, lock *sync.RWMutex) (/* loadCount */ int, /* latestBlockNumber */ uint64) {
	loadCount := 0
	locked := false
	var maxBlockNumber uint64 = 0
	ReadCsvFileByLineWithSkip(callChainOriginalGasSiteInfoFilename, callChainOriginalGasSiteInfoLineLoadCount, func() {
		if lock != nil {
			lock.Lock()
			locked = true
		}
	}, func(record []string) {
		blockNumberString := record[0]
		blockNumber, err := strconv.ParseUint(blockNumberString, 10, 64)
		PanicOnError(err)
		transactionIndexString := record[1]
		transactionIndex, err := strconv.ParseUint(transactionIndexString, 10, 64)
		PanicOnError(err)
		callChainIndexesString := record[2]
		transientGasString := record[3]
		transientGas, err := strconv.ParseUint(transientGasString, 10, 64)
		PanicOnError(err)
		callChainCallTypeString := record[4]
		callChainCallType := typing.ParseCallType(callChainCallTypeString)
		callChainToAddressTail := record[5]
		callChainValue := record[6]

		if blockNumber > maxBlockNumber {
			maxBlockNumber = blockNumber
		}

		callLocator := typing.CallLocator{
			BlockNumber:      blockNumber,
			TransactionIndex: transactionIndex,
			CallChainIndexes: callChainIndexesString,
		}
		if originalGasSiteInfo, exist := callChainOriginalGasSiteInfo[callLocator]; exist {
			if originalGasSiteInfo.TransientGas != transientGas {
				panic("LoadCallChainOriginalGasSiteInfo: Inconsistent data detected: newTransientGas != oldTransientGas, blockNumber = " +
					blockNumberString + ", transactionIndex = " + transactionIndexString + ", newTransientGas = " + transientGasString +
					", oldTransientGas = " + strconv.FormatUint(originalGasSiteInfo.TransientGas, 10))
			}
		} else {
			callChainOriginalGasSiteInfo[callLocator] = &typing.GasSiteInfo{
				TransientGas:  transientGas,
				ToAddressTail: callChainToAddressTail,
				Value:         callChainValue,
				Type:          callChainCallType,
			}
			loadCount += 1
		}
	}, func() {
		if locked {
			lock.Unlock()
		}
	})
	callChainOriginalGasSiteInfoLineLoadCount += loadCount

	var maxNumber uint64
	if maxBlockNumber == 0 {
		maxNumber = 0
	} else {
		maxNumber = maxBlockNumber - 1
	}
	return loadCount, maxNumber
}

func LoadTransactionsOriginalGasUsedAndTruncateFile(cleanGasBriefSnippetFilename string, transactionsOriginalGasUsed map[string]uint64, dataLock *sync.RWMutex, fileLock *flock.Flock, truncateSize int64) int {
	//errOutOfGas := "out of gas"
	//errCodeStoreOutOfGas := "contract creation code storage out of gas"
	//errGasUintOverflow := "gas uint64 overflow"

	PanicOnError(fileLock.Lock())
	if dataLock != nil {
		dataLock.Lock()
	}
	loadCount := 0
	ReadCsvFileByLine(cleanGasBriefSnippetFilename, func(record []string) {
		transactionHash := strings.ToLower(record[0])
		gasUsedString := record[1]
		gasUsed, err := strconv.ParseUint(gasUsedString, 10, 64)
		PanicOnError(err)
		//vmErr := record[2]
		// record gas only if vmErr is not gas-related error
		//if vmErr != errOutOfGas && vmErr != errCodeStoreOutOfGas && vmErr != errGasUintOverflow {
			if originalGasUsed, exist := transactionsOriginalGasUsed[transactionHash]; exist {
				if originalGasUsed != gasUsed {
					panic("LoadTransactionsOriginalGasUsed: Inconsistent data detected: newGasUsed != oldGasUsed, txHash = " +
						transactionHash + ", newGasUsed = " + gasUsedString + ", oldGasUsed = " + strconv.FormatUint(originalGasUsed, 10))
				}
			} else {
				transactionsOriginalGasUsed[transactionHash] = gasUsed
				loadCount += 1
			}
		//}
	}, nil)
	if dataLock != nil {
		dataLock.Unlock()
	}
	if loadCount > 0 {
		PanicOnError(os.Truncate(cleanGasBriefSnippetFilename, truncateSize))
	}
	PanicOnError(fileLock.Unlock())

	return loadCount
}

func LoadCallChainOriginalEvmErrorsAndTruncateFile(callChainEvmErrorSnippetFilename string, callChainOriginalErrorCallInfo map[typing.CallLocator]*typing.CallInfo, dataLock *sync.RWMutex, fileLock *flock.Flock, truncateSize int64) (/* loadCount */ int, /* latestBlockNumber */ uint64) {
	PanicOnError(fileLock.Lock())
	if dataLock != nil {
		dataLock.Lock()
	}
	loadCount := 0
	var maxBlockNumber uint64 = 0
	ReadCsvFileByLine(callChainEvmErrorSnippetFilename, func(record []string) {
		blockNumberString := record[0]
		blockNumber, err := strconv.ParseUint(blockNumberString, 10, 64)
		PanicOnError(err)
		transactionIndexString := record[1]
		transactionIndex, err := strconv.ParseUint(transactionIndexString, 10, 64)
		PanicOnError(err)
		callChainIndexesString := record[2]
		callChainEvmErrorMessage := record[3]
		callChainCallTypeString := record[4]
		callChainCallType := typing.ParseCallType(callChainCallTypeString)
		callChainToAddressTail := record[5]
		callChainValue := record[6]

		if blockNumber > maxBlockNumber {
			maxBlockNumber = blockNumber
		}

		callLocator := typing.CallLocator{
			BlockNumber:      blockNumber,
			TransactionIndex: transactionIndex,
			CallChainIndexes: callChainIndexesString,
		}
		if originalErrorCallInfo, exist := callChainOriginalErrorCallInfo[callLocator]; exist {
			if originalErrorCallInfo.ErrorMessage != callChainEvmErrorMessage {
				panic("LoadCallChainOriginalEvmErrors: Inconsistent data detected: newEvmError != oldEvmError, blockNumber = " +
					blockNumberString + ", transactionIndex = " + transactionIndexString + ", newEvmError = " + callChainEvmErrorMessage +
					", oldEvmError = " + originalErrorCallInfo.ErrorMessage)
			}
		} else {
			callChainOriginalErrorCallInfo[callLocator] = &typing.CallInfo{
				ErrorMessage:  callChainEvmErrorMessage,
				ToAddressTail: callChainToAddressTail,
				Value:         callChainValue,
				Type:          callChainCallType,
			}
			loadCount += 1
		}
	}, nil)
	if dataLock != nil {
		dataLock.Unlock()
	}
	if loadCount > 0 {
		PanicOnError(os.Truncate(callChainEvmErrorSnippetFilename, truncateSize))
	}
	PanicOnError(fileLock.Unlock())

	var maxNumber uint64
	if maxBlockNumber == 0 {
		maxNumber = 0
	} else {
		maxNumber = maxBlockNumber - 1
	}
	return loadCount, maxNumber
}

func LoadCallChainOriginalGasSiteInfoAndTruncateFile(callChainOriginalGasSiteInfoSnippetFilename string, callChainOriginalGasSiteInfo map[typing.CallLocator]*typing.GasSiteInfo, dataLock *sync.RWMutex, fileLock *flock.Flock, truncateSize int64) (/* loadCount */ int, /* latestBlockNumber */ uint64) {
	PanicOnError(fileLock.Lock())
	if dataLock != nil {
		dataLock.Lock()
	}
	loadCount := 0
	var maxBlockNumber uint64 = 0
	ReadCsvFileByLine(callChainOriginalGasSiteInfoSnippetFilename, func(record []string) {
		blockNumberString := record[0]
		blockNumber, err := strconv.ParseUint(blockNumberString, 10, 64)
		PanicOnError(err)
		transactionIndexString := record[1]
		transactionIndex, err := strconv.ParseUint(transactionIndexString, 10, 64)
		PanicOnError(err)
		callChainIndexesString := record[2]
		transientGasString := record[3]
		transientGas, err := strconv.ParseUint(transientGasString, 10, 64)
		PanicOnError(err)
		callChainCallTypeString := record[4]
		callChainCallType := typing.ParseCallType(callChainCallTypeString)
		callChainToAddressTail := record[5]
		callChainValue := record[6]

		if blockNumber > maxBlockNumber {
			maxBlockNumber = blockNumber
		}

		callLocator := typing.CallLocator{
			BlockNumber:      blockNumber,
			TransactionIndex: transactionIndex,
			CallChainIndexes: callChainIndexesString,
		}
		if originalGasSiteInfo, exist := callChainOriginalGasSiteInfo[callLocator]; exist {
			if originalGasSiteInfo.TransientGas != transientGas {
				panic("LoadCallChainOriginalGasSiteInfo: Inconsistent data detected: newTransientGas != oldTransientGas, blockNumber = " +
					blockNumberString + ", transactionIndex = " + transactionIndexString + ", newTransientGas = " + transientGasString +
					", oldTransientGas = " + strconv.FormatUint(originalGasSiteInfo.TransientGas, 10))
			}
		} else {
			callChainOriginalGasSiteInfo[callLocator] = &typing.GasSiteInfo{
				TransientGas:  transientGas,
				ToAddressTail: callChainToAddressTail,
				Value:         callChainValue,
				Type:          callChainCallType,
			}
			loadCount += 1
		}
	}, nil)
	if dataLock != nil {
		dataLock.Unlock()
	}
	if loadCount > 0 {
		PanicOnError(os.Truncate(callChainOriginalGasSiteInfoSnippetFilename, truncateSize))
	}
	PanicOnError(fileLock.Unlock())

	var maxNumber uint64
	if maxBlockNumber == 0 {
		maxNumber = 0
	} else {
		maxNumber = maxBlockNumber - 1
	}
	return loadCount, maxNumber
}
