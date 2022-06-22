package util

import (
	"github.com/ethereum/go-ethereum/monitor/typing"
	"github.com/gofrs/flock"
	"os"
	"strings"
	"sync"
)

type Filename = string
type CsvLineData = []string
type CsvFileBuffer = map[Filename][]CsvLineData

var (
	bufferMaxSize int

	csvFileBuffer = make(CsvFileBuffer) // filename -> data array
	csvLock sync.Mutex

	snippetCsvFileBuffer = make(CsvFileBuffer)
	snippetCsvLock sync.Mutex
)

func SetBufferMaxSize(maxSize int) {
	bufferMaxSize = maxSize
}

// ------------------------------
// common
// ------------------------------

func FlushAllCsvFileBuffersAndSnippetBuffers(monitorConfig *typing.MonitorConfig) {
	if !monitorConfig.ReplaceMonitoredContractCode {
		FlushCsvFileBuffer(monitorConfig.MonitorCleanResultFile)
		FlushCsvFileBuffer(monitorConfig.MonitorCleanGasBriefFile)
		FlushCsvFileBuffer(monitorConfig.MonitorCallChainEvmErrorFile)
		FlushCsvFileBuffer(monitorConfig.MonitorCallChainGasSiteInfoFile)
		FlushCsvFileBuffer(monitorConfig.MonitorOpCodeStatisticsFile)

		FlushSnippetCsvFileBuffer(monitorConfig.CleanGasBriefSnippetFilename, monitorConfig.TransactionsOriginalGasUsedSnippetFileLock)
		FlushSnippetCsvFileBuffer(monitorConfig.CallChainEvmErrorSnippetFilename, monitorConfig.CallChainOriginalErrorCallInfoSnippetFileLock)
		FlushSnippetCsvFileBuffer(monitorConfig.CallChainGasSiteInfoSnippetFilename, monitorConfig.CallChainOriginalGasSiteInfoSnippetFileLock)
	} else {
		FlushCsvFileBuffer(monitorConfig.MonitorDirtyResultFile)
		FlushCsvFileBuffer(monitorConfig.MonitorDirtyGasBriefFile)
		FlushCsvFileBuffer(monitorConfig.MonitorErrorLogFile)
		FlushCsvFileBuffer(monitorConfig.MonitorOpCodeStatisticsFile)
		//FlushCsvFileBuffer(monitorConfig.MonitorCodeReplaceLogFile) // does not use the buffer
		FlushCsvFileBuffer(monitorConfig.MonitorOriginalBlockHashFile)
		FlushCsvFileBuffer(monitorConfig.MonitorTaintedBlockHashFile)
	}
}

// ------------------------------
// CsvFileBuffer
// ------------------------------

func SaveToCsvFileBuffer(file *os.File, data []string, logFilename string) {
	csvLock.Lock()
	defer csvLock.Unlock()

	saveToCsvFileBufferUnsafe(file, data)
	//size := saveToCsvFileBufferUnsafe(file, data)
	//if size >= bufferMaxSize {
	//	flushedSize := flushCsvFileBufferUnsafe(file)
	//	if flushedSize > 0 {
	//		log.Warn("[implicit-flush] Flushed " + strconv.Itoa(flushedSize) + " entries to " + logFilename)
	//	}
	//}
}

// saveToCsvFileBufferUnsafe WARNING: thread-unsafe
// should be called from a thread-safe function
func saveToCsvFileBufferUnsafe(file *os.File, data []string) int {
	filename := file.Name()
	if _, exist := csvFileBuffer[filename]; !exist {
		csvFileBuffer[filename] = []CsvLineData{}
	}
	buffer := csvFileBuffer[filename]
	buffer = append(buffer, data)
	csvFileBuffer[filename] = buffer
	return len(buffer)
}

func FlushCsvFileBuffer(file *os.File) int {
	csvLock.Lock()
	defer csvLock.Unlock()

	return flushCsvFileBufferUnsafe(file)
}

// FlushCsvFileBuffer WARNING: thread-unsafe
// should be called from a thread-safe function
func flushCsvFileBufferUnsafe(file *os.File) int {
	filename := file.Name()
	lineData := csvFileBuffer[filename]
	dataSize := len(lineData)
	if dataSize == 0 {  // no data to flush
		return 0
	}
	var builder strings.Builder
	for _, bufferLine := range lineData {
		first := true
		for _, item := range bufferLine {
			if first {
				first = false
			} else {
				builder.WriteString(",")
			}
			if len(item) > 0 {
				builder.WriteString(escapeCsvString(item))
			}
		}
		builder.WriteString("\n")
	}
	_, err := file.WriteString(builder.String())
	PanicOnError(err)
	csvFileBuffer[filename] = []CsvLineData{} // clear buffer
	return dataSize
}

// ------------------------------
// SnippetCsvFileBuffer
// ------------------------------

func SaveToSnippetCsvFileBuffer(filename string, fileLock *flock.Flock, data []string, logFilename string) {
	snippetCsvLock.Lock()
	defer snippetCsvLock.Unlock()

	saveToSnippetCsvFileBufferUnsafe(filename, data)
	//size := saveToSnippetCsvFileBufferUnsafe(filename, data)
	//if size >= bufferMaxSize {
	//	flushedSize := flushSnippetCsvFileBufferUnsafe(filename, fileLock)
	//	if flushedSize > 0 {
	//		log.Warn("[implicit-flush] Flushed " + strconv.Itoa(flushedSize) + " entries to " + logFilename)
	//	}
	//}
}

func saveToSnippetCsvFileBufferUnsafe(filename string, data []string) int {
	if _, exist := snippetCsvFileBuffer[filename]; !exist {
		snippetCsvFileBuffer[filename] = []CsvLineData{}
	}
	buffer := snippetCsvFileBuffer[filename]
	buffer = append(buffer, data)
	snippetCsvFileBuffer[filename] = buffer
	return len(buffer)
}

func flushSnippetCsvFileBufferUnsafe(filename string, fileLock *flock.Flock) int {
	lineData := snippetCsvFileBuffer[filename]
	dataSize := len(lineData)
	if dataSize == 0 {  // no data to flush
		return 0
	}
	var builder strings.Builder
	for _, bufferLine := range lineData {
		first := true
		for _, item := range bufferLine {
			if first {
				first = false
			} else {
				builder.WriteString(",")
			}
			if len(item) > 0 {
				builder.WriteString(escapeCsvString(item))
			}
		}
		builder.WriteString("\n")
	}
	dumpIntoSnippetCsvFile(filename, fileLock, builder.String())
	snippetCsvFileBuffer[filename] = []CsvLineData{} // clear buffer
	return dataSize
}

func dumpIntoSnippetCsvFile(filename string, snippetCsvFileLock *flock.Flock, data string) {
	err := snippetCsvFileLock.Lock()
	PanicOnError(err)
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	PanicOnError(err)
	_, err = file.WriteString(data)
	PanicOnError(err)
	err = file.Close()
	PanicOnError(err)
	err = snippetCsvFileLock.Unlock()
	PanicOnError(err)
}

func FlushSnippetCsvFileBuffer(filename string, fileLock *flock.Flock) int {
	snippetCsvLock.Lock()
	defer snippetCsvLock.Unlock()

	return flushSnippetCsvFileBufferUnsafe(filename, fileLock)
}
