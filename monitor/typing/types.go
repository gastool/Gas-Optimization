package typing

import (
	"github.com/gofrs/flock"
	"os"
	"sync"
)

type MonitorConfig struct {
	MonitorCleanResultFile                    *os.File // not nil when ReplaceMonitoredContractCode == false
	MonitorCleanGasBriefFile                  *os.File // not nil when ReplaceMonitoredContractCode == false
	MonitorDirtyResultFile                    *os.File // not nil when ReplaceMonitoredContractCode == true
	MonitorDirtyGasBriefFile                  *os.File // not nil when ReplaceMonitoredContractCode == true
	MonitorErrorLogFile                       *os.File // not nil when ReplaceMonitoredContractCode == true
	MonitorCodeReplaceLogFile                 *os.File // not nil when ReplaceMonitoredContractCode == true
	MonitorCallChainEvmErrorFile              *os.File // not nil when ReplaceMonitoredContractCode == false
	MonitorCallChainGasSiteInfoFile           *os.File // not nil when ReplaceMonitoredContractCode == false
	MonitorOpCodeStatisticsFile               *os.File
	MonitorOriginalBlockHashFile              *os.File // not nil when ReplaceMonitoredContractCode == true
	MonitorTaintedBlockHashFile               *os.File // not nil when ReplaceMonitoredContractCode == true

	CleanGasBriefFilename                     string   // used when DynamicallyUpdateDependencyInfo && ReplaceMonitoredContractCode
	CallChainEvmErrorFilename                 string   // used when DynamicallyUpdateDependencyInfo && ReplaceMonitoredContractCode
	CallChainGasSiteInfoFilename              string   // used when DynamicallyUpdateDependencyInfo && ReplaceMonitoredContractCode
	CleanGasBriefSnippetFilename              string
	CallChainEvmErrorSnippetFilename          string
	CallChainGasSiteInfoSnippetFilename       string

	MonitoredAddresses                        map[string]*MonitoredContractInfo
	TransactionsOriginalGasUsed               map[string]uint64             // not nil when ReplaceMonitoredContractCode == true
	CallChainOriginalErrorCallInfo            map[CallLocator]*CallInfo     // not nil when ReplaceMonitoredContractCode == true
	CallChainOriginalGasSiteInfo              map[CallLocator]*GasSiteInfo  // not nil when ReplaceMonitoredContractCode == true

	TransactionsOriginalGasUsedGcList         []string
	CallChainOriginalErrorCallInfoGcList      []*CallLocator  // WARNING: may contain duplicated elements
	CallChainOriginalGasSiteInfoGcList        []*CallLocator
	TransactionsOriginalGasUsedPeakSize       int
	CallChainOriginalErrorCallInfoPeakSize    int
	CallChainOriginalGasSiteInfoPeakSize      int
	TransactionsOriginalGasUsedJunkSize       int
	CallChainOriginalErrorCallInfoJunkSize    int
	CallChainOriginalGasSiteInfoJunkSize      int

	DependencyInfoLatestBlockNumber           uint64

	TransactionsOriginalGasUsedLock               *sync.RWMutex
	CallChainOriginalErrorCallInfoLock            *sync.RWMutex
	CallChainOriginalGasSiteInfoLock              *sync.RWMutex
	TransactionsOriginalGasUsedSnippetFileLock    *flock.Flock
	CallChainOriginalErrorCallInfoSnippetFileLock *flock.Flock
	CallChainOriginalGasSiteInfoSnippetFileLock   *flock.Flock

	ReplaceMonitoredContractCode              bool
	ContractBeforeOptimizationDir             string
	ContractAfterOptimizationDir              string
	ContractAfterOptimizationDisableDir       string
	IncludeMinerTransactions                  bool
	ForceFlushKeyCsvFileBufferPerBatches      int
	ForceFlushLessKeyCsvFileBufferPerBatches  int
	DynamicallyUpdateDependencyInfo           bool
	DynamicallyUpdateDependencyInfoPerBatches int
	DependencyInfoGC                          bool
	DependencyInfoGCPerBatches                int

	CleanGasBriefSnippetFileTruncateSize        int64
	CallChainEvmErrorSnippetFileTruncateSize    int64
	CallChainGasSiteInfoSnippetFileTruncateSize int64
}

type CallLocator struct {
	BlockNumber      uint64
	TransactionIndex uint64
	CallChainIndexes CallChainIndexesString
}

type CallChainIndexesString = string

type MonitoredContractInfo struct {
	ContractName string
	OptimizerEnabled bool
	ConstructorArgumentString string
}

type CallInfo struct {
	ErrorMessage  string
	ToAddressTail string
	Value         string
	Type          CallType
}

type GasSiteInfo struct {
	TransientGas    uint64
	ToAddressTail   string
	Value           string
	Type            CallType
}

type CallType int

const (
	Unknown CallType = iota
	Call
	StaticCall
	DelegateCall
	CallCode
	Create
	Create2
)

func (callType CallType) ReadableString() string {
	switch callType {
	case Unknown:      return "Unknown"
	case Call:         return "Call"
	case StaticCall:   return "StaticCall"
	case DelegateCall: return "DelegateCall"
	case CallCode:     return "CallCode"
	case Create:       return "Create"
	case Create2:      return "Create2"
	default:           panic("impossible")
	}
}

func (callType CallType) IntString() string {
	switch callType {
	case Unknown:      return "0"
	case Call:         return "1"
	case StaticCall:   return "2"
	case DelegateCall: return "3"
	case CallCode:     return "4"
	case Create:       return "5"
	case Create2:      return "6"
	default:           panic("impossible")
	}
}

func ParseCallType(callTypeString string) CallType {
	switch callTypeString {
	case "0": return Unknown
	case "1": return Call
	case "2": return StaticCall
	case "3": return DelegateCall
	case "4": return CallCode
	case "5": return Create
	case "6": return Create2
	default: panic("Illegal callTypeString <" + callTypeString + ">")
	}
}
