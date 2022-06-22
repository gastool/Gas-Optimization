package util

import (
	"encoding/json"
	"errors"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/monitor/typing"
	lru "github.com/hashicorp/golang-lru"
	"io/ioutil"
	"strconv"
	"strings"
)

const cacheSize = 20000
var creationBytecodeCache, _ = lru.New(cacheSize)
var deployedBytecodeCache, _ = lru.New(cacheSize)

// cache value of creationBytecodeCache and deployedBytecodeCache
type codeOrError struct {
	code []byte
	err error
}

func LoadReplacedCreationBytecode(monitorConfig *typing.MonitorConfig, address common.Address) ([]byte, error) {
	if cacheValue, exist := creationBytecodeCache.Get(address); exist {
		return cacheValue.(codeOrError).code, cacheValue.(codeOrError).err
	}
	contractInfo, err := loadReplacedContractInfo(monitorConfig, address)
	if err != nil {
		creationBytecodeCache.Add(address, codeOrError{err: err})
		return nil, err
	}
	if len(contractInfo.Evm.Bytecode.Object) == 0 {
		panic("Loaded zero-length contract bytecode")
	}
	if strings.Contains(contractInfo.Evm.Bytecode.Object, "__") {
		err := errors.New("PLACEHOLDER_FOUND_IN_CREATION_BYTECODE")
		creationBytecodeCache.Add(address, codeOrError{err: err})
		return nil, err
	}
	monitoredContractInfo := monitorConfig.MonitoredAddresses[ConvertAddressToLowerCaseString(address)]
	code := turnCodeIntoBytes(contractInfo.Evm.Bytecode.Object + monitoredContractInfo.ConstructorArgumentString)
	creationBytecodeCache.Add(address, codeOrError{code: code})
	return code, nil
}

func LoadReplacedDeployedBytecode(monitorConfig *typing.MonitorConfig, address common.Address) ([]byte, error) {
	if cacheValue, exist := deployedBytecodeCache.Get(address); exist {
		return cacheValue.(codeOrError).code, cacheValue.(codeOrError).err
	}
	contractInfo, err := loadReplacedContractInfo(monitorConfig, address)
	if err != nil {
		deployedBytecodeCache.Add(address, codeOrError{err: err})
		return nil, err
	}
	if len(contractInfo.Evm.DeployedBytecode.Object) == 0 {
		panic("Loaded zero-length contract deployed bytecode")
	}
	if strings.Contains(contractInfo.Evm.DeployedBytecode.Object, "__") {
		err := errors.New("PLACEHOLDER_FOUND_IN_DEPLOYED_BYTECODE")
		deployedBytecodeCache.Add(address, codeOrError{err: err})
		return nil, err
	}
	code := turnCodeIntoBytes(contractInfo.Evm.DeployedBytecode.Object)
	deployedBytecodeCache.Add(address, codeOrError{code: code})
	return code, nil
}

func CheckCompiledCodeExist(addressString string, name string, compiledPath string) bool {
	if !strings.HasSuffix(compiledPath, "/") {
		panic("compiledPath must ends with '/'")
	}
	compiledFilename := compiledPath + strings.ToLower(addressString + ".json")
	return FileExists(compiledFilename)
}

func loadReplacedContractInfo(monitorConfig *typing.MonitorConfig, address common.Address) (*compiledContractContractFileInfo, error) {
	lowerCaseAddress := ConvertAddressToLowerCaseString(address)
	monitoredContractInfo := monitorConfig.MonitoredAddresses[lowerCaseAddress]

	var compiledPath string
	if monitoredContractInfo.OptimizerEnabled {
		compiledPath = monitorConfig.ContractAfterOptimizationDir
	} else {
		compiledPath = monitorConfig.ContractAfterOptimizationDisableDir
	}
	if !strings.HasSuffix(compiledPath, "/") {
		panic("compiledPath must ends with '/'")
	}
	compiledFilename := compiledPath + strings.ToLower(address.String() + ".json")
	if !FileExists(compiledFilename) {
		return nil, errors.New("COMPILED_FILE_NOT_FOUND")
	}
	compileFile, err := ioutil.ReadFile(compiledFilename)
	PanicOnError(err)
	var compiledContract compiledContract
	err = json.Unmarshal(compileFile, &compiledContract)
	PanicOnError(err)
	for _, compileError := range compiledContract.Errors {
		if compileError.Severity == "error" {
			return nil, errors.New("COMPILE_ERROR")
		}
	}

	// compiled json file may have two possible formats
	contractsInfo, rootExist := compiledContract.Contracts["TestContract.sol"]
	if !rootExist {
		contractsInfo, rootExist = compiledContract.Contracts[""]
	}
	if !rootExist {
		return nil, errors.New("ILLEGAL_COMPILED_FILE_FORMAT")
	}
	contractInfo, contractExist := contractsInfo[monitoredContractInfo.ContractName]
	if !contractExist {
		return nil, errors.New("CONTRACT_NAME_NOT_FOUND_IN_COMPILED_FILE")
	}
	return &contractInfo, nil
}

type compiledContract struct {
	Errors []compiledContractError                                   `json:"errors"`
	Contracts map[string]map[string]compiledContractContractFileInfo `json:"contracts"` // fileName -> contractName ->
}

type compiledContractError struct {
	Severity string `json:"severity"`
}

type compiledContractContractFileInfo struct {
	Evm compiledContractContractFileInfoEvm `json:"evm"`
}

type compiledContractContractFileInfoEvm struct {
	Bytecode         compiledContractContractFileInfoEvmByteCode `json:"bytecode"`
	DeployedBytecode compiledContractContractFileInfoEvmByteCode `json:"deployedBytecode"`
}

type compiledContractContractFileInfoEvmByteCode struct {
	Object string `json:"object"`
}

func turnCodeIntoBytes(code string) []byte {
	var result []byte
	for i := 0; i < len(code); i += 2 {
		stringUnit := code[i : i + 2]
		byteUnit, err := strconv.ParseUint(stringUnit, 16, 64)
		PanicOnError(err)
		result = append(result, byte(byteUnit))
	}
	return result
}
