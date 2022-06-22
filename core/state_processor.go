// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/consensus/misc"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/monitor/util"
	"github.com/ethereum/go-ethereum/params"
	"os"
	"strconv"
	"strings"
	"time"
)

// StateProcessor is a basic Processor, which takes care of transitioning
// state from one point to another.
//
// StateProcessor implements Processor.
type StateProcessor struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards
}

// NewStateProcessor initialises a new StateProcessor.
func NewStateProcessor(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *StateProcessor {
	return &StateProcessor{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

// Process processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb and applying any rewards to both
// the processor (coinbase) and any included uncles.
//
// Process returns the receipts and logs accumulated during the process and
// returns the amount of gas that was used in the process. If any of the
// transactions failed to execute due to insufficient gas it will return an error.
func (p *StateProcessor) Process(block *types.Block, statedb *state.StateDB, cfg vm.Config) (types.Receipts, []*types.Log, uint64, error) {
	var (
		receipts types.Receipts
		usedGas  = new(uint64)
		header   = block.Header()
		allLogs  []*types.Log
		gp       = new(GasPool).AddGas(block.GasLimit())
	)
	// Mutate the block and state according to any hard-fork specs
	if p.config.DAOForkSupport && p.config.DAOForkBlock != nil && p.config.DAOForkBlock.Cmp(block.Number()) == 0 {
		misc.ApplyDAOHardFork(statedb)
	}
	blockContext := NewEVMBlockContext(header, p.bc, nil)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)
	// Iterate over and process the individual transactions
	for i, tx := range block.Transactions() {
		msg, err := tx.AsMessage(types.MakeSigner(p.config, header.Number))
		if err != nil {
			return nil, nil, 0, err
		}
		statedb.Prepare(tx.Hash(), block.Hash(), i)
		receipt, err := applyTransaction(msg, p.config, p.bc, nil, gp, statedb, header, tx, usedGas, vmenv)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("could not apply tx %d [%v]: %w", i, tx.Hash().Hex(), err)
		}

		// [IMPORTANT]
		// fake the header root by the root hash of tainted trie
		//hash := new(common.Hash)
		//hash.SetBytes(receipt.PostState)
		//block.HeaderNoCopy().Root = *hash

		receipts = append(receipts, receipt)
		allLogs = append(allLogs, receipt.Logs...)
	}
	// Finalize the block, applying any consensus engine specific extras (e.g. block rewards)
	//p.engine.Finalize(p.bc, header, statedb, block.Transactions(), block.Uncles())

	// [IMPORTANT]
	// fake the header root
	p.engine.Finalize(p.bc, block.HeaderNoCopy(), statedb, block.Transactions(), block.Uncles())

	if vmenv.MonitorConfig.ReplaceMonitoredContractCode {
		// invalidate all caches
		block.ClearHashAndSizeCache()
		//p.bc.bodyCache.Purge()
		//p.bc.bodyRLPCache.Purge()
		//p.bc.receiptsCache.Purge()
		//p.bc.blockCache.Purge()
		//p.bc.txLookupCache.Purge()
		//p.bc.futureBlocks.Purge()
		//p.bc.badBlocks.Purge()
		//p.bc.hc.headerCache.Purge()
		//p.bc.hc.numberCache.Purge()
		//p.bc.hc.tdCache.Purge()

		// save to global cache if not exist
		util.SaveTaintedBlockHash(block.NumberU64(), block.Hash())
	}

	return receipts, allLogs, *usedGas, nil
}

func applyTransaction(msg types.Message, config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, evm *vm.EVM) (*types.Receipt, error) {
	//lowerCaseAddress := util.ConvertAddressToLowerCaseString(msg.From())
	//_, shouldMonitor := evm.MonitorConfig.MonitoredAddresses[lowerCaseAddress]
	//shouldMonitor := false

	if evm.MonitorConfig.ReplaceMonitoredContractCode {
		txHashString := strings.ToLower(tx.Hash().String())
		evm.MonitorConfig.TransactionsOriginalGasUsedLock.Lock() // cannot use RLock() because we need to write to gc map
		if originalGasUsed, exist := evm.MonitorConfig.TransactionsOriginalGasUsed[txHashString]; exist {
			msg.OriginalGasUsed = originalGasUsed
			if evm.MonitorConfig.DependencyInfoGC {
				evm.MonitorConfig.TransactionsOriginalGasUsedGcList = append(evm.MonitorConfig.TransactionsOriginalGasUsedGcList, txHashString) // add to gc list
			}
		}
		evm.MonitorConfig.TransactionsOriginalGasUsedLock.Unlock()
	}
	evm.TransactionHash = tx.Hash()

	// Create a new context to be used in the EVM environment
	txContext := NewEVMTxContext(msg)
	// Add addresses to access list if applicable
	if config.IsYoloV2(header.Number) {
		statedb.AddAddressToAccessList(msg.From())
		if dst := msg.To(); dst != nil {
			statedb.AddAddressToAccessList(*dst)
			// If it's a create-tx, the destination will be added inside evm.create
		}
		for _, addr := range evm.ActivePrecompiles() {
			statedb.AddAddressToAccessList(addr)
		}
	}

	// Update the evm with the new transaction context.
	evm.Reset(txContext, statedb)
	// Apply the transaction to the current state (included in the env)
	result, err := ApplyMessage(evm, msg, gp)
	if err != nil {
		return nil, err
	}
	postJournal := evm.StateDB.DumpJournal()
	entries := evm.StateDB.Dirties()

	// Update the state with pending changes
	var root []byte
	if config.IsByzantium(header.Number) {
		statedb.Finalise(true)
	} else {
		root = statedb.IntermediateRoot(config.IsEIP158(header.Number)).Bytes()
	}
	*usedGas += result.UsedGas

	// Create a new receipt for the transaction, storing the intermediate root and gas used by the tx
	// based on the eip phase, we're passing whether the root touch-delete accounts.
	receipt := types.NewReceipt(root, result.Failed(), *usedGas)
	receipt.TxHash = tx.Hash()
	receipt.GasUsed = result.UsedGas
	// if the transaction created a contract, store the creation address in the receipt.
	if msg.To() == nil {
		receipt.ContractAddress = crypto.CreateAddress(evm.TxContext.Origin, tx.Nonce())
	}
	// Set the receipt logs and create a bloom for filtering
	receipt.Logs = statedb.GetLogs(tx.Hash())
	receipt.Bloom = types.CreateBloom(types.Receipts{receipt})
	receipt.BlockHash = statedb.BlockHash()
	receipt.BlockNumber = header.Number
	receipt.TransactionIndex = uint(statedb.TxIndex())

	hitAddressMap := make(map[string]struct{})
	hitFrom := false
	hitTo := false
	hitDirty := false
	hitCallee := false
	hitReadee := false
	fromAddress := util.ConvertAddressToLowerCaseString(msg.From())
	if _, hit := evm.MonitorConfig.MonitoredAddresses[fromAddress]; hit {
		hitAddressMap[fromAddress] = struct{}{}
		hitFrom = true
	}
	if msg.To() != nil {
		toAddress := util.ConvertAddressToLowerCaseString(*msg.To())
		if _, hit := evm.MonitorConfig.MonitoredAddresses[toAddress]; hit {
			hitAddressMap[toAddress] = struct{}{}
			hitTo = true
		}
	}
	lowerCaseMinerAddress := util.ConvertAddressToLowerCaseString(header.Coinbase)
	for dirtyAddress := range entries {
		lowerCaseDirtyAddress := util.ConvertAddressToLowerCaseString(dirtyAddress)
		if evm.MonitorConfig.IncludeMinerTransactions || lowerCaseDirtyAddress != lowerCaseMinerAddress {
			if _, hit := evm.MonitorConfig.MonitoredAddresses[lowerCaseDirtyAddress]; hit {
				hitAddressMap[lowerCaseDirtyAddress] = struct{}{}
				hitDirty = true
			}
		}
	}
	for calleeAddress := range evm.CalleeAddresses {
		if _, hit := evm.MonitorConfig.MonitoredAddresses[calleeAddress]; hit {
			hitAddressMap[calleeAddress] = struct{}{}
			hitCallee = true
		}
	}
	for readeeAddress := range evm.ReadeeAddresses {
		if _, hit := evm.MonitorConfig.MonitoredAddresses[readeeAddress]; hit {
			hitAddressMap[readeeAddress] = struct{}{}
			hitReadee = true
		}
	}

	var hitAddresses []string
	for hitAddress := range hitAddressMap {
		hitAddresses = append(hitAddresses, hitAddress)
	}
	var hitPoints []string
	if hitFrom { hitPoints = append(hitPoints, "from") }
	if hitTo { hitPoints = append(hitPoints, "to") }
	if hitDirty { hitPoints = append(hitPoints, "dirty") }
	if hitCallee { hitPoints = append(hitPoints, "callee") }
	if hitReadee { hitPoints = append(hitPoints, "readee") }

	if len(hitAddresses) > 0 {
	//if len(hitAddresses) > 0 && header.Number.Uint64() >= 1129997 && header.Number.Uint64() <= 1131811 {
		vmErr := ""
		msgTo := ""
		if result.Err != nil {
			vmErr = result.Err.Error()
		}
		if msg.To() != nil {
			msgTo = msg.To().String()
		}
		var targetMonitorResultCsvFile *os.File
		if evm.MonitorConfig.ReplaceMonitoredContractCode {
			targetMonitorResultCsvFile = evm.MonitorConfig.MonitorDirtyResultFile
		} else {
			targetMonitorResultCsvFile = evm.MonitorConfig.MonitorCleanResultFile
		}
		go util.SaveToCsvFileBuffer(targetMonitorResultCsvFile, []string{
			receipt.TxHash.String(),                                   // transactionHash
			receipt.BlockNumber.String(),                              // blockNumber
			strconv.Itoa(int(receipt.TransactionIndex)),               // transactionIndex
			strings.Join(hitAddresses, ","),                      // hitMonitoredAddresses
			strings.Join(hitPoints, ","),                         // hitPoints
			msg.From().String(),                                       // from
			msgTo,                                                     // to
			msg.Value().String(),                                      // value
			strconv.FormatUint(evm.TransactionRealGasUsed, 10),  // gasUsed
			hex.EncodeToString(msg.Data()),                            // input
			hex.EncodeToString(result.ReturnData),                     // return
			vmErr,                                                     // error
			postJournal,                                               // postJournal
			time.Now().Local().String(),                               // time
		}, "TargetResultCsvFile")

		// save gas brief
		gasBriefData := []string {
			receipt.TxHash.String(),                                   // transactionHash
			strconv.FormatUint(evm.TransactionRealGasUsed, 10),  // gasUsed
			vmErr,                                                     // error
			strings.Join(hitAddresses, ","),                      // hitMonitoredAddresses
		}
		if !evm.MonitorConfig.ReplaceMonitoredContractCode {
			go util.SaveToCsvFileBuffer(evm.MonitorConfig.MonitorCleanGasBriefFile, gasBriefData, "CleanGasBriefFile")
			go util.SaveToSnippetCsvFileBuffer(evm.MonitorConfig.CleanGasBriefSnippetFilename,
				evm.MonitorConfig.TransactionsOriginalGasUsedSnippetFileLock, gasBriefData, "CleanGasBriefSnippetFile")
		} else {
			go util.SaveToCsvFileBuffer(evm.MonitorConfig.MonitorDirtyGasBriefFile, gasBriefData, "DirtyGasBriefFile")
		}
	}

	if !evm.MonitorConfig.ReplaceMonitoredContractCode {
		// save CallChainOriginalErrorCallInfo
		for callChainIndexesString, callChainEvmErrorCallInfo := range evm.CallChainOriginalErrorCallInfo {
			originalEvmErrorCallInfo := []string{
				receipt.BlockNumber.String(),                // blockNumber
				strconv.Itoa(int(receipt.TransactionIndex)), // transactionIndex
				callChainIndexesString,                      // callChainIndexesString
				callChainEvmErrorCallInfo.ErrorMessage,      // evmErrorMessage
				callChainEvmErrorCallInfo.Type.IntString(),  // callType
				callChainEvmErrorCallInfo.ToAddressTail,     // toAddressTail
				callChainEvmErrorCallInfo.Value,             // value
			}
			go util.SaveToCsvFileBuffer(evm.MonitorConfig.MonitorCallChainEvmErrorFile, originalEvmErrorCallInfo, "CallChainEvmErrorFile")
			go util.SaveToSnippetCsvFileBuffer(evm.MonitorConfig.CallChainEvmErrorSnippetFilename,
				evm.MonitorConfig.CallChainOriginalErrorCallInfoSnippetFileLock, originalEvmErrorCallInfo, "CallChainEvmErrorSnippetFile")
		}

		// save CallChainOriginalGasSiteInfo
		for callChainIndexesStringWithGas, callChainGasSiteInfo := range evm.CallChainOriginalGasSiteInfo {
			gasSiteInfo := []string{
				receipt.BlockNumber.String(),                                     // blockNumber
				strconv.Itoa(int(receipt.TransactionIndex)),                      // transactionIndex
				callChainIndexesStringWithGas,                                    // callChainIndexesString
				strconv.FormatUint(callChainGasSiteInfo.TransientGas, 10),  // transientGas
				callChainGasSiteInfo.Type.IntString(),                            // callType
				callChainGasSiteInfo.ToAddressTail,                               // toAddressTail
				callChainGasSiteInfo.Value,                                       // value
			}
			go util.SaveToCsvFileBuffer(evm.MonitorConfig.MonitorCallChainGasSiteInfoFile, gasSiteInfo, "CallChainGasSiteInfoFile")
			go util.SaveToSnippetCsvFileBuffer(evm.MonitorConfig.CallChainGasSiteInfoSnippetFilename,
				evm.MonitorConfig.CallChainOriginalGasSiteInfoSnippetFileLock, gasSiteInfo, "CallChainGasSiteInfoSnippetFile")
		}
	}

	// save SStoreCount and SLoadCount if msg.To() is a contract (code length > 0)
	// or this is a contract creation transaction
	toAddress := msg.To()
	if toAddress == nil || evm.StateDB.GetCodeSize(*toAddress) > 0 {
		isContractCreation := "0"
		if toAddress == nil {
			isContractCreation = "1"
		}
		go util.SaveToCsvFileBuffer(evm.MonitorConfig.MonitorOpCodeStatisticsFile, []string{
			receipt.BlockNumber.String(),                              // blockNumber
			strconv.Itoa(int(receipt.TransactionIndex)),               // transactionIndex
			strconv.FormatUint(evm.TransactionRealGasUsed, 10),  // gasUsed
			strconv.FormatUint(evm.SStoreCount, 10),             // sstoreCount
			strconv.FormatUint(evm.SLoadCount, 10),              // sloadCount
			strconv.FormatUint(evm.OpCodeCount, 10),             // opCodeCount
			isContractCreation,                                        // isContractCreation
		}, "OpCodeStatisticsFile")
	}

	return receipt, err
}

// ApplyTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. It returns the receipt
// for the transaction, gas used and an error if the transaction failed,
// indicating the block was invalid.
func ApplyTransaction(config *params.ChainConfig, bc ChainContext, author *common.Address, gp *GasPool, statedb *state.StateDB, header *types.Header, tx *types.Transaction, usedGas *uint64, cfg vm.Config) (*types.Receipt, error) {
	msg, err := tx.AsMessage(types.MakeSigner(config, header.Number))
	if err != nil {
		return nil, err
	}
	// Create a new context to be used in the EVM environment
	blockContext := NewEVMBlockContext(header, bc, author)
	vmenv := vm.NewEVM(blockContext, vm.TxContext{}, statedb, config, cfg)
	return applyTransaction(msg, config, bc, author, gp, statedb, header, tx, usedGas, vmenv)
}
