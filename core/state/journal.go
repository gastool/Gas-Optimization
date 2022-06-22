// Copyright 2016 The go-ethereum Authors
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

package state

import (
	"encoding/json"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// journalEntry is a modification entry in the state change journal that can be
// reverted on demand.
type journalEntry interface {
	// revert undoes the changes introduced by this journal entry.
	revert(*StateDB)

	// dirtied returns the Ethereum address modified by this journal entry.
	dirtied() *common.Address
}

// journal contains the list of state modifications applied since the last state
// commit. These are tracked to be able to be reverted in case of an execution
// exception or revertal request.
type journal struct {
	entries []journalEntry         // Current changes tracked by the journal
	dirties map[common.Address]int // Dirty accounts and the number of changes
}

// newJournal create a new initialized journal.
func newJournal() *journal {
	return &journal{
		dirties: make(map[common.Address]int),
	}
}

// append inserts a new modification entry to the end of the change journal.
func (j *journal) append(entry journalEntry) {
	j.entries = append(j.entries, entry)
	if addr := entry.dirtied(); addr != nil {
		j.dirties[*addr]++
	}
}

// revert undoes a batch of journalled modifications along with any reverted
// dirty handling too.
func (j *journal) revert(statedb *StateDB, snapshot int) {
	for i := len(j.entries) - 1; i >= snapshot; i-- {
		// Undo the changes made by the operation
		j.entries[i].revert(statedb)

		// Drop any dirty tracking induced by the change
		if addr := j.entries[i].dirtied(); addr != nil {
			if j.dirties[*addr]--; j.dirties[*addr] == 0 {
				delete(j.dirties, *addr)
			}
		}
	}
	j.entries = j.entries[:snapshot]
}

// dirty explicitly sets an address to dirty, even if the change entries would
// otherwise suggest it as clean. This method is an ugly hack to handle the RIPEMD
// precompile consensus exception.
func (j *journal) dirty(addr common.Address) {
	j.dirties[addr]++
}

// length returns the current number of entries in the journal.
func (j *journal) length() int {
	return len(j.entries)
}

func (j *journal) dump() string {
	result, err := json.Marshal(struct {
		Entries []journalEntry `json:"entries"`
		Dirties map[common.Address]int `json:"dirties"`
	}{
		Entries: j.entries,
		Dirties: j.dirties,
	})
	if err != nil {
		panic("Cannot dump journal")
	}
	return string(result)
}

type (
	// Changes to the account trie.
	createObjectChange struct {
		account *common.Address
	}
	resetObjectChange struct {
		prev         *stateObject
		prevdestruct bool
		after		 *stateObject
	}
	suicideChange struct {
		account     *common.Address
		prev        bool // whether account had already suicided
		prevbalance *big.Int
	}

	// Changes to individual accounts.
	balanceChange struct {
		account *common.Address
		prev    *big.Int
		after	*big.Int
	}
	nonceChange struct {
		account *common.Address
		prev    uint64
		after   uint64
	}
	storageChange struct {
		account       *common.Address
		key, prevalue common.Hash
		after         common.Hash
	}
	codeChange struct {
		account            *common.Address
		prevcode, prevhash []byte
		afterCode		   []byte
		afterHash		   []byte
	}

	// Changes to other state values.
	refundChange struct {
		prev uint64
		after uint64
	}
	addLogChange struct {
		txhash common.Hash
	}
	addPreimageChange struct {
		hash common.Hash
	}
	touchChange struct {
		account *common.Address
	}
	// Changes to the access list
	accessListAddAccountChange struct {
		address *common.Address
	}
	accessListAddSlotChange struct {
		address *common.Address
		slot    *common.Hash
	}
)

func (ch createObjectChange) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		Account *common.Address `json:"account"`
	}{
		Type: "createObjectChange",
		Account: ch.account,
	})
}

func (ch resetObjectChange) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type 	     string `json:"type"`
		Prev         *stateObject `json:"prev"`
		Prevdestruct bool `json:"prevdestruct"`
		After		 *stateObject `json:"after"`
	}{
		Type: "resetObjectChange",
		Prev: ch.prev,
		Prevdestruct: ch.prevdestruct,
		After: ch.after,
	})
}

func (ch suicideChange) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type        string `json:"type"`
		Account     *common.Address `json:"account"`
		Prev        bool `json:"prev"`
		Prevbalance *big.Int `json:"prevbalance"`
	}{
		Type: "suicideChange",
		Account: ch.account,
		Prev: ch.prev,
		Prevbalance: ch.prevbalance,
	})
}

func (ch balanceChange) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type    string `json:"type"`
		Account *common.Address `json:"account"`
		Prev    *big.Int `json:"prev"`
		After	*big.Int `json:"after"`
	}{
		Type: "balanceChange",
		Account: ch.account,
		Prev: ch.prev,
		After: ch.after,
	})
}

func (ch nonceChange) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type    string `json:"type"`
		Account *common.Address `json:"account"`
		Prev    uint64 `json:"prev"`
		After   uint64 `json:"after"`
	}{
		Type: "nonceChange",
		Account: ch.account,
		Prev: ch.prev,
		After: ch.after,
	})
}

func (ch storageChange) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type          string `json:"type"`
		Account       *common.Address `json:"account"`
		Key           common.Hash `json:"key"`
		Prevalue	  common.Hash `json:"prevalue"`
		After         common.Hash `json:"after"`
	}{
		Type: "storageChange",
		Account: ch.account,
		Key: ch.key,
		Prevalue: ch.prevalue,
		After: ch.after,
	})
}

func (ch codeChange) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type               string `json:"type"`
		Account            *common.Address `json:"account"`
		Prevcode           []byte `json:"prevcode"`
		Prevhash           []byte `json:"prevhash"`
		AfterCode		   []byte `json:"afterCode"`
		AfterHash		   []byte `json:"afterHash"`
	}{
		Type: "codeChange",
		Account: ch.account,
		Prevcode: ch.prevcode,
		Prevhash: ch.prevhash,
		AfterCode: ch.afterCode,
		AfterHash: ch.afterHash,
	})
}

func (ch refundChange) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		Prev uint64 `json:"prev"`
		After uint64 `json:"after"`
	}{
		Type: "refundChange",
		Prev: ch.prev,
		After: ch.after,
	})
}

func (ch addLogChange) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		Txhash common.Hash `json:"txhash"`
	}{
		Type: "addLogChange",
		Txhash: ch.txhash,
	})
}

func (ch addPreimageChange) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		Hash common.Hash `json:"hash"`
	}{
		Type: "addPreimageChange",
		Hash: ch.hash,
	})
}

func (ch touchChange) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		Account *common.Address `json:"account"`
	}{
		Type: "touchChange",
		Account: ch.account,
	})
}

func (ch accessListAddAccountChange) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		Address *common.Address `json:"address"`
	}{
		Type: "accessListAddAccountChange",
		Address: ch.address,
	})
}

func (ch accessListAddSlotChange) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		Address *common.Address `json:"address"`
		Slot    *common.Hash `json:"slot"`
	}{
		Type: "accessListAddSlotChange",
		Address: ch.address,
		Slot: ch.slot,
	})
}


func (ch createObjectChange) revert(s *StateDB) {
	delete(s.stateObjects, *ch.account)
	delete(s.stateObjectsDirty, *ch.account)
}

func (ch createObjectChange) dirtied() *common.Address {
	return ch.account
}

func (ch resetObjectChange) revert(s *StateDB) {
	s.setStateObject(ch.prev)
	if !ch.prevdestruct && s.snap != nil {
		delete(s.snapDestructs, ch.prev.addrHash)
	}
}

func (ch resetObjectChange) dirtied() *common.Address {
	return nil
}

func (ch suicideChange) revert(s *StateDB) {
	obj := s.getStateObject(*ch.account)
	if obj != nil {
		obj.suicided = ch.prev
		obj.setBalance(ch.prevbalance)
	}
}

func (ch suicideChange) dirtied() *common.Address {
	return ch.account
}

var ripemd = common.HexToAddress("0000000000000000000000000000000000000003")

func (ch touchChange) revert(s *StateDB) {
}

func (ch touchChange) dirtied() *common.Address {
	return ch.account
}

func (ch balanceChange) revert(s *StateDB) {
	s.getStateObject(*ch.account).setBalance(ch.prev)
}

func (ch balanceChange) dirtied() *common.Address {
	return ch.account
}

func (ch nonceChange) revert(s *StateDB) {
	s.getStateObject(*ch.account).setNonce(ch.prev)
}

func (ch nonceChange) dirtied() *common.Address {
	return ch.account
}

func (ch codeChange) revert(s *StateDB) {
	s.getStateObject(*ch.account).setCode(common.BytesToHash(ch.prevhash), ch.prevcode)
}

func (ch codeChange) dirtied() *common.Address {
	return ch.account
}

func (ch storageChange) revert(s *StateDB) {
	s.getStateObject(*ch.account).setState(ch.key, ch.prevalue)
}

func (ch storageChange) dirtied() *common.Address {
	return ch.account
}

func (ch refundChange) revert(s *StateDB) {
	s.refund = ch.prev
}

func (ch refundChange) dirtied() *common.Address {
	return nil
}

func (ch addLogChange) revert(s *StateDB) {
	logs := s.logs[ch.txhash]
	if len(logs) == 1 {
		delete(s.logs, ch.txhash)
	} else {
		s.logs[ch.txhash] = logs[:len(logs)-1]
	}
	s.logSize--
}

func (ch addLogChange) dirtied() *common.Address {
	return nil
}

func (ch addPreimageChange) revert(s *StateDB) {
	delete(s.preimages, ch.hash)
}

func (ch addPreimageChange) dirtied() *common.Address {
	return nil
}

func (ch accessListAddAccountChange) revert(s *StateDB) {
	/*
		One important invariant here, is that whenever a (addr, slot) is added, if the
		addr is not already present, the add causes two journal entries:
		- one for the address,
		- one for the (address,slot)
		Therefore, when unrolling the change, we can always blindly delete the
		(addr) at this point, since no storage adds can remain when come upon
		a single (addr) change.
	*/
	s.accessList.DeleteAddress(*ch.address)
}

func (ch accessListAddAccountChange) dirtied() *common.Address {
	return nil
}

func (ch accessListAddSlotChange) revert(s *StateDB) {
	s.accessList.DeleteSlot(*ch.address, *ch.slot)
}

func (ch accessListAddSlotChange) dirtied() *common.Address {
	return nil
}
