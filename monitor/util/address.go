package util

import (
	"fmt"
	"github.com/ethereum/go-ethereum/common"
)

func ConvertAddressToLowerCaseString(address common.Address) string {
	return fmt.Sprintf("0x%x", address)
}

func IsAddressExistInMap(address common.Address, addressMap map[string]string) bool {
	lowerCaseDirtyAddress := ConvertAddressToLowerCaseString(address)
	_, hit := addressMap[lowerCaseDirtyAddress]
	return hit
}
