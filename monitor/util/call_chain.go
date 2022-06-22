package util

import (
	"strconv"
	"strings"
)

func EncodeCallChainIndexes(callChainIndexes []uint64) string {
	if len(callChainIndexes) == 0 {
		return ""
	}
	var builder strings.Builder
	first := true
	for _, index := range callChainIndexes {
		if first {
			first = false
		} else {
			builder.WriteString("_")
		}
		builder.WriteString(strconv.FormatUint(index, 10))
	}
	return builder.String()
}

func EncodeCallChainIndexesAtRunSite(callChainIndexes []uint64) string {
	if len(callChainIndexes) == 0 {
		return ""
	}
	var builder strings.Builder
	first := true
	for _, index := range callChainIndexes {
		if first {
			first = false
		} else {
			builder.WriteString("_")
		}
		builder.WriteString(strconv.FormatUint(index, 10))
	}
	builder.WriteString("_r")
	return builder.String()
}

func DecodeCallChainIndexes(callChainIndexesString string) []uint64 {
	if len(callChainIndexesString) == 0 {
		return []uint64{}
	}
	indexStrings := strings.Split(callChainIndexesString, "_")
	var indexes []uint64
	for _, indexString := range indexStrings {
		index, err := strconv.ParseUint(indexString, 10, 64)
		PanicOnError(err)
		indexes = append(indexes, index)
	}
	return indexes
}

