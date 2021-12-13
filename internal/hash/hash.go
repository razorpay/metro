package hash

import (
	"hash/fnv"
)

func ComputeHash(arr []byte) int {
	h := fnv.New32a()
	h.Write(arr)
	return int(h.Sum32())
}
