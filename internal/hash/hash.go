package hash

import (
	"hash/fnv"
)

// ComputeHash resolves a checksum given a byte array
func ComputeHash(arr []byte) int {
	h := fnv.New32a()
	h.Write(arr)
	return int(h.Sum32())
}
