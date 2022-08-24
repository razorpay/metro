package utils

import (
	"encoding/base64"
)

// Encode given a string, returns a base64 encoded version of it
func Encode(input string) string {
	return base64.StdEncoding.EncodeToString([]byte(input))
}

// Decode given a base64 encoded string, returns a decoded version
func Decode(input string) string {
	decoded, _ := base64.StdEncoding.DecodeString(input)
	return string(decoded)
}

// DecodeSlice given a slice of base64 encoded strings, returns a slice of decoded strings
func DecodeSlice(input []string) []string {
	decodedSlice := make([]string, 0)
	for _, s := range input {
		decodedSlice = append(decodedSlice, Decode(s))
	}
	return decodedSlice
}
