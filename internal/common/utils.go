package common

import "encoding/base64"

// Encode given a string, returns a base64 encoded version of it
func Encode(input string) string {
	return base64.StdEncoding.EncodeToString([]byte(input))
}

// Decode given a base64 encoded string, returns a decoded version
func Decode(input string) string {
	decoded, _ := base64.StdEncoding.DecodeString(input)
	return string(decoded)
}
