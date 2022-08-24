package utils

import (
	"encoding/base64"
	"reflect"
	"unsafe"
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

//EqualOnly compares fields defined in the fields array
func EqualOnly(sub1 interface{}, sub2 interface{}, fields []string) bool {
	val1 := reflect.ValueOf(sub1).Elem()
	val2 := reflect.ValueOf(sub2).Elem()
	for i := 0; i < val1.NumField(); i++ {
		typeField := val1.Type().Field(i)
		if !contains(typeField.Name, fields) {
			continue
		}
		value1 := val1.Field(i)
		value2 := val2.Field(i)
		value1 = reflect.NewAt(value1.Type(), unsafe.Pointer(value1.UnsafeAddr())).Elem()
		value2 = reflect.NewAt(value2.Type(), unsafe.Pointer(value2.UnsafeAddr())).Elem()
		if value1.Interface() != value2.Interface() {
			return false
		}
	}
	return true
}

func contains(name string, fields []string) bool {
	for _, field := range fields {
		if field == name {
			return true
		}
	}
	return false
}
