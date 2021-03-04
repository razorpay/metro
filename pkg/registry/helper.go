package registry

// GetKeys returns slice of keys from slice of pairs
func GetKeys(pairs []Pair) []string {
	keys := []string{}

	for _, pair := range pairs {
		keys = append(keys, pair.Key)
	}

	return keys
}
