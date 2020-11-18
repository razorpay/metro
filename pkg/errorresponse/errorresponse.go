package errorresponse

import (
	"encoding/json"
)

// Error response struct.
type Error struct {
	Error         Public    `json:"error"`
	InternalError *Internal `json:"internal_error,omitempty"`
}

// Public ...
// Note: Attribute values when JSON-marshalled should be set to null when ]
// unset. omitempty tags are therefore not used.
type Public struct {
	Code        string            `json:"code"`
	Description string            `json:"description"`
	Step        *string           `json:"step"`
	Reason      *string           `json:"reason"`
	Field       *string           `json:"field"`
	Source      *string           `json:"source"`
	Metadata    map[string]string `json:"metadata"`
}

// Internal ...
type Internal struct {
	Code        string            `json:"code"`
	Description string            `json:"description,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

func (p Public) MarshalJSON() ([]byte, error) {
	// We're aliasing the Public struct to avoid a recursive
	// call of MarshalJSON() on p
	type publicAlias Public
	var a = publicAlias(p)

	// We're initializing Metadata if nil. This is done to set the
	// empty JSON-marshalled value to `{}` instead of null.
	if a.Metadata == nil {
		a.Metadata = map[string]string{}
	}

	return json.Marshal(a)
}

// String returns a pointer to the string value passed in arguments. Shorthand
// to assign the *string attributes in the Public or Internal struct.
func String(value string) *string {
	return &value
}
