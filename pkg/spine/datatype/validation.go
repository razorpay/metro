package datatype

import (
	"errors"
	"fmt"
	"regexp"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/gobuffalo/nulls"
)

const (
	// regular expression to validation rzp standard ID
	RegexRZPID = `^[a-zA-Z0-9]{14}$`

	// regular expression to validation unix timestamp
	RegexUnixTimestamp = `^([\d]{10}|0)$`
)

// ValidateNullableInt64 checks NullableInt64 against the rules provided
func ValidateNullableInt64(value nulls.Int64, rules ...validation.RuleFunc) validation.RuleFunc {
	return func(value interface{}) error {
		v := value.(nulls.Int64)
		for _, f := range rules {
			if err := f(v.Int64); err != nil {
				return err
			}
		}

		return nil
	}
}

// IsRZPID checks the given string is a valid 14-char Razorpay ID
func IsRZPID(value interface{}) error {
	return isValidString(value, RegexRZPID)
}

// IsTimestamp will validate if the value is a valid unix timestamp or not
func IsTimestamp(value interface{}) error {
	if value == nil {
		return nil
	}

	return MatchRegex(fmt.Sprintf("%v", value), RegexUnixTimestamp)
}

// MatchRegex checks if given input matches a given regex or not
func MatchRegex(value string, regex string) error {
	if validString, err := regexp.Compile(regex); err != nil {
		return errors.New("invalid regex")
	} else if !validString.MatchString(value) {
		return errors.New("not a valid input")
	}

	return nil
}

// isValidString checks if given input matches a given regex or not
func isValidString(value interface{}, regex string) error {
	// let the nil handled by required validation
	if value == nil {
		return nil
	}

	if str, err := isString(value); err != nil {
		return err
	} else if str == "" {
		return nil
	} else {
		return MatchRegex(str, regex)
	}
}

// isString checks if the given data is valid string or not
func isString(value interface{}) (string, error) {
	if str, ok := value.(string); !ok {
		return "", errors.New("must be a string")
	} else {
		return str, nil
	}
}
