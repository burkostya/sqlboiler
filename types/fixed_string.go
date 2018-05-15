package types

import (
	"database/sql/driver"
	"errors"
	"strings"
)

// FixedString is an clickhouse's FixedString type
type FixedString string

func (str FixedString) trimZero() FixedString {
	return FixedString(strings.Trim(string(str), string("\x00")))
}

// String output trimmed string.
func (str FixedString) String() string {
	return string(str.trimZero())
}

// Value returns str as a value.
func (str FixedString) Value() (driver.Value, error) {
	return string(str.String()), nil
}

// Scan stores the src in *str.
func (str *FixedString) Scan(src interface{}) error {
	var source string

	switch src.(type) {
	case string:
		source = src.(string)
	default:
		return errors.New("incompatible type for FixedString")
	}

	*str = FixedString(source).trimZero()

	return nil
}
