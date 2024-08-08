package item

import (
	"encoding/json"
	"fmt"
)

type Type int

const (
	// TypeUnknown is an unknown type -- you should not use this and instead return an error when you encounter it
	TypeUnknown Type = -1
	// TypeTest is a test type
	TypeTest Type = -42
)

const (
	// TypeSeed is a seed URL
	TypeSeed Type = iota
	// TypeOutlink is an outlink URL
	TypeOutlink
	// TypeAsset is an asset URL
	TypeAsset
)

var typeStrings = [...]string{
	TypeSeed:    "seed",
	TypeOutlink: "outlink",
	TypeAsset:   "asset",
}

func (t Type) String() string {
	if t >= 0 && int(t) < len(typeStrings) {
		return typeStrings[t]
	}
	return fmt.Sprintf("Unknown(%d)", int(t))
}

func (t Type) MarshalJSON() ([]byte, error) {
	return json.Marshal(int(t))
}

func (t *Type) UnmarshalJSON(data []byte) error {
	var value int
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*t = Type(value)
	return nil
}

func TypeFromString(s string) (Type, error) {
	for i, str := range typeStrings {
		if s == str {
			return Type(i), nil
		}
	}
	return Type(-1), fmt.Errorf("unknown type: %s", s)
}

func (t Type) Int() int {
	return int(t)
}
