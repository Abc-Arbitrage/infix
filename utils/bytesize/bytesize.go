package bytesize

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strconv"
)

const (
	_ = 1 << (10 * iota)
	KB
	MB
	GB
	TB
	PB
)

type ByteSize uint64

func (b ByteSize) UInt64() uint64 {
	return uint64(b)
}

func (b ByteSize) AsInt() int {
	return int(b)
}

func (b ByteSize) HumanString() string {
	switch val := uint64(b); {
	case val >= PB:
		return fmt.Sprintf("%dPB", val/PB)
	case val >= TB:
		return fmt.Sprintf("%dTB", val/TB)
	case val >= GB:
		return fmt.Sprintf("%dGB", val/GB)
	case val >= MB:
		return fmt.Sprintf("%dMB", val/MB)
	case val >= KB:
		return fmt.Sprintf("%dKB", val/KB)
	default:
		return fmt.Sprintf("%d", val)
	}
}

func (b ByteSize) MarshalText() ([]byte, error) {
	if b == 0 {
		return []byte("0"), nil
	}
	var abs = int64(b)
	if abs < 0 {
		abs = -abs
	}
	switch {
	case abs%PB == 0:
		val := b.UInt64() / PB
		return []byte(fmt.Sprintf("%dpb", val)), nil
	case abs%TB == 0:
		val := b.UInt64() / TB
		return []byte(fmt.Sprintf("%dtb", val)), nil
	case abs%GB == 0:
		val := b.UInt64() / GB
		return []byte(fmt.Sprintf("%dgb", val)), nil
	case abs%MB == 0:
		val := b.UInt64() / MB
		return []byte(fmt.Sprintf("%dmb", val)), nil
	case abs%KB == 0:
		val := b.UInt64() / KB
		return []byte(fmt.Sprintf("%dkb", val)), nil
	default:
		return []byte(fmt.Sprintf("%d", b.UInt64())), nil
	}
}

func (p *ByteSize) UnmarshalText(text []byte) error {
	n, err := Parse(string(text))
	if err != nil {
		return err
	}
	*p = ByteSize(n)
	return nil
}

var (
	fullRegexp = regexp.MustCompile(`^\s*(\-?[\d\.]+)\s*([kmgtp]?b|[bkmgtp]|)\s*$`)
	digitsOnly = regexp.MustCompile(`^\-?\d+$`)
)

var (
	ErrBadByteSize     = errors.New("invalid bytesize")
	ErrBadByteSizeUnit = errors.New("invalid bytesize unit")
)

func Parse(s string) (uint64, error) {
	if !fullRegexp.MatchString(s) {
		return 0, ErrBadByteSize
	}

	subs := fullRegexp.FindStringSubmatch(s)
	if len(subs) != 3 {
		return 0, ErrBadByteSize
	}

	text := subs[1]
	unit := subs[2]

	size := uint64(1)
	switch unit {
	case "b", "":
	case "k", "kb":
		size = KB
	case "m", "mb":
		size = MB
	case "g", "gb":
		size = GB
	case "t", "tb":
		size = TB
	case "p", "pb":
		size = PB
	default:
		return 0, ErrBadByteSizeUnit
	}

	if digitsOnly.MatchString(text) {
		n, err := strconv.ParseUint(text, 10, 64)
		if err != nil {
			return 0, ErrBadByteSize
		}
		size *= n
	} else {
		n, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return 0, ErrBadByteSize
		}
		size = uint64(float64(size) * n)
	}
	return size, nil
}

func MustParse(s string) uint64 {
	v, err := Parse(s)
	if err != nil {
		log.Panicf("parse bytesize failed: %s", err)
	}
	return v
}
