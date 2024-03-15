package db

import (
	"bytes"
	"os"
	"strings"
)

func cp(bz []byte) (ret []byte) {
	ret = make([]byte, len(bz))
	copy(ret, bz)
	return ret
}

// Returns a slice of the same length (big endian)
// except incremented by one.
// Returns nil on overflow (e.g. if bz bytes are all 0xFF)
// CONTRACT: len(bz) > 0
func cpIncr(bz []byte) (ret []byte) {
	if len(bz) == 0 {
		panic("cpIncr expects non-zero bz length")
	}
	ret = cp(bz)
	for i := len(bz) - 1; i >= 0; i-- {
		if ret[i] < byte(0xFF) {
			ret[i]++
			return
		}
		ret[i] = byte(0x00)
		if i == 0 {
			// Overflow
			return nil
		}
	}
	return nil
}

// See DB interface documentation for more information.
func IsKeyInDomain(key, start, end []byte) bool {
	if bytes.Compare(key, start) < 0 {
		return false
	}
	if end != nil && bytes.Compare(end, key) <= 0 {
		return false
	}
	return true
}

func FileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}

func parseOptParams(params string) map[string]string {
	if len(params) == 0 {
		return nil
	}

	opts := make(map[string]string)
	for _, s := range strings.Split(params, ",") {
		opt := strings.Split(s, "=")
		if len(opt) != 2 {
			panic("Invalid options parameter, like this 'block_size=4kb,statistics=true")
		}
		opts[strings.TrimSpace(opt[0])] = strings.TrimSpace(opt[1])
	}
	return opts
}
