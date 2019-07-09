package inmemory

import (
	"encoding/json"
	"fmt"
	"github.com/ethanrowe/botlnek/pkg/model"
	"strconv"
)

const (
	COUNTER_FORMAT_STRING = "%016x"
)

type InMemoryCounter int64

func (c InMemoryCounter) Cmp(a, b model.Counter) int {
	ai, bi := int64(a.(InMemoryCounter)), int64(b.(InMemoryCounter))
	if ai < bi {
		return -1
	} else if ai > bi {
		return 1
	}
	return 0
}

func (c InMemoryCounter) Less(a, b model.Counter) bool {
	return int64(a.(InMemoryCounter)) < int64(b.(InMemoryCounter))
}

func (c InMemoryCounter) MarshalJson() ([]byte, error) {
	return json.Marshal(
		fmt.Sprintf(COUNTER_FORMAT_STRING, int64(c)),
	)
}

func (c *InMemoryCounter) UnmarshalJson(data []byte) error {
	var s string
	err := json.Unmarshal(data, &s)
	if err != nil {
		return err
	}
	x, err := strconv.ParseInt(s, 16, 64)
	if err != nil {
		return err
	}
	*c = InMemoryCounter(x)
	return nil
}
