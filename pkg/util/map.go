package util

import (
	"io"
	"sort"
)

var nullChar = []byte("\000")

type StringKVPair struct {
	Key   string
	Value string
}

type StringKVPairs []StringKVPair

func (p StringKVPairs) Len() int           { return len(p) }
func (p StringKVPairs) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p StringKVPairs) Less(i, j int) bool { return p[i].Key < p[j].Key }
func (p StringKVPairs) WriteTo(w io.Writer) {
	for _, pair := range p {
		w.Write([]byte(pair.Key))
		w.Write(nullChar)
		w.Write([]byte(pair.Value))
		w.Write(nullChar)
	}
}

func NewStringKVPairs(m map[string]string) StringKVPairs {
	pairs := make(StringKVPairs, len(m))
	i := 0
	for k, v := range m {
		pairs[i].Key, pairs[i].Value = k, v
		i++
	}
	sort.Sort(pairs)
	return pairs
}
