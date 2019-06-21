package util

import (
	"fmt"
	"testing"
)

func TestStringKVPairsSort(t *testing.T) {
	m := make(map[string]string)

	keys := []string{"bar", "baz", "foo"}
	vals := []string{"Bar!", "Baz...", "Foo?"}
	for i, key := range keys {
		m[key] = vals[i]
	}
	pairs := NewStringKVPairs(m)

	gotKeys, gotVals := make([]string, len(pairs)), make([]string, len(pairs))

	for i, pair := range pairs {
		gotKeys[i] = pair.Key
		gotVals[i] = pair.Value
	}

	if l := len(pairs); l != len(keys) {
		t.Fatalf("Expected length %d, received %d", len(keys), l)
	}

	for i, key := range keys {
		if k := gotKeys[i]; k != key {
			t.Errorf("Expected key %d: %s; received %s", i, key, k)
		}
		if v := gotVals[i]; v != vals[i] {
			t.Errorf("Expected val %d: %s; received %s", i, vals[i], v)
		}
	}
}

type writeableBuffer struct {
	Buffer []byte
}

func (buff *writeableBuffer) Write(p []byte) (n int, e error) {
	buff.Buffer = append(buff.Buffer, p...)
	n = len(p)
	return
}

func TestStringKVPairsWriteTo(t *testing.T) {
	writeable := &writeableBuffer{Buffer: make([]byte, 0)}

	pairs := NewStringKVPairs(map[string]string{
		"this key":     "this key's value",
		"that key":     "that key's value",
		"not this key": "not this",
		"nor that key": "nor this",
	})

	pairs.WriteTo(writeable)

	// We expect the binary representation to be in sorted-key
	// order: k0, v0, k1, v1, ... kN, vN
	// We expect each entry to be null-terminated.
	expected := fmt.Sprintf(
		"%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00%s\x00",
		"nor that key", "nor this",
		"not this key", "not this",
		"that key", "that key's value",
		"this key", "this key's value",
	)
	received := string(writeable.Buffer)

	if received != expected {
		t.Errorf("WriteTo mismatch; (received) %q != %q (expected)", received, expected)
	}
}
