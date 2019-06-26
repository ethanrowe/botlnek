package model

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/ethanrowe/botlnek/pkg/util"
	"time"
)

func hashKVPairs(pairs util.StringKVPairs) string {
	hash := sha256.New()
	pairs.WriteTo(hash)
	return fmt.Sprintf("%x", hash.Sum(nil))
}

// The specific counter implementation details come
// from the persistence layer, so we simply want an
// interface that makes sense.
type Counter interface {
	// -1 if a < b, 0 of a == b, 1 if a > b
	Cmp(a, b Counter) int
	// a < b; for the sort package
	Less(a, b Counter) bool
	json.Marshaler
	json.Unmarshaler
}

type ClockEntry struct {
	Count       Counter
	Approximate time.Time
}

type Source struct {
	Keys     map[string]string
	Location string
}

func (s Source) KeyHash() string {
	return hashKVPairs(util.NewStringKVPairs(s.Keys))
}

type SourceRegistration struct {
	ClockEntry
	Source
}

type SourceRegistrations map[string]SourceRegistration

type SourceMap map[string]SourceRegistrations

type Partition struct {
	Key     string
	Attrs   util.StringKVPairs
	Sources SourceMap
}

type Domain struct {
	DomainKey string
	Attrs     util.StringKVPairs
	// Partitions map[string]Partition
}

func (d Domain) Equals(other Domain) bool {
	if d.DomainKey != other.DomainKey {
		return false
	}
	return hashKVPairs(d.Attrs) == hashKVPairs(other.Attrs)
}
