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
}

type ClockEntry struct {
	SeqNum      Counter
	Approximate time.Time
}

type Source struct {
	Keys  map[string]string
	Attrs map[string]string
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

type SourceLog struct {
	VersionIdx int
	Key        string
	Source     Source
}

type SourceLogMap map[string][]SourceLog

type Aggregate struct {
	Key     AggregateKey
	Attrs   map[string]string
	Log     []ClockEntry
	Sources SourceLogMap
}

func (p Aggregate) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			Key     AggregateKey
			Attrs   map[string]string
			Log     []ClockEntry
			Sources SourceLogMap
		}{
			p.Key,
			p.Attrs,
			p.Log,
			p.Sources,
		},
	)
}

type Domain struct {
	Key   DomainKey
	Attrs util.StringKVPairs
	// Aggregates map[string]Aggregate
}

func (d Domain) Equals(other Domain) bool {
	if d.Key != other.Key {
		return false
	}
	return hashKVPairs(d.Attrs) == hashKVPairs(other.Attrs)
}

// Helper type for json conversion
type domainJson struct {
	Key   DomainKey
	Attrs map[string]string
}

func (d Domain) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		domainJson{
			Key:   d.Key,
			Attrs: d.Attrs.ToMap(),
		},
	)
}

func (d *Domain) UnmarshalJSON(data []byte) error {
	var intermediary domainJson
	err := json.Unmarshal(data, &intermediary)
	if err == nil {
		d.Key = intermediary.Key
		d.Attrs = util.NewStringKVPairs(intermediary.Attrs)
	}
	return err
}
