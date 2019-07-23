package model

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/ethanrowe/botlnek/pkg/util"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSourceKeyHash(t *testing.T) {
	received := Source{
		Keys: map[string]string{
			"a":        "aye",
			"b":        "bi",
			"c":        "si",
			"z":        "zi",
			"aardvark": "huh?",
		},
	}.KeyHash()

	hash := sha256.New()
	hash.Write([]byte(
		fmt.Sprintf(
			"%s\x00",
			strings.Join(
				[]string{
					"a", "aye",
					"aardvark", "huh?",
					"b", "bi",
					"c", "si",
					"z", "zi",
				},
				"\x00",
			),
		),
	))
	expected := fmt.Sprintf("%x", hash.Sum(nil))
	if expected != received {
		t.Errorf(
			"KeyHash mismatch; (received) %q != %q (expected)",
			received,
			expected,
		)
	}
}

func TestDomainJsonMarshalling(t *testing.T) {
	key := "my special domain key"
	attrs := map[string]string{
		"A key":       "A value",
		"key B":       "value B",
		"other-stuff": "yes, other stuff.",
	}

	domain := Domain{
		Key:   DomainKey(key),
		Attrs: util.NewStringKVPairs(attrs),
	}

	serialized, err := json.Marshal(domain)
	if err != nil {
		t.Fatalf("Error marshaling domain: %s", err)
	}

	// Verify the structure of the json by unpacking it;
	// we don't want to do byte-level comparison because the
	// order of k/v pairs in attrs is undefined.
	rawStruct := make(map[string]interface{})
	if err = json.Unmarshal(serialized, &rawStruct); err != nil {
		t.Fatalf("Error umarshaling JSON: %s", err)
	}

	// Exactly two keys; they'll be "Key" and "Attrs"
	if len(rawStruct) != 2 {
		t.Errorf("Expected two keys, got: %d", len(rawStruct))
	}

	got, ok := rawStruct["Key"]
	if !ok {
		t.Errorf("Expected 'Key' not found")
	}

	if got.(string) != key {
		t.Errorf("Key expected %q but received %q", key, got.(string))
	}

	got, ok = rawStruct["Attrs"]
	if !ok {
		t.Errorf("Expected 'Attrs' not found")
	}

	_, ok = got.(map[string]interface{})
	if !ok {
		t.Errorf("Expected 'Attrs' was not a map")
	}
	gotAttrs := make(map[string]string)
	for k, v := range got.(map[string]interface{}) {
		gotAttrs[k] = v.(string)
	}

	if !reflect.DeepEqual(gotAttrs, attrs) {
		t.Errorf("Attrs expected %q but received %q", attrs, gotAttrs)
	}
}

func TestDomainJsonUnmarshaling(t *testing.T) {
	key := "here is my key"
	attrs := map[string]string{
		"023":            "zero two 3",
		"four-five-six":  "4,5,6",
		"seven ate nein": "7?  Ayt?  9?",
	}

	jsonable := struct {
		Key   string
		Attrs map[string]string
	}{
		key,
		attrs,
	}

	data, _ := json.Marshal(jsonable)

	expected := Domain{
		Key:   DomainKey(key),
		Attrs: util.NewStringKVPairs(attrs),
	}

	var got Domain
	err := json.Unmarshal(data, &got)
	if err != nil {
		t.Fatalf("Error unmarshaling data: %s", err)
	}

	if !expected.Equals(got) {
		t.Fatalf("Mismatch; expected %q but received %q", expected, got)
	}
}

type testCounter string

func (c testCounter) Cmp(a, b Counter) int {
	as, bs := string(a.(testCounter)), string(b.(testCounter))
	if as < bs {
		return -1
	} else if as == bs {
		return 0
	}
	return 1
}

func (c testCounter) Less(a, b Counter) bool {
	return string(a.(testCounter)) < string(b.(testCounter))
}

type testJsonSourceReg struct {
	SeqNum      string
	Approximate string
	Keys        map[string]string
	Attrs       map[string]string
}

func exampleSourceReg(seqnum string) (SourceRegistration, testJsonSourceReg) {
	keys, attrs := make(map[string]string), make(map[string]string)
	keys[fmt.Sprintf("%s-key", seqnum)] = fmt.Sprintf("%s-value", seqnum)
	keys[fmt.Sprintf("key-%s", seqnum)] = fmt.Sprintf("value-%s", seqnum)
	attrs[fmt.Sprintf("%s-attr", seqnum)] = fmt.Sprintf("%s-value", seqnum)
	attrs[fmt.Sprintf("attr-%s", seqnum)] = fmt.Sprintf("value-%s", seqnum)
	t := SourceRegistration{
		ClockEntry{testCounter(seqnum), time.Now()},
		Source{keys, attrs},
	}
	return t, testJsonSourceReg{
		SeqNum:      seqnum,
		Approximate: t.Approximate.Format(time.RFC3339Nano),
		Keys:        keys,
		Attrs:       attrs,
	}
}

// Form we expect in our source collections,
// where the registration clock details are found
// via the version index, and the source is a
// nested structure.
type testJsonSourceLogEntry struct {
	VersionIdx int
	Key        string
	Source     struct {
		Keys  map[string]string
		Attrs map[string]string
	}
}

// And the version details we expect to receive.
type testJsonVersion struct {
	SeqNum      string
	Approximate string
}

func (v testJsonVersion) ClockEntry() ClockEntry {
	t, _ := time.Parse(time.RFC3339Nano, v.Approximate)
	return ClockEntry{testCounter(v.SeqNum), t}
}

func exampleSourceLogEntry(prefix string, count int) (SourceLog, testJsonSourceLogEntry, testJsonVersion) {
	seqnum := fmt.Sprintf("%s-%09d", prefix, count)
	keys, attrs := make(map[string]string), make(map[string]string)
	keys[fmt.Sprintf("%s-key", seqnum)] = fmt.Sprintf("%s-value", seqnum)
	keys[fmt.Sprintf("key-%s", seqnum)] = fmt.Sprintf("value-%s", seqnum)
	attrs[fmt.Sprintf("%s-attr", seqnum)] = fmt.Sprintf("%s-value", seqnum)
	attrs[fmt.Sprintf("attr-%s", seqnum)] = fmt.Sprintf("value-%s", seqnum)
	s := Source{keys, attrs}
	t := SourceLog{
		VersionIdx: count,
		Key:        hashKVPairs(util.NewStringKVPairs(keys)),
		Source:     s,
	}
	v := testJsonVersion{
		SeqNum:      seqnum,
		Approximate: time.Now().Format(time.RFC3339Nano),
	}
	l := testJsonSourceLogEntry{
		VersionIdx: count,
		Key:        t.Key,
		Source: struct {
			Keys  map[string]string
			Attrs map[string]string
		}{
			Keys:  keys,
			Attrs: attrs,
		},
	}
	return t, l, v
}

type testSourceTriple struct {
	Entry   SourceLog
	Mock    testJsonSourceLogEntry
	Version testJsonVersion
}

func exampleSourceRegs(prefix string) chan testSourceTriple {
	c := make(chan testSourceTriple)
	go func() {
		i := 0
		for {
			log, mock, ver := exampleSourceLogEntry(prefix, i)
			c <- testSourceTriple{log, mock, ver}
			i++
		}
	}()
	return c
}

func TestAggregateJsonMarshaling(t *testing.T) {
	// A nasty structure representing our expected JSON
	// result, from which we can build the model object.
	// Note that Sources is a map of string source tokens
	// to a map of string keys (source keys) to lists of sources
	// (multiple sources could have the same key in this example,
	// if for instance an admin wanted to forcibly register the
	// same event due to a rebuilt input)
	sources := exampleSourceRegs("aggr-marshal")
	src_foo_a := <-sources
	src_foo_b := <-sources
	src_bar_a := <-sources
	src_bar_b := <-sources
	src_baz_a := <-sources
	src_baz_b := <-sources

	jsonable := struct {
		Key     string
		Attrs   map[string]string
		Log     []testJsonVersion
		Sources map[string][]testJsonSourceLogEntry
	}{
		"a-aggregate-key",
		map[string]string{
			"foo-aggr-attr": "foo-aggr-attr-val",
			"bar-aggr-attr": "bar-aggr-attr-val",
		},
		[]testJsonVersion{
			src_foo_a.Version,
			src_foo_b.Version,
			src_bar_a.Version,
			src_bar_b.Version,
			src_baz_a.Version,
			src_baz_b.Version,
		},
		map[string][]testJsonSourceLogEntry{
			"foo-token": []testJsonSourceLogEntry{
				src_foo_a.Mock,
				src_foo_b.Mock,
			},
			"bar-token": []testJsonSourceLogEntry{
				src_bar_a.Mock,
				src_bar_b.Mock,
			},
			"baz-token": []testJsonSourceLogEntry{
				src_baz_a.Mock,
				src_baz_b.Mock,
			},
		},
	}

	aggr := Aggregate{
		Key:   AggregateKey(jsonable.Key),
		Attrs: jsonable.Attrs,
		Log: []ClockEntry{
			src_foo_a.Version.ClockEntry(),
			src_foo_b.Version.ClockEntry(),
			src_bar_a.Version.ClockEntry(),
			src_bar_b.Version.ClockEntry(),
			src_baz_a.Version.ClockEntry(),
			src_baz_b.Version.ClockEntry(),
		},
		Sources: SourceLogMap{
			"foo-token": []SourceLog{src_foo_a.Entry, src_foo_b.Entry},
			"bar-token": []SourceLog{src_bar_a.Entry, src_bar_b.Entry},
			"baz-token": []SourceLog{src_baz_a.Entry, src_baz_b.Entry},
		},
	}

	aggrData, err := json.Marshal(aggr)
	if err != nil {
		t.Fatalf("Failure marshalling aggregate: %s", err)
	}

	specData, err := json.Marshal(jsonable)
	if err != nil {
		t.Fatalf("Failure marshalling jsonable representation: %s", err)
	}

	// We don't want byte-level comparison; we want to see that the unmarshalled
	// representations of the data are equivalent.
	var fromAggrData, fromSpecData interface{}
	err = json.Unmarshal(aggrData, &fromAggrData)
	if err != nil {
		t.Fatalf("Failure unmarshalling aggregate-derived JSON: %s", err)
	}

	err = json.Unmarshal(specData, &fromSpecData)
	if err != nil {
		t.Fatalf("Failure unmarshalling spec-derived JSON: %s", err)
	}

	if !reflect.DeepEqual(fromAggrData, fromSpecData) {
		t.Errorf("Aggregate-based JSON structure doesn't match expectation (\n\tgot: %q\n\texpected: %q)", fromAggrData, fromSpecData)
	}
}
