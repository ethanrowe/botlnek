package model

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/ethanrowe/botlnek/pkg/util"
	"reflect"
	"strings"
	"testing"
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
		Location: "irrelevant",
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
