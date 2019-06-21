package model

import (
	"crypto/sha256"
	"fmt"
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
