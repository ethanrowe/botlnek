package inmemory

import (
	"fmt"
	"github.com/ethanrowe/botlnek/pkg/model"
	"github.com/ethanrowe/botlnek/pkg/util"
	"testing"
)

func makeTestDomain(i int, keyvals ...string) model.Domain {
	attrs := make(map[string]string)
	maxlen := len(keyvals) / 2
	for c := 0; c < maxlen; c++ {
		k, v := keyvals[c*2], keyvals[c*2+1]
		attrs[k] = v
	}
	return model.Domain{
		DomainKey: fmt.Sprintf("test-domain-%03d", i),
		Attrs:     util.NewStringKVPairs(attrs),
	}
}

func TestGetDomainEmpty(t *testing.T) {
	s := NewInMemoryStore()
	defer s.Stop()
	d, e := s.GetDomain("some-domain")
	if d != nil && e != nil {
		t.Error("GetDomain on empty store should result in nil")
	}
}

func TestAppendNewDomainSuccess(t *testing.T) {
	s := NewInMemoryStore()
	defer s.Stop()
	d1 := makeTestDomain(0, "a", "Aye", "b", "bI", "c", "see?")
	r, e := s.AppendNewDomain(d1)
	if e != nil {
		t.Fatal("Should not return an error")
	}
	if !r.Equals(d1) {
		t.Fatalf("Did not return an equal copy of the new domain (got %s; expected %s", r, d1)
	}

	d2 := makeTestDomain(1, "foo", "FOO", "bar", "BAR")
	r, e = s.AppendNewDomain(d2)
	if e != nil {
		t.Fatal("Should not return an error")
	}
	if !r.Equals(d2) {
		t.Fatalf("did not return an equal copy of the new domain (got %s; expected %s", r, d2)
	}

	// redundant append should give nil result, but still no error
	r, e = s.AppendNewDomain(d2)
	if e != nil || r != nil {
		t.Errorf("Redundant append should give nil, non-error result (result: %s; error: %s", r, e)
	}

	r, e = s.GetDomain(d1.DomainKey)
	if e != nil {
		t.Fatal("Should not return an error")
	}
	if !r.Equals(d1) {
		t.Fatalf("did not return an equal copy of the retrieved domain (got %s; expected %s", r, d1)
	}

	r, e = s.GetDomain(d2.DomainKey)
	if e != nil {
		t.Fatal("Should not return an error")
	}
	if !r.Equals(d2) {
		t.Fatalf("did not return an equal copy of the retrieved domain (got %s; expected %s", r, d2)
	}
}
