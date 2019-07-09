package inmemory

import (
	"fmt"
	"github.com/ethanrowe/botlnek/pkg/model"
	"github.com/ethanrowe/botlnek/pkg/util"
	"reflect"
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
		Key:   model.DomainKey(fmt.Sprintf("test-domain-%03d", i)),
		Attrs: util.NewStringKVPairs(attrs),
	}
}

func generateTestPartitionKeys(prefix string) chan model.PartitionKey {
	count := int64(0)
	chn := make(chan model.PartitionKey)
	go func() {
		for {
			key := fmt.Sprintf("partition-%s-%d", prefix, count)
			count++
			chn <- model.PartitionKey(key)
		}
	}()
	return chn
}

func generateTestTokens(prefix string) chan string {
	count := int64(0)
	chn := make(chan string)
	go func() {
		for {
			key := fmt.Sprintf("token-%s-%d", prefix, count)
			count++
			chn <- key
		}
	}()
	return chn
}

func generateTestSources(prefix string) chan model.Source {
	count := int64(0)
	chn := make(chan model.Source)
	go func() {
		for {
			keys, attrs := make(map[string]string), make(map[string]string)
			for i := 0; i < 3; i++ {
				kk := fmt.Sprintf("source-key-%s-%d-key-%d", prefix, count, i)
				kv := fmt.Sprintf("source-key-%s-%d-val-%d", prefix, count, i)
				ak := fmt.Sprintf("source-attr-%s-%d-key-%d", prefix, count, i)
				av := fmt.Sprintf("source-attr-%s-%d-val-%d", prefix, count, i)
				keys[kk] = kv
				attrs[ak] = av
			}
			count++
			chn <- model.Source{keys, attrs}
		}
	}()
	return chn
}

func TestGetDomainEmpty(t *testing.T) {
	s := NewInMemoryStore()
	defer s.Stop()
	d, e := s.GetDomain(model.DomainKey("some-domain"))
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

	r, e = s.GetDomain(d1.Key)
	if e != nil {
		t.Fatal("Should not return an error")
	}
	if !r.Equals(d1) {
		t.Fatalf("did not return an equal copy of the retrieved domain (got %s; expected %s", r, d1)
	}

	r, e = s.GetDomain(d2.Key)
	if e != nil {
		t.Fatal("Should not return an error")
	}
	if !r.Equals(d2) {
		t.Fatalf("did not return an equal copy of the retrieved domain (got %s; expected %s", r, d2)
	}
}

type scenarioSource struct {
	Source model.Source
	Token  string
	Key    string
}

func newScenarioSource(source model.Source, token string) scenarioSource {
	return scenarioSource{
		source,
		token,
		source.KeyHash(),
	}
}

type scenario map[model.DomainKey]map[model.PartitionKey][]scenarioSource

func (sc scenario) expect(d model.DomainKey, p model.PartitionKey, t string, s model.Source) {
	dom, ok := sc[d]
	if !ok {
		dom = make(map[model.PartitionKey][]scenarioSource)
		sc[d] = dom
	}
	part, ok := dom[p]
	if !ok {
		part = make([]scenarioSource, 0)
		dom[p] = part
	}
	dom[p] = append(part, newScenarioSource(s, t))
}

func (s scenario) verify(store *InMemoryStore) (r bool, err error) {
	r = false
	for dk, parts := range s {
		for pk, sources := range parts {
			// Get the partition first.
			part, err := store.GetPartition(dk, pk)
			if part == nil && err == nil {
				err = fmt.Errorf("Could not find domain %q partition %q", dk, pk)
				return r, err
			}
			if err != nil {
				return r, err
			}

			// Keep track of the source counts per token,
			// which our ordered structure doesn't give you
			// for free.
			expectPerToken := make(map[string]int)
			receivedPerToken := make(map[string]int)
			seqnos := make([]model.Counter, len(sources))

			for i, source := range sources {
				fmt.Printf("Verifying source: %q\n\n", source)
				expectPerToken[source.Token]++
				// Find the token
				tks, ok := part.Sources[source.Token]
				if !ok {
					err = fmt.Errorf("Could not find domain %q partition %q token %q", dk, pk, source.Token)
					return r, err
				}
				// Find the source by deterministic key
				src, ok := tks[source.Key]
				if !ok {
					err = fmt.Errorf("Could not find domain %q partition %q token %q source %q (%q)", dk, pk, source.Token, source.Key, source.Source)
					return r, err
				}
				seqnos[i] = src.SeqNum
				// Verify order.
				if i > 0 {
					if !src.SeqNum.Less(seqnos[i-1], src.SeqNum) {
						err = fmt.Errorf("domain %q partition %q token %q source #%d count is out of order with source #%d (%q is not less than %q", dk, pk, source.Token, i, i-1, seqnos[i-1], src.SeqNum)
						return r, err
					}
				}
				// Verify keys and attrs
				if !reflect.DeepEqual(source.Source.Keys, src.Keys) {
					err = fmt.Errorf("domain %q partition %q token %q source #%d wrong keys (wanted %q; got %q)", dk, pk, source.Token, i, source.Source.Keys, src.Keys)
					return r, err
				}
				if !reflect.DeepEqual(source.Source.Attrs, src.Attrs) {
					err = fmt.Errorf("domain %q partition %q token %q source #%d wrong attrs (wanted %q; got %q)", dk, pk, source.Token, i, source.Source.Attrs, src.Attrs)
					return r, err
				}
			}

			// Build up the token-to-source-count mapping for
			// population-level comparison
			for token, srcs := range part.Sources {
				receivedPerToken[token] = len(srcs)
			}

			if !reflect.DeepEqual(expectPerToken, receivedPerToken) {
				err = fmt.Errorf("domain %q partition %q token sources mismatch (got %q; expected %q", dk, pk, receivedPerToken, expectPerToken)
				return r, err
			}
		}
	}
	r = err == nil
	return
}

func TestAppendNewSource(t *testing.T) {
	s := NewInMemoryStore()
	defer s.Stop()

	partkeyGen := generateTestPartitionKeys("sourceappend")
	tokenGen := generateTestTokens("sourceappend")
	sourceGen := generateTestSources("sourceappend")

	d1 := makeTestDomain(0, "a", "Aye", "b", "bI", "c", "see?")
	r, e := s.AppendNewDomain(d1)
	if e != nil {
		t.Fatal("Should not return an error")
	}
	if r == nil {
		t.Fatal("Test domain is not unique")
	}

	// Two distinct source structures; they'll get repeated
	// across tokens and partitions.
	sources := []model.Source{<-sourceGen, <-sourceGen}
	// And two distinct tokens, that'll also get repeated across partitions.
	tokens := []string{<-tokenGen, <-tokenGen}
	// And two distinct partitions, that we may repeat across domains.
	parts := []model.PartitionKey{<-partkeyGen, <-partkeyGen}

	// The data structure that expresses the order in which we expect to
	// find things.
	expectations := make(scenario)

	// Add and verify in source, token, part order.
	//expectations.expect(d1.Key, parts[0], tokens[0], sources[0])
	for _, part := range parts {
		for _, tok := range tokens {
			for _, src := range sources {
				fmt.Printf("Adding expectation for domain %q partition %q token %q source: %q", d1.Key, part, tok, src)
				expectations.expect(d1.Key, part, tok, src)
				res, err := s.AppendNewSource(d1.Key, part, tok, src)
				if err != nil {
					t.Fatalf("Failed appending domain %q, partition %q token %q source %q:\n\t%s", d1.Key, part, tok, src, err)
				}

				if res == nil {
					t.Fatalf("Unexpected nil appending domain %q partition %q token %q source %q", d1.Key, part, tok, src)
				} else if !reflect.DeepEqual(*res, src) {
					t.Fatalf("Result mistmatch appending domain %q partition %q token %q (\n\twanted: %q\n\tgot: %q)", d1.Key, part, tok, src, *res)
				}

				passed, err := expectations.verify(s)
				if err != nil {
					t.Fatalf("Expectation not met: %s", err)
				}
				if !passed {
					t.Fatalf("Expectation failed without clear error")
				}

				// If I do the same append again, we should get a nil
				// pointer back, indicating no-op, and find that the
				// state still matches the accumulated expectation.
				res, err = s.AppendNewSource(d1.Key, part, tok, src)
				if res != nil {
					t.Fatalf("Unexpected non-nil append result on domain %q partition %q token %q source %q", d1.Key, part, tok, src)
				}
				passed, err = expectations.verify(s)
				if err != nil || !passed {
					t.Fatalf("Post-noop append expectation failed (passed: %v; error: %s)", passed, err)
				}
			}
		}
	}

}
