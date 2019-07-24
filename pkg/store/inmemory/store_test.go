package inmemory

import (
	"encoding/json"
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

func generateTestAggregateKeys(prefix string) chan model.AggregateKey {
	count := int64(0)
	chn := make(chan model.AggregateKey)
	go func() {
		for {
			key := fmt.Sprintf("aggregate-%s-%d", prefix, count)
			count++
			chn <- model.AggregateKey(key)
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

type scenario map[model.DomainKey]map[model.AggregateKey][]scenarioSource

func (sc scenario) expect(d model.DomainKey, p model.AggregateKey, t string, s model.Source) {
	dom, ok := sc[d]
	if !ok {
		dom = make(map[model.AggregateKey][]scenarioSource)
		sc[d] = dom
	}
	aggr, ok := dom[p]
	if !ok {
		aggr = make([]scenarioSource, 0)
		dom[p] = aggr
	}
	dom[p] = append(aggr, newScenarioSource(s, t))
}

func (s scenario) verify(store *InMemoryStore) (r bool, err error) {
	r = false
	for dk, aggrs := range s {
		for pk, sources := range aggrs {
			// Get the aggregate first.
			aggr, err := store.GetAggregate(dk, pk)
			if aggr == nil && err == nil {
				err = fmt.Errorf("Could not find domain %q aggregate %q", dk, pk)
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

			for i, source := range sources {
				expectPerToken[source.Token]++
				// Find the token
				tks, ok := aggr.Sources[source.Token]
				if !ok {
					err = fmt.Errorf("Could not find domain %q aggregate %q token %q", dk, pk, source.Token)
					return r, err
				}
				// Find the source by position within the collection
				srcLog := tks[expectPerToken[source.Token]-1]
				if srcLog.Key != source.Key {
					err = fmt.Errorf("Could not find domain %q aggregate %q token %q source %q (%q)\n\treceived: %q", dk, pk, source.Token, source.Key, source.Source, srcLog)
					return r, err
				}
				// Verify that the version index matches our
				// loop index, since we expect strong ordering
				// of sources within the aggregate.
				if srcLog.VersionIdx != i {
					err = fmt.Errorf("Source VersionIdx %d does not match expected index %d", srcLog.VersionIdx, i)
					return r, err
				}

				// Verify order of referenced seqnums.
				if i > 0 {
					thisClock := aggr.Log[i]
					prevClock := aggr.Log[i-1]
					if !prevClock.SeqNum.Less(prevClock.SeqNum, thisClock.SeqNum) {
						err = fmt.Errorf("domain %q aggregate %q token %q source #%d count is out of order with source #%d (%q is not less than %q", dk, pk, source.Token, i, i-1, prevClock.SeqNum, thisClock.SeqNum)
						return r, err
					}
				}

				// Verify keys and attrs
				src := srcLog.Source
				if !reflect.DeepEqual(source.Source.Keys, src.Keys) {
					err = fmt.Errorf("domain %q aggregate %q token %q source #%d wrong keys (wanted %q; got %q)", dk, pk, source.Token, i, source.Source.Keys, src.Keys)
					return r, err
				}
				if !reflect.DeepEqual(source.Source.Attrs, src.Attrs) {
					err = fmt.Errorf("domain %q aggregate %q token %q source #%d wrong attrs (wanted %q; got %q)", dk, pk, source.Token, i, source.Source.Attrs, src.Attrs)
					return r, err
				}
			}

			// Build up the token-to-source-count mapping for
			// population-level comparison
			for token, srcs := range aggr.Sources {
				receivedPerToken[token] = len(srcs)
			}

			if !reflect.DeepEqual(expectPerToken, receivedPerToken) {
				err = fmt.Errorf("domain %q aggregate %q token sources mismatch (got %q; expected %q", dk, pk, receivedPerToken, expectPerToken)
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

	aggrkeyGen := generateTestAggregateKeys("sourceappend")
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
	// across tokens and aggregates.
	sources := []model.Source{<-sourceGen, <-sourceGen}
	// And two distinct tokens, that'll also get repeated across aggregates.
	tokens := []string{<-tokenGen, <-tokenGen}
	// And two distinct aggregates, that we may repeat across domains.
	aggrs := []model.AggregateKey{<-aggrkeyGen, <-aggrkeyGen}

	// The data structure that expresses the order in which we expect to
	// find things.
	expectations := make(scenario)

	// Add and verify in source, token, aggr order.
	//expectations.expect(d1.Key, aggrs[0], tokens[0], sources[0])
	for _, aggr := range aggrs {
		for _, tok := range tokens {
			for _, src := range sources {
				expectations.expect(d1.Key, aggr, tok, src)
				res, err := s.AppendNewSource(d1.Key, aggr, tok, src)
				if err != nil {
					t.Fatalf("Failed appending domain %q, aggregate %q token %q source %q:\n\t%s", d1.Key, aggr, tok, src, err)
				}

				if res == nil {
					t.Fatalf("Unexpected nil appending domain %q aggregate %q token %q source %q", d1.Key, aggr, tok, src)
				} else if !reflect.DeepEqual(*res, src) {
					t.Fatalf("Result mistmatch appending domain %q aggregate %q token %q (\n\twanted: %q\n\tgot: %q)", d1.Key, aggr, tok, src, *res)
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
				res, err = s.AppendNewSource(d1.Key, aggr, tok, src)
				if res != nil {
					t.Fatalf("Unexpected non-nil append result on domain %q aggregate %q token %q source %q", d1.Key, aggr, tok, src)
				}
				passed, err = expectations.verify(s)
				if err != nil || !passed {
					t.Fatalf("Post-noop append expectation failed (passed: %v; error: %s)", passed, err)
				}
			}
		}
	}

}

func TestAppendNewSourceNotification(t *testing.T) {
	s := NewInMemoryStore()
	defer s.Stop()

	aggrkeyGen := generateTestAggregateKeys("sourceappend-notify")
	tokenGen := generateTestTokens("sourceappend-notify")
	sourceGen := generateTestSources("sourceappend-notify")

	d := makeTestDomain(0, "I", "notify", "on", "source-append")
	r, e := s.AppendNewDomain(d)
	if e != nil {
		t.Fatalf("Failed to append domain: %s", e)
	}
	if r == nil {
		t.Fatal("Test domain wasn't unique!")
	}

	pk := <-aggrkeyGen
	tk := <-tokenGen

	sources := []model.Source{<-sourceGen, <-sourceGen, <-sourceGen}
	sourceLogs := make([]model.SourceLog, len(sources))

	// Buffer to the expected number of messages so we don't have to
	// worry about missing messages due to non-blocking send.
	events := make(chan []byte, len(sources))
	// Subscribing gives us a channel for signaling completion.
	done := s.SubscribeToMutations(events)

	// We expect a full representation of the aggregate with every
	// new source.
	// For now, I'm just gonna verify that the sources are there.
	for i, source := range sources {
		sourceLogs[i] = model.SourceLog{
			VersionIdx: i,
			Key:        source.KeyHash(),
			Source:     source,
		}

		fmt.Println("Appending source:", d.Key, pk, tk, source)
		resp, err := s.AppendNewSource(d.Key, pk, tk, source)
		if err != nil {
			t.Fatalf("Failed appending source %q: %s", source, err)
		}
		if resp == nil {
			t.Fatalf("Appended source not unique: %q", source)
		}
	}

	fmt.Println("checking expectations")
	// Now we verify that we received a bunch of json messages, the source contents
	// of which align with our expectations above.
	// Our test for now ignores some other stuff like clock comparisons.
	for i := 0; i < len(sources); i++ {
		rawReceived := <-events
		if rawReceived == nil {
			t.Fatalf("Received empty message")
		}
		received := struct {
			DomainKey model.DomainKey
			Aggregate struct {
				Key     model.AggregateKey
				Attrs   map[string]string
				Log     []map[string]string
				Sources map[string][]model.SourceLog
			}
		}{}
		err := json.Unmarshal(rawReceived, &received)
		if err != nil {
			t.Fatalf("Failed to unmarshal notification: %s", err)
		}

		if received.DomainKey != d.Key {
			t.Errorf("Domain key mismatch; got %q, expected %q", received.DomainKey, d.Key)
		}

		if received.Aggregate.Key != pk {
			t.Errorf("Aggregate key mistmatch; got %q, expected %q", received.Aggregate.Key, pk)
		}

		receivedSources, ok := received.Aggregate.Sources[tk]
		if !ok {
			t.Fatalf("Missing aggregate token %q", tk)
		}

		if !reflect.DeepEqual(receivedSources, sourceLogs[0:i+1]) {
			t.Errorf("Mismatch of sources:\n\tExpected %q\n\tReceived %q\n", sourceLogs[0:i+1], receivedSources)
		}
	}

	close(events)
	done <- true
}
