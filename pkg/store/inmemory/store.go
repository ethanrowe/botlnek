package inmemory

import (
	"github.com/ethanrowe/botlnek/pkg/model"
	"time"
)

type aggregateContainer struct {
	Count     InMemoryCounter
	Aggregate model.Aggregate
}

func newAggregateContainer(aggregate model.AggregateKey) *aggregateContainer {
	return &aggregateContainer{
		Aggregate: model.Aggregate{
			Key:     aggregate,
			Sources: make(model.SourceMap),
			Attrs:   make(map[string]string),
		},
	}
}

func (pc *aggregateContainer) next() InMemoryCounter {
	r := pc.Count
	pc.Count = InMemoryCounter(int64(r) + 1)
	return r
}

type aggregateStore struct {
	// A map of aggregate keys to aggregate containers
	Map map[model.AggregateKey]*aggregateContainer
}

func newAggregateStore() aggregateStore {
	return aggregateStore{
		Map: make(map[model.AggregateKey]*aggregateContainer),
	}
}

type InMemoryStore struct {
	// Map of domains, by domain key
	domains map[model.DomainKey]model.Domain
	// Map of aggregateStores, by domain key
	aggregates map[model.DomainKey]aggregateStore
	requests   chan operation
	stop       chan bool
	running    bool
	notifier   *JSONNotifier
}

func NewInMemoryStore() *InMemoryStore {
	s := &InMemoryStore{
		domains:    make(map[model.DomainKey]model.Domain),
		aggregates: make(map[model.DomainKey]aggregateStore),
		requests:   make(chan operation),
		stop:       make(chan bool),
		running:    false,
		notifier: &JSONNotifier{
			notifications: make(chan []byte),
			joins:         make(chan chan []byte),
			exits:         make(chan chan []byte),
			clients:       make(map[chan []byte]bool),
		},
	}
	go s.Run()
	return s
}

func (s *InMemoryStore) Submit(op operation) operation {
	blocker := newBlockingOp(op)
	s.requests <- blocker
	<-blocker.done
	close(blocker.done)
	return op
}

func (s *InMemoryStore) Run() {
	s.running = true

	if s.notifier != nil {
		go s.notifier.Run()
	}

	for {
		select {
		case op := <-s.requests:
			op.Do()
		case _ = <-s.stop:
			break
		}
	}
	close(s.requests)
	close(s.stop)

	if s.notifier != nil {
		s.notifier.stop <- true
	}

	s.running = false
}

func (s *InMemoryStore) Stop() {
	s.stop <- true
}

func (s *InMemoryStore) AppendNewDomain(d model.Domain) (*model.Domain, error) {
	container := newDomainOp(func(op *domainOp) {
		_, ok := s.domains[d.Key]
		if !ok {
			s.domains[d.Key] = d
			op.Domain = &d
		}
	})
	s.Submit(container)
	return container.Domain, container.Err
}

/*
func (s *InMemoryStore) SetDomain(model.Domain) (model.Domain, error) {
	return
}

func (s *InMemoryStore) GetDomainKeys(boundaryKey string, limit int, reverse bool) []string {
	return
}
*/

func (s *InMemoryStore) GetDomain(key model.DomainKey) (*model.Domain, error) {
	container := newDomainOp(func(op *domainOp) {
		got, ok := s.domains[key]
		if ok {
			op.Domain = &got
		}
	})
	s.Submit(container)
	return container.Domain, container.Err
}

func (s *InMemoryStore) AppendNewSource(domain model.DomainKey, aggregate model.AggregateKey, token string, source model.Source) (*model.Source, error) {
	container := newSourceOp(func(op *sourceOp) {
		aggrs, ok := s.aggregates[domain]
		if !ok {
			aggrs = newAggregateStore()
		}
		aggrContainer, ok := aggrs.Map[aggregate]
		if !ok {
			aggrContainer = newAggregateContainer(aggregate)
		}
		registrations, ok := aggrContainer.Aggregate.Sources[token]
		if !ok {
			registrations = make(model.SourceRegistrations)
		}
		srckey := source.KeyHash()
		reg, ok := registrations[srckey]
		// This is a new reg if it's not present already.
		if !ok {
			reg = model.SourceRegistration{
				model.ClockEntry{
					aggrContainer.next(),
					time.Now(),
				},
				source,
			}
			op.Source = &source
		}
		if op.Err == nil && op.Source != nil {
			// In this case it's a new entry, so mutate the
			// store.  Our mutations are confined to a single
			// goroutine, so this is safe.
			registrations[srckey] = reg
			aggrContainer.Aggregate.Sources[token] = registrations
			aggrs.Map[aggregate] = aggrContainer
			s.aggregates[domain] = aggrs

			// And notify, ignoring errors.
			_ = s.NotifyMutationSubscribers(model.AggregateMessage{
				DomainKey: domain,
				Aggregate: aggrContainer.Aggregate,
			})
		}
	})
	s.Submit(container)
	return container.Source, container.Err
}

/*
func (s *InMemoryStore) AppendNewAggregate(domainKey string, aggregateKey string, model.Aggregate) (model.Aggregate, error) {
}

func (s *InMemoryStore) SetAggregate(domainKey string, aggregateKey string, model.Aggregate) (model.Aggregate, error) {
}
*/

func (s *InMemoryStore) GetAggregate(domain model.DomainKey, aggregate model.AggregateKey) (*model.Aggregate, error) {
	container := newAggregateOp(func(op *aggregateOp) {
		sourceMap, ok := s.aggregates[domain]
		if !ok {
			return
		}
		aggrContainer, ok := sourceMap.Map[aggregate]
		if !ok {
			return
		}
		op.Aggregate = &aggrContainer.Aggregate
		op.Err = nil
	})
	s.Submit(container)
	return container.Aggregate, container.Err
}

func (s *InMemoryStore) SubscribeToMutations(client chan []byte) chan interface{} {
	return s.notifier.Subscribe(client)
}

func (s *InMemoryStore) NotifyMutationSubscribers(message interface{}) error {
	return s.notifier.Notify(message)
}
