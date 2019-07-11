package inmemory

import (
	"github.com/ethanrowe/botlnek/pkg/model"
	"time"
)

type partitionContainer struct {
	Count     InMemoryCounter
	Partition model.Partition
}

func newPartitionContainer(partition model.PartitionKey) *partitionContainer {
	return &partitionContainer{
		Partition: model.Partition{
			Key:     partition,
			Sources: make(model.SourceMap),
			Attrs:   make(map[string]string),
		},
	}
}

func (pc *partitionContainer) next() InMemoryCounter {
	r := pc.Count
	pc.Count = InMemoryCounter(int64(r) + 1)
	return r
}

type partitionStore struct {
	// A map of partition keys to partition containers
	Map map[model.PartitionKey]*partitionContainer
}

func newPartitionStore() partitionStore {
	return partitionStore{
		Map: make(map[model.PartitionKey]*partitionContainer),
	}
}

type InMemoryStore struct {
	// Map of domains, by domain key
	domains map[model.DomainKey]model.Domain
	// Map of partitionStores, by domain key
	partitions map[model.DomainKey]partitionStore
	requests   chan operation
	stop       chan bool
	running    bool
	notifier   *JSONNotifier
}

func NewInMemoryStore() *InMemoryStore {
	s := &InMemoryStore{
		domains:    make(map[model.DomainKey]model.Domain),
		partitions: make(map[model.DomainKey]partitionStore),
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

func (s *InMemoryStore) AppendNewSource(domain model.DomainKey, partition model.PartitionKey, token string, source model.Source) (*model.Source, error) {
	container := newSourceOp(func(op *sourceOp) {
		parts, ok := s.partitions[domain]
		if !ok {
			parts = newPartitionStore()
		}
		partContainer, ok := parts.Map[partition]
		if !ok {
			partContainer = newPartitionContainer(partition)
		}
		registrations, ok := partContainer.Partition.Sources[token]
		if !ok {
			registrations = make(model.SourceRegistrations)
		}
		srckey := source.KeyHash()
		reg, ok := registrations[srckey]
		// This is a new reg if it's not present already.
		if !ok {
			reg = model.SourceRegistration{
				model.ClockEntry{
					partContainer.next(),
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
			partContainer.Partition.Sources[token] = registrations
			parts.Map[partition] = partContainer
			s.partitions[domain] = parts

			// And notify, ignoring errors.
			_ = s.NotifyMutationSubscribers(model.PartitionMessage{
				DomainKey: domain,
				Partition: partContainer.Partition,
			})
		}
	})
	s.Submit(container)
	return container.Source, container.Err
}

/*
func (s *InMemoryStore) AppendNewPartition(domainKey string, partitionKey string, model.Partition) (model.Partition, error) {
}

func (s *InMemoryStore) SetPartition(domainKey string, partitionKey string, model.Partition) (model.Partition, error) {
}
*/

func (s *InMemoryStore) GetPartition(domain model.DomainKey, partition model.PartitionKey) (*model.Partition, error) {
	container := newPartitionOp(func(op *partitionOp) {
		sourceMap, ok := s.partitions[domain]
		if !ok {
			return
		}
		partContainer, ok := sourceMap.Map[partition]
		if !ok {
			return
		}
		op.Partition = &partContainer.Partition
		op.Err = nil
	})
	s.Submit(container)
	return container.Partition, container.Err
}

func (s *InMemoryStore) SubscribeToMutations(client chan []byte) chan interface{} {
	return s.notifier.Subscribe(client)
}

func (s *InMemoryStore) NotifyMutationSubscribers(message interface{}) error {
	return s.notifier.Notify(message)
}
