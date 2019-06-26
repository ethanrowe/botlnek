package inmemory

import (
	"github.com/ethanrowe/botlnek/pkg/model"
)

type partitionStore struct {
	// An ordered log of partitions registered to this store
	Log []model.Partition
	// A map of partition keys to partitions
	Map map[string]model.Partition
}

type InMemoryStore struct {
	// Map of domains, by string domain key
	domains map[string]model.Domain
	// Map of partitionStores, by string domain key
	partitions map[string]partitionStore
	requests   chan operation
	stop       chan bool
	running    bool
}

func NewInMemoryStore() *InMemoryStore {
	s := &InMemoryStore{
		domains:    make(map[string]model.Domain),
		partitions: make(map[string]partitionStore),
		requests:   make(chan operation),
		stop:       make(chan bool),
		running:    false,
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
	s.running = false
}

func (s *InMemoryStore) Stop() {
	s.stop <- true
}

func (s *InMemoryStore) AppendNewDomain(d model.Domain) (*model.Domain, error) {
	container := newDomainOp(func(op *domainOp) {
		_, ok := s.domains[d.DomainKey]
		if !ok {
			s.domains[d.DomainKey] = d
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

func (s *InMemoryStore) GetDomain(domainKey string) (*model.Domain, error) {
	container := newDomainOp(func(op *domainOp) {
		got, ok := s.domains[domainKey]
		if ok {
			op.Domain = &got
		}
	})
	s.Submit(container)
	return container.Domain, container.Err
}

/*
func (s *InMemoryStore) AppendNewPartition(domainKey string, partitionKey string, model.Partition) (model.Partition, error) {
}

func (s *InMemoryStore) SetPartition(domainKey string, partitionKey string, model.Partition) (model.Partition, error) {
}

func (s *InMemoryStore) GetPartition(domainKey string, partitionKey string) (model.Partition, error) {
}
*/
