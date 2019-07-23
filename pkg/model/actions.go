package model

type DomainKey string
type AggregateKey string

type DomainWriter interface {
	AppendNewDomain(Domain) (*Domain, error)
	//SetDomain(model.Domain) (*model.Domain, error)
}

type DomainReader interface {
	GetDomain(DomainKey) (*Domain, error)
}

type AggregateWriter interface {
	AppendNewSource(DomainKey, AggregateKey, string, Source) (*Source, error)
}

type AggregateReader interface {
	GetAggregate(DomainKey, AggregateKey) (*Aggregate, error)
}

type MutationNotifier interface {
	SubscribeToMutations(chan []byte) chan interface{}
	NotifyMutationSubscribers(interface{}) error
}
