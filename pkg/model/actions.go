package model

type DomainKey string
type PartitionKey string

type DomainWriter interface {
	AppendNewDomain(Domain) (*Domain, error)
	//SetDomain(model.Domain) (*model.Domain, error)
}

type DomainReader interface {
	GetDomain(DomainKey) (*Domain, error)
}
