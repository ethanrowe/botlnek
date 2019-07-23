package model

type AggregateMessage struct {
	DomainKey DomainKey
	Aggregate Aggregate
}
