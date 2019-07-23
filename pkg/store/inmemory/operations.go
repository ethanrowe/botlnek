package inmemory

import (
	"github.com/ethanrowe/botlnek/pkg/model"
)

type operation interface {
	Do()
}

type blockingOp struct {
	op   operation
	done chan bool
}

func (b blockingOp) Do() {
	b.op.Do()
	b.done <- true
}

func newBlockingOp(op operation) blockingOp {
	return blockingOp{
		op:   op,
		done: make(chan bool),
	}
}

type domainOp struct {
	doer   func(*domainOp)
	Domain *model.Domain
	Err    error
}

func newDomainOp(d func(*domainOp)) *domainOp {
	return &domainOp{doer: d}
}

func (op *domainOp) Do() {
	op.doer(op)
}

type sourceOp struct {
	doer   func(*sourceOp)
	Source *model.Source
	Err    error
}

func newSourceOp(op func(*sourceOp)) *sourceOp {
	return &sourceOp{doer: op}
}

func (op *sourceOp) Do() {
	op.doer(op)
}

type aggregateOp struct {
	doer      func(*aggregateOp)
	Aggregate *model.Aggregate
	Err       error
}

func newAggregateOp(d func(*aggregateOp)) *aggregateOp {
	return &aggregateOp{doer: d}
}

func (op *aggregateOp) Do() {
	op.doer(op)
}
