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

type partitionOp struct {
	doer      func(*partitionOp)
	Partition *model.Partition
	Err       error
}

func newPartitionOp(d func(*partitionOp)) *partitionOp {
	return &partitionOp{doer: d}
}

func (op *partitionOp) Do() {
	op.doer(op)
}
