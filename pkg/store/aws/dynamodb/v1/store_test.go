package v1

import (
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"testing"
	"time"
)

type mockDynamo struct {
	dynamodbiface.DynamoDBAPI
}

func newMockDynamo() *mockDynamo {
	return &mockDynamo{}
}

func TestDefaultNames(t *testing.T) {
	mock := newMockDynamo()
	tbl := "some-table" + time.Now().Format(time.RFC3339)

	store := NewStore(mock, tbl)

	if store.Service != mock {
		t.Errorf("store service did not match input; wanted %v but got %v", mock, store.Service)
	}

	if store.TableName != tbl {
		t.Errorf("store table name did not match input; got %q", store.TableName)
	}

	if store.AggregateKeyColumn != "DkAk" {
		t.Errorf("store default aggregate key column incorrect; got %q", store.AggregateKeyColumn)
	}

	if store.AggregateMemberKeyColumn != "AgMem" {
		t.Errorf("store default aggregate member sort column incorrect; got %q", store.AggregateMemberKeyColumn)
	}
}
