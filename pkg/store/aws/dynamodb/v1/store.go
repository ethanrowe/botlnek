package v1

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/ethanrowe/botlnek/pkg/model"
	"github.com/ethanrowe/botlnek/pkg/store/inmemory"
	"strconv"
)

const (
	ColNameRevision  = "rev"
	ColNameKeys      = "km"
	ColNameAttrs     = "am"
	ColNameTimestamp = "ts"
)

type DynamoDbStoreV1 struct {
	Service                  dynamodbiface.DynamoDBAPI
	TableName                string
	AggregateKeyColumn       string
	AggregateMemberKeyColumn string
}

func NewStore(service dynamodbiface.DynamoDBAPI, tablename string) *DynamoDbStoreV1 {
	return &DynamoDbStoreV1{
		Service:                  service,
		TableName:                tablename,
		AggregateKeyColumn:       "DkAk",
		AggregateMemberKeyColumn: "AgMem",
	}
}

func (store *DynamoDbStoreV1) HashKey(dk model.DomainKey, ak model.AggregateKey) string {
	data, _ := json.Marshal([]string{string(dk), string(ak)})
	return string(data)
}

func (store *DynamoDbStoreV1) ReadRevisionItem(item map[string]*dynamodb.AttributeValue) (out model.ClockEntry) {
	num, _ := strconv.Atoi(aws.StringValue(item[ColNameRevision].N))
	out.SeqNum = inmemory.InMemoryCounter(num)
	out.Approximate, _ = FromDynamoMillisTimestamp(item[ColNameTimestamp])
	return
}

func (store *DynamoDbStoreV1) ReadSourceItem(item map[string]*dynamodb.AttributeValue) (string, model.SourceLog) {
	keyparts := make([]string, 2)
	// TODO: this error handling is garbage.  Rather, this non-handling.
	_ = json.Unmarshal([]byte(aws.StringValue(item[store.AggregateMemberKeyColumn].S)), &keyparts)
	ver, _ := strconv.Atoi(aws.StringValue(item[ColNameRevision].N))
	log := model.SourceLog{
		Key:        keyparts[1],
		VersionIdx: ver,
		Source: model.Source{
			Keys:  FromDynamoStringMap(item[ColNameKeys]),
			Attrs: FromDynamoStringMap(item[ColNameAttrs]),
		},
	}
	return keyparts[0], log
}

func (store *DynamoDbStoreV1) GetAggregate(dk model.DomainKey, ak model.AggregateKey) (*model.Aggregate, error) {
	reader := NewAggregateDynamoItemReader(store)
	query := reader.QueryInput(dk, ak)
	result, err := store.Service.Query(query)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Result from query: %v", result)

	_ = reader.IngestItems(result)
	return reader.Aggregate, reader.Error
}
