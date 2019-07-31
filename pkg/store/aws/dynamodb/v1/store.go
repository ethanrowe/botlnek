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
	ColNameRevision = "rev"
	ColNameKeys     = "km"
	ColNameAttrs    = "am"
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
	// ignore out.Approximate =
	return
}

func (store *DynamoDbStoreV1) ReadSourceItem(item map[string]*dynamodb.AttributeValue) (string, model.SourceLog) {
	keyparts := make([]string, 2)
	fmt.Printf("ReadSourceItem key part: %v\n", aws.StringValue(item[store.AggregateMemberKeyColumn].S))
	err := json.Unmarshal([]byte(aws.StringValue(item[store.AggregateMemberKeyColumn].S)), &keyparts)
	fmt.Printf("ReadSourceItem parsed key parts: %v\n\twith error: %v\n", keyparts, err)
	log := model.SourceLog{
		Key: keyparts[1],
		Source: model.Source{
			Keys:  FromDynamoStringMap(item[ColNameKeys]),
			Attrs: FromDynamoStringMap(item[ColNameAttrs]),
		},
	}
	return keyparts[0], log
}

func (store *DynamoDbStoreV1) GetAggregate(dk model.DomainKey, ak model.AggregateKey) (*model.Aggregate, error) {
	query := &dynamodb.QueryInput{
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :v1", store.AggregateKeyColumn)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v1": {
				S: aws.String(store.HashKey(dk, ak)),
			},
		},
		ProjectionExpression: aws.String(fmt.Sprintf("%s,%s,%s,%s", store.AggregateMemberKeyColumn, ColNameRevision, ColNameKeys, ColNameAttrs)),
		TableName:            aws.String(store.TableName),
		ScanIndexForward:     aws.Bool(false),
		ConsistentRead:       aws.Bool(true),
	}
	result, err := store.Service.Query(query)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Result from query: %v", result)
	// A healthy aggregate should have at least two items;
	// the version item, and at least one source item.

	if aws.Int64Value(result.Count) > 1 {
		log := make([]model.ClockEntry, 1)
		log[0] = store.ReadRevisionItem(result.Items[0])
		groupKey, sourceLogEntry := store.ReadSourceItem(result.Items[1])
		sourceLogEntry.VersionIdx = 0
		return &model.Aggregate{
			Key: ak,
			Log: log,
			Sources: model.SourceLogMap{
				groupKey: []model.SourceLog{sourceLogEntry},
			},
		}, nil
	}
	return nil, nil

}
