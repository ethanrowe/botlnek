package v1

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ethanrowe/botlnek/pkg/model"
)

type AggregateDynamoItemReader struct {
	Store         *DynamoDbStoreV1
	Aggregate     *model.Aggregate
	Error         error
	logComplete   bool
	logKeyToIndex map[string]int
}

func NewAggregateDynamoItemReader(store *DynamoDbStoreV1) *AggregateDynamoItemReader {
	return &AggregateDynamoItemReader{
		Store: store,
		Aggregate: &model.Aggregate{
			Log:     make([]model.ClockEntry, 0),
			Sources: make(model.SourceLogMap),
		},
		logComplete:   false,
		logKeyToIndex: make(map[string]int),
	}
}

func (bldr *AggregateDynamoItemReader) QueryInput(dk model.DomainKey, ak model.AggregateKey) *dynamodb.QueryInput {
	return &dynamodb.QueryInput{
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :v1", bldr.Store.AggregateKeyColumn)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v1": {
				S: aws.String(bldr.Store.HashKey(dk, ak)),
			},
		},
		ProjectionExpression: aws.String(fmt.Sprintf("%s,%s,%s,%s", bldr.Store.AggregateMemberKeyColumn, ColNameRevision, ColNameKeys, ColNameAttrs)),
		TableName:            aws.String(bldr.Store.TableName),
		ScanIndexForward:     aws.Bool(false),
		ConsistentRead:       aws.Bool(true),
	}
}

// True value will mean still good to process
func (bldr *AggregateDynamoItemReader) IngestItems(data *dynamodb.QueryOutput) bool {
	// A healthy aggregate should have at least two items;
	// the version item, and at least one source item.
	bldr.Error = nil

	if aws.Int64Value(data.Count) > 1 {
		groupKey, sourceLogEntry := bldr.Store.ReadSourceItem(data.Items[1])
		sourceLogEntry.VersionIdx = 0
		bldr.Aggregate.Log = append(bldr.Aggregate.Log, bldr.Store.ReadRevisionItem(data.Items[0]))
		bldr.Aggregate.Sources = model.SourceLogMap{
			groupKey: []model.SourceLog{sourceLogEntry},
		}
	} else {
		bldr.Aggregate = nil
	}
	return true
}
