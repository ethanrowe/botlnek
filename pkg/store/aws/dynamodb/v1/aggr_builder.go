package v1

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ethanrowe/botlnek/pkg/model"
	"strings"
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
		ConsistentRead:       aws.Bool(true),
	}
}

// True value will mean still good to process
func (bldr *AggregateDynamoItemReader) IngestItems(data *dynamodb.QueryOutput) bool {
	bldr.Error = nil
	numLogs := 0
	memKey := bldr.Store.AggregateMemberKeyColumn

	// Scan for the revision rows first, then assume the rest are source rows.
	for _, item := range data.Items {
		if strings.HasPrefix(aws.StringValue(item[memKey].S), "R") {
			numLogs++
		} else {
			break
		}
	}

	if numLogs < 1 {
		bldr.Aggregate = nil
		return false
	}

	bldr.Aggregate.Log = make([]model.ClockEntry, numLogs)
	for i, item := range data.Items[:numLogs] {
		bldr.Aggregate.Log[i] = bldr.Store.ReadRevisionItem(item)
	}

	// Everything else is a source item
	numItems := len(data.Items) - numLogs
	for _, item := range data.Items[numLogs:] {
		groupKey, sourceEntry := bldr.Store.ReadSourceItem(item)
		group, ok := bldr.Aggregate.Sources[groupKey]
		if !ok {
			// TODO: reconsider this mem alloc algorithm; while it
			// guarantees capacity, it's a terrible algorithm if you
			// have a wide range of collection keys.
			group = make([]model.SourceLog, 0, numItems)
		}
		// reslice; we're guaranteed to have capacity.
		group = group[:len(group)+1]
		group[len(group)-1] = sourceEntry
		// reassign because we resliced.
		bldr.Aggregate.Sources[groupKey] = group
		// decrement numItems because we can guarantee capacity for later
		// groups with a smaller number.
		numItems--
	}

	return true
}
