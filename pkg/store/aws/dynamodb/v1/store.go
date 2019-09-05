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
	"time"
)

const (
	ColNameRevision  = "rev"
	ColNameKeys      = "km"
	ColNameAttrs     = "am"
	ColNameTimestamp = "ts"
	RevSortKeyMin    = "R0000000000"
	RevSortKeyMax    = "R9999999999"
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

func (store *DynamoDbStoreV1) SourceSortKey(colKey string, source model.Source) string {
	data, _ := json.Marshal([]string{colKey, source.KeyHash()})
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

	_ = reader.IngestItems(result)
	return reader.Aggregate, reader.Error
}

func (store *DynamoDbStoreV1) GetNextRev(hashKey *string) (next int, err error) {
	resp, err := store.Service.Query(&dynamodb.QueryInput{
		TableName:              aws.String(store.TableName),
		KeyConditionExpression: aws.String("#H = :hk AND #S BETWEEN :smin AND :smax"),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":hk":   {S: hashKey},
			":smin": {S: aws.String(RevSortKeyMin)},
			":smax": {S: aws.String(RevSortKeyMax)},
		},
		ExpressionAttributeNames: map[string]*string{
			"#H": aws.String(store.AggregateKeyColumn),
			"#S": aws.String(store.AggregateMemberKeyColumn),
		},
		Limit:                aws.Int64(1),
		ScanIndexForward:     aws.Bool(false),
		ProjectionExpression: aws.String("rev"),
	})
	if err == nil {
		if aws.Int64Value(resp.Count) > 0 {
			next, err = strconv.Atoi(aws.StringValue(resp.Items[0]["rev"].N))
			if err == nil {
				next++
			}
		}
	}
	return
}

func (store *DynamoDbStoreV1) AppendNewSource(dk model.DomainKey, ak model.AggregateKey, colkey string, source model.Source) (*model.Source, error) {
	hashKey := aws.String(store.HashKey(dk, ak))
	nextRev, err := store.GetNextRev(hashKey)
	if err != nil {
		return nil, err
	}
	revSort := aws.String(fmt.Sprintf("R%010d", nextRev))
	revVal := &dynamodb.AttributeValue{N: aws.String(fmt.Sprintf("%d", nextRev))}
	resp, err := store.Service.TransactWriteItems(&dynamodb.TransactWriteItemsInput{
		TransactItems: []*dynamodb.TransactWriteItem{
			{
				Put: &dynamodb.Put{
					TableName:           aws.String(store.TableName),
					ConditionExpression: aws.String("attribute_not_exists(#S)"),
					ExpressionAttributeNames: map[string]*string{
						"#S": aws.String(store.AggregateMemberKeyColumn),
					},
					Item: map[string]*dynamodb.AttributeValue{
						store.AggregateKeyColumn: {
							S: hashKey,
						},
						store.AggregateMemberKeyColumn: {
							S: revSort,
						},
						"rev": revVal,
						"ts":  ToDynamoMillisTimestamp(time.Now()),
					},
				},
			},
			{
				Put: &dynamodb.Put{
					TableName:           aws.String(store.TableName),
					ConditionExpression: aws.String("attribute_not_exists(#S)"),
					ExpressionAttributeNames: map[string]*string{
						"#S": aws.String(store.AggregateMemberKeyColumn),
					},
					Item: map[string]*dynamodb.AttributeValue{
						store.AggregateKeyColumn: {
							S: hashKey,
						},
						store.AggregateMemberKeyColumn: {
							S: aws.String(store.SourceSortKey(colkey, source)),
						},
						"rev": revVal,
						"km":  ToDynamoStringMap(source.Keys),
						"am":  ToDynamoStringMap(source.Attrs),
					},
				},
			},
		},
	})
	if err != nil {
		mpe, perr := NewAwsMultipartException(err.Error())
		if perr == nil {
			// If the second item's condition failed, that means
			// the item already exists, which is a successful result.
			// For this situation, the first item's success/failure
			// is irrelevant.
			if mpe.ExceptionParts[1] == AwsExcTypeConditionalCheckFailed {
				// The source should be nil in this case,
				// because we didn't mutate anything.
				return nil, nil
			}

		} else {
			fmt.Println("Problem parsing MPE:", perr)
			fmt.Println("ERROR", err)
		}
		return nil, err
	}
	fmt.Println("Got dynamo insert responses:", resp)
	return &source, nil
}
