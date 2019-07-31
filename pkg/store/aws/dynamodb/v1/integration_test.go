// +build integration

package v1

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/ethanrowe/botlnek/pkg/model"
	"github.com/ethanrowe/botlnek/pkg/util"
	"reflect"
	"testing"
	"time"
)

var testDynamoDBEndpoint string
var testRunPrefix string

func init() {
	testDynamoDBEndpoint = util.Getenv("BOTLNEK_DYNAMODB_ENDPOINT", "http://localhost:8000")
	testRunPrefix = time.Now().Format("20060102150405")
}

type dynamoDBSession struct {
	service dynamodbiface.DynamoDBAPI
	prefix  string
	names   chan chan string
}

func newDynamoDBSession(prefix string) *dynamoDBSession {
	sess := session.Must(session.NewSession())
	dyn := dynamodb.New(sess, aws.NewConfig().WithEndpoint(testDynamoDBEndpoint))
	names := make(chan chan string)
	dbs := &dynamoDBSession{
		service: dyn,
		prefix:  prefix,
		names:   names,
	}
	go dbs.nameGen()
	return dbs
}

func (s *dynamoDBSession) nameGen() {
	i := 0
	for outchan := range s.names {
		outchan <- fmt.Sprintf("%s%s%04d", s.prefix, testRunPrefix, i)
		i++
	}
}

func (s *dynamoDBSession) Next() string {
	c := make(chan string, 1)
	s.names <- c
	return <-c
}

func (s *dynamoDBSession) BuildTable(table string, pk string, sk string) {
	result, err := s.service.CreateTable(
		&dynamodb.CreateTableInput{
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String(pk),
					KeyType:       aws.String("HASH"),
				},
				{
					AttributeName: aws.String(sk),
					KeyType:       aws.String("RANGE"),
				},
			},
			AttributeDefinitions: []*dynamodb.AttributeDefinition{
				{
					AttributeName: aws.String(pk),
					AttributeType: aws.String("S"),
				},
				{
					AttributeName: aws.String(sk),
					AttributeType: aws.String("S"),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
			TableName: aws.String(table),
		},
	)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Table build result for %s: %v\n", table, result)
}

type testScenario struct {
	Session *dynamoDBSession
	Name    string
	Store   *DynamoDbStoreV1
	done    chan bool
	Done    bool
}

func (db *dynamoDBSession) NewScenario() *testScenario {
	name := db.Next()
	ts := &testScenario{
		Session: db,
		Name:    name,
		Store:   NewStore(db.service, name),
		Done:    false,
		done:    make(chan bool),
	}
	db.BuildTable(name, ts.Store.AggregateKeyColumn, ts.Store.AggregateMemberKeyColumn)
	go ts.finisher()
	return ts
}

func (ts *testScenario) Finish() {
	ts.done <- true
}

func (ts *testScenario) finisher() {
	<-ts.done
	ts.Done = true
	ts.teardown()
}

// no op for now
func (ts *testScenario) teardown() {
	return
}

func (ts *testScenario) BatchWriteInput(items ...*dynamodb.WriteRequest) *dynamodb.BatchWriteItemInput {
	return &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			ts.Name: items,
		},
	}
}

func TestRead(t *testing.T) {
	sess := newDynamoDBSession("stub")
	scenario := sess.NewScenario()
	defer scenario.Finish()
	dk := model.DomainKey("some-domain")
	ak := model.AggregateKey("test-aggregate")
	var store model.AggregateReader

	// Cast as an aggregate reader since that's how we're using it.
	store = scenario.Store

	// No item exists, so we expect our first get to give nothing.
	received, err := store.GetAggregate(dk, ak)
	if received != nil || err != nil {
		t.Fatalf("Expected no aggregate with no error; got agg %v and err %s", received, err)
	}

	// Prep the table with our stored representation of a single-source
	// item.
	// We expect an item for the version, and an item for the source.
	pKey := fmt.Sprintf("[%q,%q]", string(dk), string(ak))
	revKey := "rev"
	keysCol := "km"
	attrsCol := "am"
	source0 := model.Source{
		Keys:  map[string]string{"key-a": "a key", "key-b": "b key"},
		Attrs: map[string]string{"attr-a": "a attr", "attr-b": "b attr"},
	}
	input := scenario.BatchWriteInput(
		&dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					scenario.Store.AggregateKeyColumn: {
						S: aws.String(pKey),
					},
					scenario.Store.AggregateMemberKeyColumn: {
						S: aws.String(revKey),
					},
					revKey: {
						N: aws.String("0"),
					},
				},
			},
		},
		&dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					scenario.Store.AggregateKeyColumn: {
						S: aws.String(pKey),
					},
					scenario.Store.AggregateMemberKeyColumn: {
						S: aws.String(fmt.Sprintf("[%q,%q]", "groupA", source0.KeyHash())),
					},
					revKey: {
						N: aws.String("0"),
					},
					keysCol: {
						M: map[string]*dynamodb.AttributeValue{
							"key-a": &dynamodb.AttributeValue{S: aws.String("a key")},
							"key-b": &dynamodb.AttributeValue{S: aws.String("b key")},
						},
					},
					attrsCol: {
						M: map[string]*dynamodb.AttributeValue{
							"attr-a": &dynamodb.AttributeValue{S: aws.String("a attr")},
							"attr-b": &dynamodb.AttributeValue{S: aws.String("b attr")},
						},
					},
				},
			},
		},
	)

	prepResult, prepErr := sess.service.BatchWriteItem(input)
	if prepErr != nil {
		t.Fatalf("Failed to prep the initial revision: %v (%v)", prepErr, prepResult)
	}

	// Now we expect the aggregate to have one source.
	received, err = store.GetAggregate(dk, ak)
	if received == nil || err != nil {
		t.Fatalf("Failed to retrieve the aggregate after initial revision: %v (%s)", received, err)
	}

	if len(received.Log) != 1 {
		t.Fatalf("Expected aggregate with 1 log entry; got %d", len(received.Log))
	}

	if len(received.Sources) != 1 {
		t.Fatalf("Expected aggregate with 1 collection key; got %d keys", len(received.Sources))
	}

	fmt.Printf("Aggregate: %v\n", received)
	colSrcs, ok := received.Sources["groupA"]
	if !ok {
		t.Fatal("Expected to find collection 'groupA'; not found.")
	}

	if len(colSrcs) != 1 {
		t.Fatalf("Expected source collection list of len 1; got len %d", len(colSrcs))
	}
	if colSrcs[0].Key != source0.KeyHash() {
		t.Fatal("Key mismatch on first source!")
	}

	if colSrcs[0].VersionIdx != 0 {
		t.Fatalf("First source expected version index %d; got %d", 0, colSrcs[0].VersionIdx)
	}

	if !reflect.DeepEqual(source0, colSrcs[0].Source) {
		t.Fatalf("Source mismatch: Expected vs. Got:\n\t%v\n\t%v", source0, colSrcs[0].Source)
	}
}
