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

func (ts *testScenario) PrepareSourceRevision(pk, revk, srck, rev string, tstamp time.Time, keys, attrs *dynamodb.AttributeValue) (*dynamodb.BatchWriteItemOutput, error) {
	revKey := "rev"
	keysCol := "km"
	attrsCol := "am"
	input := ts.BatchWriteInput(
		&dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					ts.Store.AggregateKeyColumn: {
						S: aws.String(pk),
					},
					ts.Store.AggregateMemberKeyColumn: {
						S: aws.String(revk),
					},
					revKey: {
						N: aws.String(rev),
					},
				},
			},
		},
		&dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: map[string]*dynamodb.AttributeValue{
					ts.Store.AggregateKeyColumn: {
						S: aws.String(pk),
					},
					ts.Store.AggregateMemberKeyColumn: {
						S: aws.String(srck),
					},
					revKey: {
						N: aws.String(rev),
					},
					keysCol:  keys,
					attrsCol: attrs,
				},
			},
		},
	)
	return ts.Session.service.BatchWriteItem(input)
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
	source0 := model.Source{
		Keys:  map[string]string{"key-a": "a key", "key-b": "b key"},
		Attrs: map[string]string{"attr-a": "a attr", "attr-b": "b attr"},
	}
	source1 := model.Source{
		Keys:  map[string]string{"key-a": "b key", "key-b": "a key"},
		Attrs: map[string]string{"attr-a": "b attr", "attr-b": "a attr"},
	}
	fmt.Print(source1)
	prepResult, prepErr := scenario.PrepareSourceRevision(
		pKey,
		"R0000000000",
		fmt.Sprintf("[%q,%q]", "groupA", source0.KeyHash()),
		"0",
		time.Now(),
		ToDynamoStringMap(source0.Keys),
		ToDynamoStringMap(source0.Attrs),
	)

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

	// Now we'll add a second and third source to the same collection.
	prepResult, prepErr = scenario.PrepareSourceRevision(
		pKey,
		"R0000000001",
		fmt.Sprintf("[%q,%q]", "groupA", source1.KeyHash()),
		"1",
		time.Now(),
		ToDynamoStringMap(source1.Keys),
		ToDynamoStringMap(source1.Attrs),
	)
	if prepErr != nil {
		t.Fatalf("Failed prepping second source: %s", prepErr)
	}

	prepResult, prepErr = scenario.PrepareSourceRevision(
		pKey,
		"R0000000002",
		fmt.Sprintf("[%q,%q]", "groupB", source0.KeyHash()),
		"2",
		time.Now(),
		ToDynamoStringMap(source0.Keys),
		ToDynamoStringMap(source0.Attrs),
	)
	if prepErr != nil {
		t.Fatalf("Failed prepping second source: %s", prepErr)
	}

	// Now we expect the aggregate to have three sources across two collections.
	received, err = store.GetAggregate(dk, ak)
	if received == nil || err != nil {
		t.Fatalf("Failed to retrieve the aggregate after additional revisions: %v (%s)", received, err)
	}

	if len(received.Log) != 3 {
		t.Errorf("Expected log with 3 entries, got %d", len(received.Log))
	}

	if len(received.Sources) != 2 {
		t.Errorf("Expected 2 source collections, got %d", len(received.Sources))
	}

	groupA, ok := received.Sources["groupA"]
	if !ok {
		t.Error("Couldn't find groupA in sources.")
	}
	groupB, ok := received.Sources["groupB"]
	if !ok {
		t.Error("Couldn't find groupB in sources.")
	}

	if len(groupA) != 2 {
		t.Errorf("Expected to find 2 entries for groupA, but found %d", len(groupA))
	}

	if len(groupB) != 1 {
		t.Errorf("Expected to find 1 entry for groupB, but found %d", len(groupB))
	}

	if groupA[0].VersionIdx != 0 || groupA[1].VersionIdx != 1 {
		t.Error("Version index mismatches for group A")
	}

	if groupB[0].VersionIdx != 2 {
		t.Error("Version index mismatch for group B")
	}

	if groupA[0].Key != source0.KeyHash() || groupA[1].Key != source1.KeyHash() {
		t.Error("Source key hash mismatches for group A")
	}

	if groupB[0].Key != source0.KeyHash() {
		t.Error("Source key hash mismatch for group B")
	}

	checks := []struct {
		Expect   model.Source
		Received model.Source
		Label    string
	}{
		{
			Expect:   source0,
			Received: groupA[0].Source,
			Label:    "groupA[0]",
		},
		{
			Expect:   source1,
			Received: groupA[1].Source,
			Label:    "groupA[1]",
		},
		{
			Expect:   source0,
			Received: groupB[0].Source,
			Label:    "groupB[0]",
		},
	}
	for _, testpair := range checks {
		if !reflect.DeepEqual(testpair.Expect, testpair.Received) {
			t.Errorf("%s source mismatch; expected vs got:\n\t%v\n\t%v", testpair.Label, testpair.Expect, testpair.Received)
		}
	}
}
