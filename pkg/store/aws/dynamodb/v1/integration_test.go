// +build integration

package v1

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/ethanrowe/botlnek/pkg/model"
	"github.com/ethanrowe/botlnek/pkg/util"
	"reflect"
	"sort"
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
	_, err := s.service.CreateTable(
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
	_, err := ts.Session.service.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: aws.String(ts.Name),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == dynamodb.ErrCodeResourceNotFoundException {
				err = nil
			}
		}
	}
	if err != nil {
		fmt.Println("Teardown of", ts.Name, "resulted in error:", err)
	}
}

func (ts *testScenario) BatchWriteInput(items ...*dynamodb.WriteRequest) *dynamodb.BatchWriteItemInput {
	return &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			ts.Name: items,
		},
	}
}

func (ts *testScenario) Query(query dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	query.TableName = aws.String(ts.Name)
	return ts.Session.service.Query(&query)
}

func (ts *testScenario) PrepareSourceRevision(pk, revk, srck, rev string, tstamp time.Time, keys, attrs *dynamodb.AttributeValue) (*dynamodb.BatchWriteItemOutput, error) {
	revKey := "rev"
	tsCol := "ts"
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
					tsCol: {
						S: aws.String(tstamp.Format("20060102T150405.999")),
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

func (ts *testScenario) PutItems(items []map[string]*dynamodb.AttributeValue) error {
	inputs := make([]*dynamodb.WriteRequest, len(items))
	for i, item := range items {
		inputs[i] = &dynamodb.WriteRequest{
			PutRequest: &dynamodb.PutRequest{
				Item: item,
			},
		}
	}
	_, err := ts.Session.service.BatchWriteItem(ts.BatchWriteInput(inputs...))
	return err
}

func (ts *testScenario) PrepareLogExpectation(pk, revk, rev string, tstamp ...time.Time) map[string]*dynamodb.AttributeValue {
	out := map[string]*dynamodb.AttributeValue{
		ts.Store.AggregateKeyColumn: {
			S: aws.String(pk),
		},
		ts.Store.AggregateMemberKeyColumn: {
			S: aws.String(revk),
		},
		"rev": {
			N: aws.String(rev),
		},
	}
	if len(tstamp) > 1 {
		panic("Only one timestamp may be provided")
	}
	if len(tstamp) == 1 {
		out["ts"] = &dynamodb.AttributeValue{
			S: aws.String(tstamp[0].Format("20060102T150405.999")),
		}
	}
	return out
}

func (ts *testScenario) PrepareSourceExpectation(pk, mk, rev string, keys, attrs map[string]string) map[string]*dynamodb.AttributeValue {
	k := make(map[string]*dynamodb.AttributeValue)
	a := make(map[string]*dynamodb.AttributeValue)
	for key, val := range keys {
		k[key] = &dynamodb.AttributeValue{S: aws.String(val)}
	}
	for key, val := range attrs {
		a[key] = &dynamodb.AttributeValue{S: aws.String(val)}
	}
	return map[string]*dynamodb.AttributeValue{
		ts.Store.AggregateKeyColumn: {
			S: aws.String(pk),
		},
		ts.Store.AggregateMemberKeyColumn: {
			S: aws.String(mk),
		},
		"rev": {
			N: aws.String(rev),
		},
		"km": {
			M: k,
		},
		"am": {
			M: a,
		},
	}
}

// The aggregate rows in item group `pk` match up (not just in provided properties,
// but in row count and order).
func (ts *testScenario) VerifyAllAggregateItems(pk string, items []map[string]*dynamodb.AttributeValue) error {
	qry, err := ts.Query(dynamodb.QueryInput{
		KeyConditionExpression: aws.String(fmt.Sprintf("%s = :v1", ts.Store.AggregateKeyColumn)),
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{
			":v1": {
				S: aws.String(pk),
			},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return err
	}
	if aws.Int64Value(qry.Count) != int64(len(items)) {
		return fmt.Errorf("expected %d items; found %d", len(items), aws.Int64Value(qry.Count))
	}

	for i, item := range items {
		got := qry.Items[i]
		for key, val := range item {
			rec, ok := got[key]
			if !ok {
				return fmt.Errorf("item %d expected attr %s not found", i, key)
			}
			if !reflect.DeepEqual(val, rec) {
				return fmt.Errorf("item %d attr %s expected %v but received %v", i, key, val, rec)
			}
		}
	}
	return nil
}

func (ts *testScenario) SortedSourceItems(items []map[string]*dynamodb.AttributeValue) []map[string]*dynamodb.AttributeValue {
	out := make([]map[string]*dynamodb.AttributeValue, len(items))
	copy(out, items)
	sorter := func(i, j int) bool {
		return aws.StringValue(out[i][ts.Store.AggregateMemberKeyColumn].S) < aws.StringValue(out[j][ts.Store.AggregateMemberKeyColumn].S)
	}
	sort.Slice(out, sorter)
	return out
}

func itemConcat(items ...[]map[string]*dynamodb.AttributeValue) []map[string]*dynamodb.AttributeValue {
	length := 0
	for _, item := range items {
		length = length + len(item)
	}
	result := make([]map[string]*dynamodb.AttributeValue, 0, length)
	for _, item := range items {
		result = append(result, item...)
	}
	return result
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

	//Round all times to nearest millisecond, since going beyond that is
	//sort of ridiculous.  Note that I splay by a millisecond in either
	//direction so we're sure to get times with a millisecond count other
	//than zero.
	times := make([]time.Time, 3)
	times[2] = time.Now().UTC().Round(time.Millisecond)
	times[1] = times[2].Add(-time.Minute + time.Millisecond)
	times[0] = times[2].Add(-2*time.Minute - time.Millisecond)

	fmt.Print(source1)
	prepResult, prepErr := scenario.PrepareSourceRevision(
		pKey,
		"R0000000000",
		fmt.Sprintf("[%q,%q]", "groupA", source0.KeyHash()),
		"0",
		times[0],
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

	if received.Log[0].Approximate != times[0] {
		t.Errorf("Log[0] time expected %s but got %s", times[0], received.Log[0].Approximate)
	}

	if len(received.Sources) != 1 {
		t.Fatalf("Expected aggregate with 1 collection key; got %d keys", len(received.Sources))
	}

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
		times[1],
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
		times[2],
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

	for i, l := range received.Log {
		if l.Approximate != times[i] {
			t.Errorf("Log[%d] expected time %s but got %s", i, times[i], l.Approximate)
		}
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

// Writing source for a new aggregate
func TestNewAggregateWrite(t *testing.T) {
	sess := newDynamoDBSession("basic-write")
	scenario := sess.NewScenario()
	defer scenario.Finish()
	dk := model.DomainKey("empty")
	ak := model.AggregateKey("new")
	colk := "some-collection"

	var store model.AggregateWriter

	// Cast as an aggregate writer since that's how we're using it.
	store = scenario.Store

	source := model.Source{
		Keys: map[string]string{
			"foo-key": "foo value",
			"bar-key": "bar value",
		},
		Attrs: map[string]string{
			"baz-attr":  "baz value",
			"bink-attr": "bink value",
		},
	}

	// We should get no error, and respSource should match source, because
	// it's new.
	respSource, respErr := store.AppendNewSource(dk, ak, colk, source)

	if respErr != nil {
		t.Fatalf("Failed inserting source: %s", respErr)
	}

	if respSource == nil {
		t.Fatal("Expected non-nil source response")
	}

	if !reflect.DeepEqual(source, *respSource) {
		t.Errorf("Source response mismatch, expected vs received:\n\t%v\n\t%v", source, *respSource)
	}

	// Now let's consider the state in the table.
	// partition key is domain key and agg key in json list
	// source sort key is collection key and source keyhash in json list
	pk := fmt.Sprintf("[%q,%q]", string(dk), string(ak))
	sk := fmt.Sprintf("[%q,%q]", colk, source.KeyHash())
	expectedItems := []map[string]*dynamodb.AttributeValue{
		scenario.PrepareLogExpectation(pk, "R0000000000", "0"),
		scenario.PrepareSourceExpectation(pk, sk, "0", source.Keys, source.Attrs),
	}

	err := scenario.VerifyAllAggregateItems(pk, expectedItems)
	if err != nil {
		t.Errorf("Expected dynamodb item failure: %s", err)
	}
}

// Writing source for an existing aggregate
func TestExistingAggregateWrite(t *testing.T) {
	sess := newDynamoDBSession("existing-write")
	scenario := sess.NewScenario()
	defer scenario.Finish()
	dk := model.DomainKey("extant")
	ak := model.AggregateKey("existing")

	sourceKey := func(groupkey, sourcekey string) string {
		return fmt.Sprintf("[%q,%q]", groupkey, sourcekey)
	}

	// Cast as an aggregate writer since that's how we're using it.
	var store model.AggregateWriter
	store = scenario.Store

	source1 := model.Source{
		Keys: map[string]string{
			"I":       "like",
			"traffic": "lights",
		},
		Attrs: map[string]string{
			"That": "is",
			"what": "I said",
		},
	}

	source2 := model.Source{
		Keys: map[string]string{
			"but":  "not",
			"when": "they",
		},
		Attrs: map[string]string{
			"are": "red",
		},
	}

	// The first two logs will have explicit timestamps because we're prepping
	// them in dynamodb directly; the others get indeterminate timestamps we'll
	// check later.
	pk := fmt.Sprintf("[%q,%q]", string(dk), string(ak))
	expectedLogItems := []map[string]*dynamodb.AttributeValue{
		scenario.PrepareLogExpectation(pk, "R0000000000", "0", time.Now().UTC().Add(-time.Minute).Round(time.Millisecond).Add(5*time.Millisecond)),
		scenario.PrepareLogExpectation(pk, "R0000000001", "1", time.Now().UTC().Add(-30*time.Second).Round(time.Millisecond).Add(-5*time.Millisecond)),
		scenario.PrepareLogExpectation(pk, "R0000000002", "2"),
		scenario.PrepareLogExpectation(pk, "R0000000003", "3"),
	}

	// The first two guys are the same source in different collections,
	// prepped in dynamodb.  The second two guys are what we expect after
	// appending the second source to those collections.
	expectedSourceItems := []map[string]*dynamodb.AttributeValue{
		scenario.PrepareSourceExpectation(pk, sourceKey("group-a", source1.KeyHash()), "0", source1.Keys, source1.Attrs),
		scenario.PrepareSourceExpectation(pk, sourceKey("group-b", source1.KeyHash()), "1", source1.Keys, source1.Attrs),
		scenario.PrepareSourceExpectation(pk, sourceKey("group-a", source2.KeyHash()), "2", source2.Keys, source2.Attrs),
		scenario.PrepareSourceExpectation(pk, sourceKey("group-b", source2.KeyHash()), "3", source2.Keys, source2.Attrs),
	}

	err := scenario.PutItems(itemConcat(expectedLogItems[:2], expectedSourceItems[:2]))
	if err != nil {
		t.Fatalf("Failed to prepare items in dynamo: %s", err)
	}

	// We'll separate our append attempts by some time so we can verify
	// that the timestamps have a reasonable relationship after the fact.
	for _, grp := range []string{"group-a", "group-b"} {
		time.Sleep(100 * time.Millisecond)
		res, err := store.AppendNewSource(dk, ak, grp, source2)

		if err != nil {
			t.Errorf("Failed to append second source to %s: %s", grp, err)
		}

		if res == nil {
			t.Errorf("expected non-nil source for append to %s", grp)
		} else if !reflect.DeepEqual(source2, *res) {
			t.Errorf("%s append of second source mismatch (expect vs receive):\n\t%v\n\t%v", grp, source2, res)
		}
	}

	// Now check the dynamodb state against the expectations.
	err = scenario.VerifyAllAggregateItems(pk, itemConcat(expectedLogItems, scenario.SortedSourceItems(expectedSourceItems)))
	if err != nil {
		t.Fatalf("Expected dynamodb item failure: %s", err)
	}

	// If we're here, state lines up with expectations, so we just need to
	// check the timestampiness of the last two logs (that were created by
	// our append operations).
}

// Writing source that already exists in target aggregate
func TestConflictingWrite(t *testing.T) {
	sess := newDynamoDBSession("conflicting-write")
	scenario := sess.NewScenario()
	defer scenario.Finish()
	dk := model.DomainKey("existing")
	ak := model.AggregateKey("conflicting")
	colk := "mygroup"

	// Cast as an aggregate writer since that's how we're using it.
	var store model.AggregateWriter
	store = scenario.Store

	source := model.Source{
		Keys: map[string]string{
			"Eennie":  "Meennie",
			"Meinnie": "Moe",
		},
		Attrs: map[string]string{
			"Not": "Relevant",
		},
	}

	pk := fmt.Sprintf("[%q,%q]", string(dk), string(ak))
	sk := fmt.Sprintf("[%q,%q]", colk, source.KeyHash())
	expectedItems := []map[string]*dynamodb.AttributeValue{
		scenario.PrepareLogExpectation(pk, "R0000000000", "0", time.Now().UTC().Round(time.Millisecond)),
		scenario.PrepareSourceExpectation(pk, sk, "0", source.Keys, map[string]string{"Don't": "matter"}),
	}

	err := scenario.PutItems(expectedItems)
	if err != nil {
		t.Fatalf("Failed to prepare items in dynamo: %s", err)
	}

	srcOut, err := store.AppendNewSource(dk, ak, colk, source)
	if err != nil {
		t.Errorf("Error reported appending source: %s", err)
	}

	if srcOut != nil {
		t.Error("Expected nil source, but received non-nil result")
	}

	err = scenario.VerifyAllAggregateItems(pk, expectedItems)
	if err != nil {
		t.Errorf("Dynamodb state mismatch: %s", err)
	}
}
