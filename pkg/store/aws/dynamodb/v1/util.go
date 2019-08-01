package v1

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"time"
)

const (
	TimestampMillis = "20060102T150405.999"
)

func ToDynamoStringMap(m map[string]string) *dynamodb.AttributeValue {
	inner := make(map[string]*dynamodb.AttributeValue)
	for key, val := range m {
		inner[key] = &dynamodb.AttributeValue{
			S: aws.String(val),
		}
	}

	return &dynamodb.AttributeValue{M: inner}
}

func FromDynamoStringMap(m *dynamodb.AttributeValue) map[string]string {
	if m.M == nil {
		return nil
	}
	result := make(map[string]string)
	for key, val := range m.M {
		result[key] = aws.StringValue(val.S)
	}
	return result
}

func ToDynamoMillisTimestamp(ts time.Time) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{
		S: aws.String(ts.UTC().Format(TimestampMillis)),
	}
}

func FromDynamoMillisTimestamp(s *dynamodb.AttributeValue) (t time.Time, e error) {
	if s.S == nil {
		e = errors.New("Given attribute is not a millisecond timestamp")
	} else {
		t, e = time.Parse(TimestampMillis, aws.StringValue(s.S))
	}
	return
}
