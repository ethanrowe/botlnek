package v1

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

