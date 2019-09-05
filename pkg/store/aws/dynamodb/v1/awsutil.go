package v1

import (
	"errors"
	"regexp"
	"strings"
)

const (
	AwsMultipartExpr = "^([a-zA-Z]+Exception): [^[]+ \\[([A-Za-z, ]+)\\]"

	AwsExcTypeConditionalCheckFailed = "ConditionalCheckFailed"
	AwsExcTypeNone                   = "None"
)

var awsMultipartRegexp *regexp.Regexp

func init() {
	var err error
	awsMultipartRegexp, err = regexp.Compile(AwsMultipartExpr)
	if err != nil {
		panic(err.Error())
	}
}

type AwsMultipartException struct {
	message        string
	ExceptionType  string
	ExceptionParts []string
}

func (e AwsMultipartException) Error() string {
	return e.message
}

func NewAwsMultipartException(msg string) (mpe AwsMultipartException, e error) {
	matches := awsMultipartRegexp.FindStringSubmatch(msg)
	if matches == nil {
		e = errors.New("Message does not match multipart exception pattern")
	} else {
		mpe.message = msg
		mpe.ExceptionType = matches[1]
		mpe.ExceptionParts = strings.Split(matches[2], ", ")
	}
	return
}
