package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func writeSingle(ctx context.Context, ddb Dynamo, wi *dynamodb.TransactWriteItem) (r Result, err error) {
	// @TODO write single
	return
}

func readSingle(ctx context.Context, ddb Dynamo, ri *dynamodb.TransactGetItem) (r Result, err error) {
	// @TODO read single
	return
}
