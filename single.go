package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func writeSingle(ctx context.Context, ddb Dynamo, w *dynamodb.TransactWriteItem) (r Result, err error) {
	return
}
