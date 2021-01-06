package ddb

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Dynamo describes the strict sub-set  official DynamoDB interface that this library uses.
type Dynamo interface {
	PutItemWithContext(
		aws.Context,
		*dynamodb.PutItemInput,
		...request.Option,
	) (*dynamodb.PutItemOutput, error)

	GetItemWithContext(
		aws.Context,
		*dynamodb.GetItemInput,
		...request.Option,
	) (*dynamodb.GetItemOutput, error)

	DeleteItemWithContext(
		aws.Context,
		*dynamodb.DeleteItemInput,
		...request.Option,
	) (*dynamodb.DeleteItemOutput, error)

	UpdateItemWithContext(
		aws.Context,
		*dynamodb.UpdateItemInput,
		...request.Option,
	) (*dynamodb.UpdateItemOutput, error)

	QueryWithContext(
		aws.Context,
		*dynamodb.QueryInput,
		...request.Option,
	) (*dynamodb.QueryOutput, error)

	TransactWriteItemsWithContext(
		aws.Context,
		*dynamodb.TransactWriteItemsInput,
		...request.Option,
	) (*dynamodb.TransactWriteItemsOutput, error)

	TransactGetItemsWithContext(
		aws.Context,
		*dynamodb.TransactGetItemsInput,
		...request.Option,
	) (*dynamodb.TransactGetItemsOutput, error)

	ScanWithContext(
		aws.Context,
		*dynamodb.ScanInput,
		...request.Option,
	) (*dynamodb.ScanOutput, error)
}

// Logger interface can be implemented to log all interaction with DynamoDB
type Logger interface {
	Printf(format string, v ...interface{})
}

// loggedDynamo implements the Dynamo interface but logs all interactions
type loggedDynamo struct {
	ddb  Dynamo
	logs Logger
}

func (lddb *loggedDynamo) logf(in interface{}) {
	lddb.logs.Printf("ddb: op: %T input: %s ", in, in)
}

func (lddb *loggedDynamo) PutItemWithContext(
	ctx aws.Context,
	in *dynamodb.PutItemInput,
	opts ...request.Option,
) (*dynamodb.PutItemOutput, error) {
	lddb.logf(in)
	return lddb.ddb.PutItemWithContext(ctx, in, opts...)
}

func (lddb *loggedDynamo) GetItemWithContext(
	ctx aws.Context,
	in *dynamodb.GetItemInput,
	opts ...request.Option,
) (*dynamodb.GetItemOutput, error) {
	lddb.logf(in)
	return lddb.ddb.GetItemWithContext(ctx, in, opts...)
}

func (lddb *loggedDynamo) DeleteItemWithContext(
	ctx aws.Context,
	in *dynamodb.DeleteItemInput,
	opts ...request.Option,
) (*dynamodb.DeleteItemOutput, error) {
	lddb.logf(in)
	return lddb.ddb.DeleteItemWithContext(ctx, in, opts...)
}

func (lddb *loggedDynamo) UpdateItemWithContext(
	ctx aws.Context,
	in *dynamodb.UpdateItemInput,
	opts ...request.Option,
) (*dynamodb.UpdateItemOutput, error) {
	lddb.logf(in)
	return lddb.ddb.UpdateItemWithContext(ctx, in, opts...)
}

func (lddb *loggedDynamo) QueryWithContext(
	ctx aws.Context,
	in *dynamodb.QueryInput,
	opts ...request.Option,
) (*dynamodb.QueryOutput, error) {
	lddb.logf(in)
	return lddb.ddb.QueryWithContext(ctx, in, opts...)
}

func (lddb *loggedDynamo) TransactWriteItemsWithContext(
	ctx aws.Context,
	in *dynamodb.TransactWriteItemsInput,
	opts ...request.Option,
) (*dynamodb.TransactWriteItemsOutput, error) {
	lddb.logf(in)
	return lddb.ddb.TransactWriteItemsWithContext(ctx, in, opts...)
}

func (lddb *loggedDynamo) TransactGetItemsWithContext(
	ctx aws.Context,
	in *dynamodb.TransactGetItemsInput,
	opts ...request.Option,
) (*dynamodb.TransactGetItemsOutput, error) {
	lddb.logf(in)
	return lddb.ddb.TransactGetItemsWithContext(ctx, in, opts...)
}

func (lddb *loggedDynamo) ScanWithContext(
	ctx aws.Context,
	in *dynamodb.ScanInput,
	opts ...request.Option,
) (*dynamodb.ScanOutput, error) {
	lddb.logf(in)
	return lddb.ddb.ScanWithContext(ctx, in, opts...)
}

// LoggedDynamo returns a dynamo interface that logs every interaction with dynamodb to the
// provider logger
func LoggedDynamo(ddb Dynamo, logs Logger) Dynamo {
	return &loggedDynamo{ddb, logs}
}
