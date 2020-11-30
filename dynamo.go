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
