package ddb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// Read represents one or more read to dynamodb
type Read struct {
	reads []*dynamodb.TransactGetItem
	err   error
}

// Get starts a read and adds one get operation
func Get(eb expression.Builder, get dynamodb.Get, key Itemizer) *Read {
	return (&Read{}).Get(eb, get, key)
}

// Get adds a get item to the read
func (r *Read) Get(eb expression.Builder, get dynamodb.Get, key Itemizer) *Read {
	expr, ok, k := expression.Expression{}, false, key.Item()
	if expr, get.Key, ok = r.prepArgs(eb, k); !ok {
		return r
	}

	pk, sk := k.Keys()
	get.Key = mapFilter(get.Key, pk, sk)
	get.ProjectionExpression = expr.Condition()
	get.ExpressionAttributeNames = expr.Names()
	r.reads = append(r.reads, &dynamodb.TransactGetItem{Get: &get})
	return r
}

// Run the read and return results
func (r *Read) Run(ctx context.Context, ddb Dynamo) (res Result, err error) {
	if r.err != nil {
		return nil, r.err
	}

	if len(r.reads) == 1 {
		return readSingle(ctx, ddb, r.reads[0])
	}

	var out *dynamodb.TransactGetItemsOutput
	if out, err = ddb.TransactGetItemsWithContext(ctx, &dynamodb.TransactGetItemsInput{
		TransactItems: r.reads,
	}); err != nil {
		return nil, fmt.Errorf("failed to transact: %w", err)
	}

	_ = out

	return
}

// prepArgs will do checks for what is provided for a write operation
func (r *Read) prepArgs(
	eb expression.Builder,
	ik Item,
) (expr expression.Expression, av map[string]*dynamodb.AttributeValue, ok bool) {
	if r.err != nil {
		return
	}

	expr, err := exprBuild(eb)
	if err != nil {
		r.err = fmt.Errorf("failed to build expression: %w", err)
		return
	}

	av, err = dynamodbattribute.MarshalMap(ik)
	if err != nil {
		r.err = fmt.Errorf("failed to marshal item: %w", err)
		return
	}

	return expr, av, true
}
