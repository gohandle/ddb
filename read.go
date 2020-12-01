package ddb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// Reader represents one or more read to dynamodb
type Reader struct {
	reads []*dynamodb.TransactGetItem
	err   error
}

// NewReader inits an empty read
func NewReader() *Reader { return &Reader{} }

// Get starts a read and adds one get operation
func Get(eb expression.Builder, get dynamodb.Get, key Itemizer) *Reader {
	return NewReader().Get(eb, get, key)
}

// Get adds a get item to the read
func (r *Reader) Get(eb expression.Builder, get dynamodb.Get, key Itemizer) *Reader {
	var k Item
	expr, ok := expression.Expression{}, false
	if expr, get.Key, k, ok = r.prepArgs(eb, key); !ok {
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
func (r *Reader) Run(ctx context.Context, ddb Dynamo) (res Result, err error) {
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

	if len(out.Responses) < 0 {
		return emptyResult{}, nil
	}

	var items []map[string]*dynamodb.AttributeValue
	for _, resp := range out.Responses {
		items = append(items, resp.Item)
	}

	return newResult(items...), nil
}

// prepArgs will do checks for what is provided for a write operation
func (r *Reader) prepArgs(
	eb expression.Builder,
	ikz Itemizer,
) (expr expression.Expression, av map[string]*dynamodb.AttributeValue, ik Item, ok bool) {
	if r.err != nil {
		return
	}

	if ikz == nil {
		r.err = fmt.Errorf("Itemizer is nil")
		return
	}

	if ik = ikz.Item(); ik == nil {
		r.err = fmt.Errorf("Item from Itemizer is nil")
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

	return expr, av, ik, true
}
