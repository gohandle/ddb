package ddb

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// Op represents a DynamoDB operation that can be build by one
// or more calls to the "Do" method.
type Op struct {
	ctx context.Context
	ddb Dynamo
	err error

	writes []*dynamodb.TransactWriteItem
	reads  []*dynamodb.TransactGetItem
	query  *dynamodb.QueryInput
	scan   *dynamodb.ScanInput
}

// Exec starts a new DynamoDB operation
func Exec(ctx context.Context, ddb Dynamo) *Op {
	return &Op{ctx: ctx, ddb: ddb}
}

// Do is a flexible method for adding a sub-operation to the operation.
func (op *Op) Do(eb expression.Builder, o interface{}, ik ...Itemizer) *Op {
	if len(ik) > 1 {
		panic("ddb: may take at most one item(key) argument")
	}

	if op.err != nil {
		return op // ignore anythig else after an error
	}

	expr, err := eb.Build()
	var uperr expression.UnsetParameterError
	if errors.As(err, &uperr) && strings.Contains(uperr.Error(), "Builder") {
		// a zero builder as an argument is fine, so we don't report this
		// error to the user.
	} else if err != nil {
		op.err = fmt.Errorf("failed to build expression: %T", err)
		return op
	}

	var itkey map[string]*dynamodb.AttributeValue
	var pk, sk string
	if len(ik) > 0 {
		item := ik[0].Item()
		pk, sk = item.Keys()
		itkey, err = dynamodbattribute.MarshalMap(item)
		if err != nil {
			op.err = fmt.Errorf("failed to marshal item(key): %w", err)
		}
	}

	// complete the sub operation and add it
	switch ot := o.(type) {
	case dynamodb.Delete:
		ot.Key = mapFilter(itkey, pk, sk)
		ot.ConditionExpression = expr.Condition()
		ot.ExpressionAttributeNames = expr.Names()
		ot.ExpressionAttributeValues = expr.Values()
		op.writes = append(op.writes, &dynamodb.TransactWriteItem{Delete: &ot})
	case dynamodb.Put:
		ot.Item = itkey
		ot.ConditionExpression = expr.Condition()
		ot.ExpressionAttributeNames = expr.Names()
		ot.ExpressionAttributeValues = expr.Values()
		op.writes = append(op.writes, &dynamodb.TransactWriteItem{Put: &ot})
	case dynamodb.ConditionCheck:
		ot.Key = mapFilter(itkey, pk, sk)
		ot.ConditionExpression = expr.Condition()
		ot.ExpressionAttributeNames = expr.Names()
		ot.ExpressionAttributeValues = expr.Values()
		op.writes = append(op.writes, &dynamodb.TransactWriteItem{ConditionCheck: &ot})
	case dynamodb.Update:
		ot.Key = mapFilter(itkey, pk, sk)
		ot.UpdateExpression = expr.Update()
		ot.ConditionExpression = expr.Condition()
		ot.ExpressionAttributeNames = expr.Names()
		ot.ExpressionAttributeValues = expr.Values()
		op.writes = append(op.writes, &dynamodb.TransactWriteItem{Update: &ot})
	case dynamodb.Get:
		ot.Key = mapFilter(itkey, pk, sk)
		ot.ProjectionExpression = expr.Projection()
		ot.ExpressionAttributeNames = expr.Names()
		op.reads = append(op.reads, &dynamodb.TransactGetItem{Get: &ot})
	case dynamodb.QueryInput:
		ot.FilterExpression = expr.Filter()
		ot.KeyConditionExpression = expr.KeyCondition()
		ot.ProjectionExpression = expr.Projection()
		ot.ExpressionAttributeNames = expr.Names()
		ot.ExpressionAttributeValues = expr.Values()
		op.query = &ot
	case dynamodb.ScanInput:
		ot.FilterExpression = expr.Filter()
		ot.ProjectionExpression = expr.Projection()
		ot.ExpressionAttributeNames = expr.Names()
		ot.ExpressionAttributeValues = expr.Values()
		op.scan = &ot
	default:
		op.err = fmt.Errorf("unsupported sub-operation for 'Do': %T", op)
	}

	return op
}

// Run the DynamoDB operation
func (op *Op) Run() (r Result, err error) {
	switch {
	case op.query != nil: // perform a query
		return &queryResult{in: op.query}, nil
	case len(op.writes) == 1: // perform a singleton write
		fallthrough
	case op.scan != nil: // perform a scan
		fallthrough
	case len(op.reads) == 1: // perform a singleton get
		fallthrough
	case len(op.writes) > 1: // perform a write transaction
		fallthrough
	case len(op.reads) > 1: // perform a read transaction
		panic("not implemented")
	default:
		return nil, fmt.Errorf("operation cannot be run, must call 'Do' at least once.")
	}
}

// mapFilter is a utility method that returns a copy 'n' of 'm' that just holds
// the provided named element.
func mapFilter(
	m map[string]*dynamodb.AttributeValue,
	names ...string,
) (n map[string]*dynamodb.AttributeValue) {
	n = make(map[string]*dynamodb.AttributeValue)
	for _, name := range names {
		if name == "" {
			continue
		}

		if _, ok := m[name]; !ok {
			continue
		}

		n[name] = m[name]
	}
	return
}
