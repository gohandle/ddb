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
		return op.singleWrite(op.writes[0])
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

func (op *Op) singleWrite(wi *dynamodb.TransactWriteItem) (r Result, err error) {
	// @TODO support: ReturnConsumedCapacity, ReturnItemCollectionMetrics, ReturnValues
	// @TODO add output returnvalues to a result

	// any of the single ops may return the attributes that were present before
	// the operation was executed. We can present that in a result
	var returns map[string]*dynamodb.AttributeValue

	switch {
	case wi.Update != nil:
		var out *dynamodb.UpdateItemOutput
		if out, err = op.ddb.UpdateItemWithContext(op.ctx, &dynamodb.UpdateItemInput{
			TableName:                 wi.Update.TableName,
			UpdateExpression:          wi.Update.UpdateExpression,
			ConditionExpression:       wi.Update.ConditionExpression,
			ExpressionAttributeNames:  wi.Update.ExpressionAttributeNames,
			ExpressionAttributeValues: wi.Update.ExpressionAttributeValues,
			Key:                       wi.Update.Key,
		}); err != nil {
			return
		}

		if out.Attributes != nil {
			returns = out.Attributes
		}
	case wi.Delete != nil:
		var out *dynamodb.DeleteItemOutput
		if out, err = op.ddb.DeleteItemWithContext(op.ctx, &dynamodb.DeleteItemInput{
			TableName:                 wi.Delete.TableName,
			ConditionExpression:       wi.Delete.ConditionExpression,
			ExpressionAttributeNames:  wi.Delete.ExpressionAttributeNames,
			ExpressionAttributeValues: wi.Delete.ExpressionAttributeValues,
			Key:                       wi.Delete.Key,
		}); err != nil {
			return
		}

		if out.Attributes != nil {
			returns = out.Attributes
		}
	case wi.Put != nil:
		var out *dynamodb.PutItemOutput
		if out, err = op.ddb.PutItemWithContext(op.ctx, &dynamodb.PutItemInput{
			TableName:                 wi.Put.TableName,
			ConditionExpression:       wi.Put.ConditionExpression,
			ExpressionAttributeNames:  wi.Put.ExpressionAttributeNames,
			ExpressionAttributeValues: wi.Put.ExpressionAttributeValues,
			Item:                      wi.Put.Item,
		}); err != nil {
			return
		}

		if out.Attributes != nil {
			returns = out.Attributes
		}
	default:
		return nil, fmt.Errorf("unsupported sub-write, must be Put, Delete or Update, got: %T", wi)
	}

	// @TODO add singleton result
	_ = returns
	// if returns != nil {
	// 	r.items = append(r.items, returns)
	// }

	return
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
