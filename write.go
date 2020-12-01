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

// Write represents a dynamodb write
type Write struct {
	writes []*dynamodb.TransactWriteItem
	err    error
}

// newWrite inits a new write
func newWrite() *Write {
	return &Write{}
}

// Put will setup a put with the check
func Put(eb expression.Builder, o dynamodb.Put, item Itemizer) *Write {
	return newWrite().Put(eb, o, item)
}

// Delete will setup a write with the delete
func Delete(eb expression.Builder, o dynamodb.Delete, key Itemizer) *Write {
	return newWrite().Delete(eb, o, key)
}

// Update will setup a write with the update
func Update(eb expression.Builder, o dynamodb.Update, key Itemizer) *Write {
	return newWrite().Update(eb, o, key)
}

// Check will setup a write with the check
func Check(eb expression.Builder, o dynamodb.ConditionCheck, key Itemizer) *Write {
	return newWrite().Check(eb, o, key)
}

// Put will add a put operation to the write
func (tx *Write) Put(eb expression.Builder, put dynamodb.Put, item Itemizer) *Write {
	expr, ok := expression.Expression{}, false
	if expr, put.Item, ok = tx.prepArgs(eb, item.Item()); !ok {
		return tx
	}

	put.ConditionExpression = expr.Condition()
	put.ExpressionAttributeNames = expr.Names()
	put.ExpressionAttributeValues = expr.Values()
	return tx
}

// Update will add a update operation to the write
func (tx *Write) Update(eb expression.Builder, upd dynamodb.Update, key Itemizer) *Write {
	expr, ok, k := expression.Expression{}, false, key.Item()
	if expr, upd.Key, ok = tx.prepArgs(eb, k); !ok {
		return tx
	}

	pk, sk := k.Keys()
	upd.Key = mapFilter(upd.Key, pk, sk)
	upd.ConditionExpression = expr.Condition()
	upd.UpdateExpression = expr.Update()
	upd.ExpressionAttributeNames = expr.Names()
	upd.ExpressionAttributeValues = expr.Values()
	return tx
}

// Update delete will add a Delete operation to the write
func (tx *Write) Delete(eb expression.Builder, del dynamodb.Delete, key Itemizer) *Write {
	expr, ok, k := expression.Expression{}, false, key.Item()
	if expr, del.Key, ok = tx.prepArgs(eb, k); !ok {
		return tx
	}

	pk, sk := k.Keys()
	del.Key = mapFilter(del.Key, pk, sk)
	del.ConditionExpression = expr.Condition()
	del.ExpressionAttributeNames = expr.Names()
	del.ExpressionAttributeValues = expr.Values()
	return tx
}

// Check will add a check operation to the write
func (tx *Write) Check(eb expression.Builder, chk dynamodb.ConditionCheck, key Itemizer) *Write {
	expr, ok, k := expression.Expression{}, false, key.Item()
	if expr, chk.Key, ok = tx.prepArgs(eb, k); !ok {
		return tx
	}

	pk, sk := k.Keys()
	chk.Key = mapFilter(chk.Key, pk, sk)
	chk.ConditionExpression = expr.Condition()
	chk.ExpressionAttributeNames = expr.Names()
	chk.ExpressionAttributeValues = expr.Values()
	return tx
}

// Run the write
func (tx *Write) Run(ctx context.Context, ddb Dynamo) (r Result, err error) {
	if len(tx.writes) == 1 {
		return writeSingle(ctx, ddb, tx.writes[0])
	}

	if _, err = ddb.TransactWriteItemsWithContext(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: tx.writes,
	}); err != nil {
		return nil, fmt.Errorf("failed to transact: %w", err)
	}

	return emptyResult{}, nil
}

// emptyResult is a result without items
type emptyResult struct{}

func (c emptyResult) Err() error { return nil }
func (c emptyResult) Next() bool { return false }
func (c emptyResult) Scan(v interface {
	Itemizer
	Deitemizer
}) (err error) {
	return
}

// prepArgs will do checks for what is provided for a write operation
func (tx *Write) prepArgs(
	eb expression.Builder,
	ik Item,
) (expr expression.Expression, av map[string]*dynamodb.AttributeValue, ok bool) {
	if tx.err != nil {
		return
	}

	expr, err := exprBuild(eb)
	if err != nil {
		tx.err = fmt.Errorf("failed to build expression: %w", err)
		return
	}

	av, err = dynamodbattribute.MarshalMap(ik)
	if err != nil {
		tx.err = fmt.Errorf("failed to marshal item: %w", err)
		return
	}

	return expr, av, true
}

// exprBuild builds the expression but ignores empty Builder error
func exprBuild(eb expression.Builder) (expr expression.Expression, err error) {
	expr, err = eb.Build()
	var uperr expression.UnsetParameterError
	if errors.As(err, &uperr) && strings.Contains(uperr.Error(), "Builder") {
		// a zero builder as an argument is fine, so we don't report this
		// error to the user.
	} else if err != nil {
		return expr, fmt.Errorf("failed to build expression: %T", err)
	}

	return expr, nil
}
