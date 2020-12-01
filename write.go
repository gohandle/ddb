package ddb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// Writer represents one or more DynamoDB write operations
type Writer struct {
	writes []*dynamodb.TransactWriteItem
	err    error
}

// NewWriter inits a new write
func NewWriter() *Writer {
	return &Writer{}
}

// Put will setup a put with the check
func Put(eb expression.Builder, o dynamodb.Put, item Itemizer) *Writer {
	return NewWriter().Put(eb, o, item)
}

// Delete will setup a write with the delete
func Delete(eb expression.Builder, o dynamodb.Delete, key Itemizer) *Writer {
	return NewWriter().Delete(eb, o, key)
}

// Update will setup a write with the update
func Update(eb expression.Builder, o dynamodb.Update, key Itemizer) *Writer {
	return NewWriter().Update(eb, o, key)
}

// Check will setup a write with the check
func Check(eb expression.Builder, o dynamodb.ConditionCheck, key Itemizer) *Writer {
	return NewWriter().Check(eb, o, key)
}

// Put will add a put operation to the write
func (tx *Writer) Put(eb expression.Builder, put dynamodb.Put, item Itemizer) *Writer {
	expr, ok := expression.Expression{}, false
	if expr, put.Item, _, ok = tx.prepArgs(eb, item); !ok {
		return tx
	}

	put.ConditionExpression = expr.Condition()
	put.ExpressionAttributeNames = expr.Names()
	put.ExpressionAttributeValues = expr.Values()
	tx.writes = append(tx.writes, &dynamodb.TransactWriteItem{Put: &put})
	return tx
}

// Update will add a update operation to the write
func (tx *Writer) Update(eb expression.Builder, upd dynamodb.Update, key Itemizer) *Writer {
	var k Item
	expr, ok := expression.Expression{}, false
	if expr, upd.Key, k, ok = tx.prepArgs(eb, key); !ok {
		return tx
	}

	pk, sk := k.Keys()
	upd.Key = mapFilter(upd.Key, pk, sk)
	upd.ConditionExpression = expr.Condition()
	upd.UpdateExpression = expr.Update()
	upd.ExpressionAttributeNames = expr.Names()
	upd.ExpressionAttributeValues = expr.Values()
	tx.writes = append(tx.writes, &dynamodb.TransactWriteItem{Update: &upd})
	return tx
}

// Update delete will add a Delete operation to the write
func (tx *Writer) Delete(eb expression.Builder, del dynamodb.Delete, key Itemizer) *Writer {
	var k Item
	expr, ok := expression.Expression{}, false
	if expr, del.Key, k, ok = tx.prepArgs(eb, key); !ok {
		return tx
	}

	pk, sk := k.Keys()
	del.Key = mapFilter(del.Key, pk, sk)
	del.ConditionExpression = expr.Condition()
	del.ExpressionAttributeNames = expr.Names()
	del.ExpressionAttributeValues = expr.Values()
	tx.writes = append(tx.writes, &dynamodb.TransactWriteItem{Delete: &del})
	return tx
}

// Check will add a check operation to the write
func (tx *Writer) Check(eb expression.Builder, chk dynamodb.ConditionCheck, key Itemizer) *Writer {
	var k Item
	expr, ok := expression.Expression{}, false
	if expr, chk.Key, k, ok = tx.prepArgs(eb, key); !ok {
		return tx
	}

	pk, sk := k.Keys()
	chk.Key = mapFilter(chk.Key, pk, sk)
	chk.ConditionExpression = expr.Condition()
	chk.ExpressionAttributeNames = expr.Names()
	chk.ExpressionAttributeValues = expr.Values()
	tx.writes = append(tx.writes, &dynamodb.TransactWriteItem{ConditionCheck: &chk})
	return tx
}

// Run the write
func (tx *Writer) Run(ctx context.Context, ddb Dynamo) (r Result, err error) {
	if tx.err != nil {
		return nil, tx.err
	}

	// if only one write, and it is not a condition check downgrade to non-transaction
	if len(tx.writes) == 1 && tx.writes[0].ConditionCheck == nil {
		return writeSingle(ctx, ddb, tx.writes[0])
	}

	if _, err = ddb.TransactWriteItemsWithContext(ctx, &dynamodb.TransactWriteItemsInput{
		// @TODO generate and set ClientRequestToken
		TransactItems: tx.writes,
	}); err != nil {
		return nil, fmt.Errorf("failed to transact: %w", err)
	}

	return emptyResult{}, nil
}

// prepArgs will do checks for what is provided for a write operation
func (tx *Writer) prepArgs(
	eb expression.Builder,
	ikz Itemizer,
) (expr expression.Expression, av map[string]*dynamodb.AttributeValue, ik Item, ok bool) {
	if tx.err != nil {
		return
	}

	if ikz == nil {
		tx.err = fmt.Errorf("itemizer is nil")
		return
	}

	if ik = ikz.Item(); ik == nil {
		tx.err = fmt.Errorf("Item returned from Itemizer is nil")
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

	return expr, av, ik, true
}
