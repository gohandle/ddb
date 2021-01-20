package ddb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// Scanner holds a DynamoDB query
type Scanner struct {
	res *scanResult
	eb  expression.Builder
}

// Scan sets up a scanner that can be run to fetch
func Scan(b expression.Builder, in dynamodb.ScanInput) (q *Scanner) {
	q = new(Scanner)
	q.res = &scanResult{pos: -1}
	q.res.in = &in
	q.eb = b
	return
}

// Run will return a Query result for iteration
func (q *Scanner) Run(ctx context.Context, ddb Dynamo) (r Result, err error) {
	expr, err := exprBuild(q.eb)
	if err != nil {
		return nil, fmt.Errorf("failed to build expression(s): %w", err)
	}

	q.res.ddb = ddb
	q.res.ctx = ctx
	q.res.in.FilterExpression = expr.Filter()
	q.res.in.ProjectionExpression = expr.Projection()
	q.res.in.ExpressionAttributeNames = expr.Names()
	q.res.in.ExpressionAttributeValues = expr.Values()
	return q.res, q.res.init()
}

// scanResult is a result that is returned when a scan operation
// is run. It will automatically get more pages as the user scans
// through the results.
type scanResult struct {
	ctx context.Context
	in  *dynamodb.ScanInput
	out *dynamodb.ScanOutput
	tot int64
	ddb Dynamo
	err error
	pos int
}

func (c *scanResult) init() (err error) {
	if c.out, err = c.ddb.ScanWithContext(c.ctx, c.in); err != nil {
		return err
	}

	c.tot = *c.out.Count
	return nil
}

func (c *scanResult) Len() int64 {
	return c.tot
}

func (c *scanResult) Err() error {
	return c.err
}

func (c *scanResult) Next() bool {

	// we got some query output, start with pos iteration
	c.pos++
	if c.out != nil && c.pos >= len(c.out.Items) {
		if c.out.LastEvaluatedKey == nil {
			return false // fully done
		}

		// else we prep the cursor for a new query
		if c.out.LastEvaluatedKey != nil {
			c.pos = 0
			c.in.ExclusiveStartKey = c.out.LastEvaluatedKey
			c.out = nil
		}
	}

	// no out, run again
	if c.out == nil {
		if c.out, c.err = c.ddb.ScanWithContext(c.ctx, c.in); c.err != nil {
			return false
		}
		c.tot += *c.out.Count
	}

	return true
}

func (c *scanResult) Scan(v interface {
	Itemizer
	Deitemizer
}) (err error) {
	it := v.Item()
	if err = dynamodbattribute.UnmarshalMap(c.out.Items[c.pos], it); err != nil {
		return
	}

	if err = v.FromItem(it); err != nil {
		return
	}

	return
}
