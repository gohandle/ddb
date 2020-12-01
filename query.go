package ddb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// Querier holds a DynamoDB query
type Querier struct {
	res *queryResult
	eb  expression.Builder
}

// Query sets up a query that can be run to fetch
func Query(b expression.Builder, in dynamodb.QueryInput) (q *Querier) {
	q = new(Querier)
	q.res = &queryResult{pos: -1}
	q.res.in = &in
	q.eb = b
	return
}

// Run will return a Query result for iteration
func (q *Querier) Run(ctx context.Context, ddb Dynamo) (r Result, err error) {
	expr, err := exprBuild(q.eb)
	if err != nil {
		return nil, fmt.Errorf("failed to build expression(s): %w", err)
	}
	q.res.ddb = ddb
	q.res.ctx = ctx
	q.res.in.FilterExpression = expr.Filter()
	q.res.in.KeyConditionExpression = expr.KeyCondition()
	q.res.in.ProjectionExpression = expr.Projection()
	q.res.in.ExpressionAttributeNames = expr.Names()
	q.res.in.ExpressionAttributeValues = expr.Values()
	return q.res, nil
}

// queryResult is a result that is returned when a query operation
// is run. It will automatically get more pages as the user scans
// through the results.
type queryResult struct {
	ctx context.Context
	in  *dynamodb.QueryInput
	out *dynamodb.QueryOutput
	ddb Dynamo
	err error
	pos int
}

func (c *queryResult) Err() error {
	return c.err
}

func (c *queryResult) Next() bool {

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

	// no out, must be the first time it is queried
	if c.out == nil {
		if c.out, c.err = c.ddb.QueryWithContext(c.ctx, c.in); c.err != nil {
			return false
		}
	}

	return true
}

func (c *queryResult) Scan(v interface {
	Itemizer
	Deitemizer
}) (err error) {
	it := v.Item()
	if err = dynamodbattribute.UnmarshalMap(c.out.Items[c.pos], it); err != nil {
		return
	}

	v.FromItem(it)
	return
}
