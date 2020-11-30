package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Results presents one-or-more items from DynamoDB.
type Result interface {
	Err() error
	Next() bool
	Scan(v interface {
		Itemizer
		Deitemizer
	}) error
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
