package ddb

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// result holds an unpaginated result of at least 1 item
type result struct {
	items []map[string]*dynamodb.AttributeValue
	pos   int
}

func newResult(its ...map[string]*dynamodb.AttributeValue) *result {
	return &result{its, -1}
}

func (c *result) Err() error {
	return nil
}

func (c *result) Next() bool {
	c.pos++
	if c.pos >= len(c.items) {
		return false
	}
	return true
}

func (c *result) Scan(v interface {
	Itemizer
	Deitemizer
}) (err error) {
	it := v.Item()
	if err = dynamodbattribute.UnmarshalMap(c.items[c.pos], it); err != nil {
		return
	}

	v.FromItem(it)
	return
}
