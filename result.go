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

func (c *result) Len() int64 {
	return int64(len(c.items))
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

	if err = v.FromItem(it); err != nil {
		return
	}

	return
}

// emptyResult is a result without items
type emptyResult struct{}

func (c emptyResult) Len() int64 { return 0 }
func (c emptyResult) Err() error { return nil }
func (c emptyResult) Next() bool { return false }
func (c emptyResult) Scan(v interface {
	Itemizer
	Deitemizer
}) (err error) {
	return
}
