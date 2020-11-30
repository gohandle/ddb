package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type Item interface {
	Keys() (pk, sk string)
}

type Itemizer interface {
	Item() Item
}

type Deitemizer interface {
	FromItem(Item)
}

type Result interface {
	Next() bool
	Scan(v interface {
		Itemizer
		Deitemizer
	}) error
}

type Op struct {
	ctx context.Context
	ddb dynamodbiface.DynamoDBAPI
}

func Exec(ctx context.Context, ddb dynamodbiface.DynamoDBAPI) *Op {
	return &Op{ctx: ctx, ddb: ddb}
}

func (op *Op) Do(eb expression.Builder, o interface{}, ik ...Item) *Op {
	return op
}

func (op *Op) Run() (r Result, err error) {
	return
}
