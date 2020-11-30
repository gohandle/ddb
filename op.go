package ddb

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// Op represents a DynamoDB operation that can be build by one
// or more calls to the "Do" method.
type Op struct {
	ctx context.Context
	ddb Dynamo
}

// Exec starts a new DynamoDB operation
func Exec(ctx context.Context, ddb Dynamo) *Op {
	return &Op{ctx: ctx, ddb: ddb}
}

// Do is a flexible method for adding a sub-operation to the operation.
func (op *Op) Do(eb expression.Builder, o interface{}, ik ...Item) *Op {
	// @TODO panic/error if the op was already run

	return op
}

// Run the DynamoDB operation
func (op *Op) Run() (r Result, err error) {
	// @TODO assert that Do was called at least once

	return
}
