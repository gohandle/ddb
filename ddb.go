package ddb

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type Item interface {
	Keys() (pk, sk string)
}

type Itemizer interface {
	Item() Item
}

type Deitemizer interface {
	FromItem(Item) error
}

type Result interface {
	Err() error
	Len() int64
	Next() bool
	Scan(v interface {
		Itemizer
		Deitemizer
	}) error
}

// mapFilter is a utility method that returns a copy 'n' of 'm' that just holds
// the specified named element.
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

// exprBuild builds the expression but ignores errors that occure when
// a zero value builder is built.
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
