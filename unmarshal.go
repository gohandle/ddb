package ddb

import (
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Copied from aws-sdk-go, because Encoder{} does not have EncodeMap()
func MarshalMap(in interface{}, enableEmptyCollections bool) (map[string]*dynamodb.AttributeValue, error) {
	encoder := dynamodbattribute.NewEncoder(func(e *dynamodbattribute.Encoder) {
		e.EnableEmptyCollections = enableEmptyCollections
	})
	av, err := encoder.Encode(in)
	if err != nil || av == nil || av.M == nil {
		return map[string]*dynamodb.AttributeValue{}, err
	}

	return av.M, nil
}

// UnmarshalAll will run through all results in 'r' and unmarshal all items into 'v'. It will
// consume the result in the process and it cannot scanned again afterwards. Each element of
// v must implement the Itemizer and Deitemizer interface.
func UnmarshalAll(r Result, v interface{}) (err error) {
	vt := reflect.TypeOf(v)
	if vt.Kind() != reflect.Ptr || vt.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("must be a pointer to a slice")
	}

	vv := reflect.ValueOf(v).Elem()
	var lastl int64
	var pos int
	for r.Next() {
		if l := r.Len(); l > lastl {
			ns := reflect.MakeSlice(vv.Type(), int(l-lastl), int(l-lastl))
			vv.Set(reflect.AppendSlice(vv, ns))
			lastl = l
		}

		// init at the index
		iv := vv.Index(pos)
		if iv.Kind() == reflect.Ptr {
			iv.Set(reflect.New(iv.Type().Elem()))
		}

		// scan if the interface allows it
		if itz, ok := iv.Interface().(interface {
			Itemizer
			Deitemizer
		}); ok {
			if err = r.Scan(itz); err != nil {
				return
			}
		}

		pos++
	}

	if err = r.Err(); err != nil {
		return
	}

	return
}
