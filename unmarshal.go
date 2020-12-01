package ddb

import (
	"fmt"
	"reflect"
)

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
