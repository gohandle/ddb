package ddb

import (
	"errors"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	e "github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// type testTable string

// func (tbl testTable) Query1(id string) (eb e.Builder, q dynamodb.QueryInput) {
// 	return
// }

// func (tbl testTable) PutEntity1IfNotExist(et *testEntity1) (eb e.Builder, p dynamodb.Put, it Item) {
// 	it = et.Item()
// 	pk, _ := it.Keys()
// 	p.SetTableName(string(tbl))
// 	return eb.WithCondition(
// 		e.AttributeNotExists(e.Name(pk)),
// 	), p, it
// }

type testItem1 struct {
	PK  string `dynamodbav:"pk"`
	Foo string `dynamodbav:"foo"`
}

func (it testItem1) Item() Item {
	return it
}

func (it testItem1) Keys() (pk, sk string) {
	return "pk", ""
}

// type testEntity1 struct {
// 	ID string
// }

// func (e *testEntity1) FromItem(it Item) {
// 	// @TODO implement how an entity is retrieved from a table item
// }

// func (e testEntity1) Item() Item {
// 	// @TODO implement how an item is created from an entity
// 	it := &testItem1{}
// 	return it
// }

// func TestOpUsage(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*4)
// 	defer cancel()

// 	tbl := testTable("foo")

// 	t.Run("put", func(t *testing.T) {
// 		ent1 := &testEntity1{"id1"}

// 		if _, err := Exec(ctx, nil).Do(tbl.PutEntity1IfNotExist(ent1)).Run(); err != nil {
// 			t.Fatalf("got: %v", err)
// 		}

// 		t.Run("query", func(t *testing.T) {
// 			r, err := Exec(ctx, nil).Do(tbl.Query1("id1")).Run()
// 			if err != nil {
// 				t.Fatalf("got: %v", err)
// 			}

// 			for r.Next() {
// 				var ent2 testEntity1
// 				if err = r.Scan(&ent2); err != nil {
// 					t.Fatalf("got: %v", err)
// 				}
// 			}

// 			if err = r.Err(); err != nil {
// 				t.Fatalf("got: %v", err)
// 			}
// 		})
// 	})
// }

func TestOpDo(t *testing.T) {
	for i, c := range []struct {
		eb        e.Builder
		sop       interface{}
		ik        []Itemizer
		expErr    error
		expWrites bool
		expReads  bool
		expQuery  bool
		expScan   bool
	}{
		{eb: e.NewBuilder(), sop: dynamodb.Delete{}, expWrites: true},
		{eb: e.NewBuilder(), sop: dynamodb.Get{}, expReads: true},
		{eb: e.NewBuilder(), sop: dynamodb.Put{}, expWrites: true},
		{eb: e.NewBuilder(), sop: dynamodb.Update{}, expWrites: true},
		{eb: e.NewBuilder(), sop: dynamodb.ConditionCheck{}, expWrites: true},
		{eb: e.NewBuilder(), sop: dynamodb.QueryInput{}, expQuery: true},
		{eb: e.NewBuilder(), sop: dynamodb.ScanInput{}, expScan: true},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			op := Exec(nil, nil).Do(c.eb, c.sop, c.ik...)
			if !errors.Is(op.err, c.expErr) {
				t.Fatalf("got: %v", op.err)
			}

			switch {
			case c.expWrites && len(op.writes) != 1:
				t.Fatalf("exp write got: %v", op.writes)
			case c.expReads && len(op.reads) != 1:
				t.Fatalf("exp read got: %v", op.writes)
			case c.expQuery && op.query == nil:
				t.Fatalf("exp query got: %v", op.query)
			case c.expScan && op.scan == nil:
				t.Fatalf("exp scan got: %v", op.scan)
			}
		})
	}
}

func TestOpKeyMarshal(t *testing.T) {
	ik := testItem1{PK: "foo"}

	op := Exec(nil, nil).Do(e.NewBuilder(), dynamodb.Update{}, ik)
	if *op.writes[0].Update.Key["pk"].S != "foo" {
		t.Fatalf("got: %v", op.writes)
	}

	// should ignore non-key attributes of the item
	if op.writes[0].Update.Key["foo"] != nil {
		t.Fatalf("got: %v", op.writes)
	}
}
