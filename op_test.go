package ddb

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	e "github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type testTable string

func (tbl testTable) Query1(id string) (eb e.Builder, q dynamodb.QueryInput) {
	return
}

func (tbl testTable) PutEntity1IfNotExist(et *testEntity1) (eb e.Builder, p dynamodb.Put, it Item) {
	it = et.Item()
	pk, _ := it.Keys()
	p.SetTableName(string(tbl))
	return eb.WithCondition(
		e.AttributeNotExists(e.Name(pk)),
	), p, it
}

type testItem1 struct{}

func (it testItem1) Keys() (pk, sk string) {
	return "pk", ""
}

type testEntity1 struct {
	ID string
}

func (e *testEntity1) FromItem(it Item) {
	// @TODO implement how an entity is retrieved from a table item
}

func (e testEntity1) Item() Item {
	// @TODO implement how an item is created from an entity
	it := &testItem1{}
	return it
}

func TestOpUsage(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*4)
	defer cancel()

	tbl := testTable("foo")

	t.Run("put", func(t *testing.T) {
		ent1 := &testEntity1{"id1"}

		if _, err := Exec(ctx, nil).Do(tbl.PutEntity1IfNotExist(ent1)).Run(); err != nil {
			t.Fatalf("got: %v", err)
		}

		t.Run("query", func(t *testing.T) {
			r, err := Exec(ctx, nil).Do(tbl.Query1("id1")).Run()
			if err != nil {
				t.Fatalf("got: %v", err)
			}

			for r.Next() {
				var ent2 testEntity1
				if err = r.Scan(&ent2); err != nil {
					t.Fatalf("got: %v", err)
				}
			}

			if err = r.Err(); err != nil {
				t.Fatalf("got: %v", err)
			}
		})
	})

}
