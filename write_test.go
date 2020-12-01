package ddb

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	e "github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

type table1 string

func (tbl table1) createInput() *dynamodb.CreateTableInput {
	return (&dynamodb.CreateTableInput{}).
		SetTableName(string(tbl)).
		SetProvisionedThroughput((&dynamodb.ProvisionedThroughput{}).
			SetReadCapacityUnits(1).
			SetWriteCapacityUnits(1)).
		SetAttributeDefinitions(
			[]*dynamodb.AttributeDefinition{
				(&dynamodb.AttributeDefinition{}).
					SetAttributeName("pk").SetAttributeType("S"),
			}).
		SetKeySchema(
			[]*dynamodb.KeySchemaElement{
				(&dynamodb.KeySchemaElement{}).
					SetAttributeName("pk").
					SetKeyType("HASH"),
			})
}

type table1Item struct {
	PK string `dynamodbav:"pk"`
	F1 string `dynamodbav:"f1"`
}

func (table1Item) Keys() (pk, sk string) {
	return "pk", ""
}

type table1Entity struct {
	ID   int
	Name string
}

func (e *table1Entity) FromItem(it Item) {
	e.ID, _ = strconv.Atoi(it.(*table1Item).PK[1:])
	e.Name = it.(*table1Item).F1
}

func (e table1Entity) Item() Item {
	return &table1Item{
		PK: "e" + strconv.Itoa(e.ID),
		F1: e.Name,
	}
}

func (tbl table1) simpleGet1(id int) (b e.Builder, g dynamodb.Get, k Itemizer) {
	g.SetTableName(string(tbl))
	return b, g, &table1Entity{ID: id}
}

func (tbl table1) simpleUpd1(id int, newName string) (b e.Builder, u dynamodb.Update, k Itemizer) {
	u.SetTableName(string(tbl))
	return b.WithUpdate(
		e.Set(e.Name("f1"), e.Value(newName)),
	), u, &table1Entity{ID: id}
}

func (tbl table1) simpleQry1(id int) (b e.Builder, q dynamodb.QueryInput) {
	pk := (&table1Entity{ID: id}).Item().(*table1Item).PK

	q.SetTableName(string(tbl))
	return b.WithKeyCondition(
		e.Key("pk").Equal(e.Value(pk)),
	), q
}

func (tbl table1) simpleDel1(id int) (b e.Builder, p dynamodb.Delete, k Itemizer) {
	p.SetTableName(string(tbl))
	return b, p, &table1Entity{ID: id}
}

func (tbl table1) simplePut1(e *table1Entity) (b e.Builder, p dynamodb.Put, it Itemizer) {
	p.SetTableName(string(tbl))
	return b, p, e
}

func TestSinglePutDeleteQuery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	tbl := table1(t.Name())
	ddb := withLocalDB(t, tbl.createInput())

	t.Run("put 10", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			e := &table1Entity{i, "name-" + strconv.Itoa(i)}
			if _, err := Put(tbl.simplePut1(e)).Run(ctx, ddb); err != nil {
				t.Fatalf("got: %v", err)
			}
		}

		t.Run("delete 5", func(t *testing.T) {
			for i := 0; i < 5; i++ {
				if _, err := Delete(tbl.simpleDel1(i)).Run(ctx, ddb); err != nil {
					t.Fatalf("got: %v", err)
				}
			}

			t.Run("update nr 6", func(t *testing.T) {
				if _, err := Update(tbl.simpleUpd1(6, "foo")).Run(ctx, ddb); err != nil {
					t.Fatalf("got: %v", err)
				}

				t.Run("query nr 6", func(t *testing.T) {
					r, err := Query(tbl.simpleQry1(6)).Run(ctx, ddb)
					if err != nil {
						t.Fatalf("got: %v", err)
					}

					for r.Next() {
						var e table1Entity
						if err := r.Scan(&e); err != nil {
							t.Fatalf("got: %v", err)
						}

						if e.ID != 6 || e.Name != "foo" {
							t.Fatalf("got: %v", e)
						}
					}

					if err = r.Err(); err != nil {
						t.Fatalf("got: %v", err)
					}
				})

				t.Run("get nr 6", func(t *testing.T) {
					r, err := Get(tbl.simpleGet1(6)).Run(ctx, ddb)
					if err != nil {
						t.Fatalf("got: %v", err)
					}

					for r.Next() {
						var e table1Entity
						if err := r.Scan(&e); err != nil {
							t.Fatalf("got: %v", err)
						}

						if e.ID != 6 || e.Name != "foo" {
							t.Fatalf("got: %v", e)
						}
					}

					if err = r.Err(); err != nil {
						t.Fatalf("got: %v", err)
					}
				})
			})

			// @TODO scan the rest
		})
	})
}
