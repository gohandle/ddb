package ddb

import (
	"context"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	e "github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// withLocalDB will run local Dynamodb for the duration of the test while creating any tables
// that are provided
func withLocalDB(tb testing.TB, tbls ...*dynamodb.CreateTableInput) (ddb *dynamodb.DynamoDB) {
	jexe, err := exec.LookPath("java")
	if jexe == "" || err != nil {
		tb.Fatalf("java not available in PATH: %v", err)
	}

	port := rand.Intn(65535-49152+1) + 49152
	cmd := exec.Command(jexe,
		"-D"+filepath.Join("internal", "localddb", "DynamoDBLocal_lib"),
		"-jar", filepath.Join("internal", "localddb", "DynamoDBLocal.jar"),
		"-inMemory", "--port", strconv.Itoa(port),
	)

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	if err = cmd.Start(); err != nil {
		tb.Fatalf("failed to start local ddb: %v", err)
	}

	tb.Cleanup(func() {
		cmd.Process.Kill()
	})

	var sess *session.Session
	if sess, err = session.NewSession(&aws.Config{
		Region:   aws.String("eu-west-1"),
		Endpoint: aws.String("http://localhost:" + strconv.Itoa(port)),
	}); err != nil {
		tb.Fatalf("failed to create local session: %v", err)
	}

	ddb = dynamodb.New(sess)
	for _, in := range tbls {
		if _, err = ddb.CreateTable(in); err != nil {
			tb.Fatalf("failed to create table %v: %v", in, err)
		}
	}

	return
}

// table1 describes a simple table that just has a string pk
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

func (tbl table1) simpleScan() (b e.Builder, s dynamodb.ScanInput) {
	s.SetTableName(string(tbl))
	s.SetLimit(2)
	return
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

func TestTable1End2End(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
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

					if act := r.Len(); act != 1 {
						t.Fatalf("got: %v", act)
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

				t.Run("scan all", func(t *testing.T) {
					r, err := Scan(tbl.simpleScan()).Run(ctx, ddb)
					if err != nil {
						t.Fatalf("got: %v", err)
					}

					// before iteration is complete it can only know about
					// the length of a page
					if act := r.Len(); act != 2 {
						t.Fatalf("got: %v", act)
					}

					var names []string
					for r.Next() {
						var e table1Entity
						if err := r.Scan(&e); err != nil {
							t.Fatalf("got: %v", err)
						}
						names = append(names, e.Name)
					}

					if act := r.Len(); act != 5 {
						t.Fatalf("got: %v", act)
					}

					if err = r.Err(); err != nil {
						t.Fatalf("got: %v", err)
					}

					if act := strings.Join(names, ","); act != "name-9,name-7,name-5,name-8,foo" {
						t.Fatalf("got: %v", act)
					}
				})
			})
		})
	})
}

// table2 describes a table with a compound key and gsi that reverses that key
type table2 string

func (tbl table2) createInput() *dynamodb.CreateTableInput {
	return (&dynamodb.CreateTableInput{}).
		SetTableName(string(tbl)).
		SetGlobalSecondaryIndexes(
			[]*dynamodb.GlobalSecondaryIndex{
				(&dynamodb.GlobalSecondaryIndex{}).
					SetIndexName("gsi1").
					SetProvisionedThroughput((&dynamodb.ProvisionedThroughput{}).
						SetReadCapacityUnits(1).
						SetWriteCapacityUnits(1)).
					SetProjection((&dynamodb.Projection{}).
						SetProjectionType(dynamodb.ProjectionTypeAll)).
					SetKeySchema(
						[]*dynamodb.KeySchemaElement{
							(&dynamodb.KeySchemaElement{}).
								SetAttributeName("kind").
								SetKeyType("HASH"),
						},
					),
			}).
		SetProvisionedThroughput((&dynamodb.ProvisionedThroughput{}).
			SetReadCapacityUnits(1).
			SetWriteCapacityUnits(1)).
		SetAttributeDefinitions(
			[]*dynamodb.AttributeDefinition{
				(&dynamodb.AttributeDefinition{}).
					SetAttributeName("pk").SetAttributeType("S"),
				(&dynamodb.AttributeDefinition{}).
					SetAttributeName("sk").SetAttributeType("S"),
				(&dynamodb.AttributeDefinition{}).
					SetAttributeName("kind").SetAttributeType("N"),
			}).
		SetKeySchema(
			[]*dynamodb.KeySchemaElement{
				(&dynamodb.KeySchemaElement{}).
					SetAttributeName("pk").
					SetKeyType("HASH"),

				(&dynamodb.KeySchemaElement{}).
					SetAttributeName("sk").
					SetKeyType("RANGE"),
			})
}

func (tbl table2) ByKind(kind int64) (b e.Builder, q dynamodb.QueryInput) {
	q.SetTableName(string(tbl))
	q.SetIndexName("gsi1")
	q.SetLimit(2)

	return b.WithKeyCondition(
		e.Key("kind").Equal(e.Value(kind)),
	), q
}

func (tbl table2) Get1(id int) (b e.Builder, op dynamodb.Get, it Itemizer) {
	op.SetTableName(string(tbl))
	return b, op, &table2Entity{ID: id}
}

func (tbl table2) Put1(e *table2Entity) (b e.Builder, op dynamodb.Put, it Itemizer) {
	op.SetTableName(string(tbl))
	return b, op, e
}

func (tbl table2) Chc1(id int, isKind int) (b e.Builder, op dynamodb.ConditionCheck, it Itemizer) {
	ent := &table2Entity{ID: id, Kind: isKind}
	item := ent.Item().(*table2Item)

	op.SetTableName(string(tbl))
	return b.WithCondition(
		e.Name("kind").Equal(e.Value(item.Kind)),
	), op, ent
}

type table2Item struct {
	PK   string `dynamodbav:"pk"`
	SK   string `dynamodbav:"sk"`
	Kind int64  `dynamodbav:"kind"`
}

func (table2Item) Keys() (pk, sk string) {
	return "pk", "sk"
}

type table2Entity struct {
	ID   int
	Kind int
}

func (e *table2Entity) FromItem(it Item) {
	e.ID, _ = strconv.Atoi(it.(*table2Item).PK)
}

func (e table2Entity) Item() Item {
	return &table2Item{
		PK:   strconv.Itoa(e.ID),
		SK:   strconv.Itoa(e.ID),
		Kind: int64(e.Kind),
	}
}

func TestTable2End2End(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	tbl := table2(t.Name())
	ddb := withLocalDB(t, tbl.createInput())

	t.Run("put tx", func(t *testing.T) {
		w := NewWriter()
		for i := 0; i < 8; i++ {
			e := &table2Entity{i, 0}
			if i > 4 {
				e.Kind = 1
			}

			w.Put(tbl.Put1(e))
		}

		if _, err := w.Run(ctx, ddb); err != nil {
			t.Fatalf("got: %v", err)
		}

		t.Run("get tx", func(t *testing.T) {
			rd := NewReader()
			for i := 0; i < 4; i++ {
				rd.Get(tbl.Get1(i))
			}

			r, err := rd.Run(ctx, ddb)
			if err != nil {
				t.Fatalf("got: %v", err)
			}

			if act := r.Len(); act != 4 {
				t.Fatalf("got: %v", act)
			}

			var ids []string
			for r.Next() {
				var e table2Entity
				if err := r.Scan(&e); err != nil {
					t.Fatalf("got: %v", err)
				}

				ids = append(ids, strconv.Itoa(e.ID))
			}

			if r.Err() != nil {
				t.Fatalf("got: %v", err)
			}

			if act := strings.Join(ids, ","); act != "0,1,2,3" {
				t.Fatalf("got: %v", act)
			}
		})

		t.Run("perform checks in transaction", func(t *testing.T) {
			if _, err := Check(tbl.Chc1(5, 1)).Run(ctx, ddb); err != nil {
				t.Fatalf("got: %v", err)
			}

			if _, err := Check(tbl.Chc1(1, 1)).Run(ctx, ddb); err == nil {
				t.Fatalf("should error, got: %v", err)
			}
		})

		t.Run("query by kind", func(t *testing.T) {
			r, err := Query(tbl.ByKind(1)).Run(ctx, ddb)
			if err != nil {
				t.Fatalf("got: %v", err)
			}

			if act := r.Len(); act != 2 {
				t.Fatalf("got: %v", act)
			}

			var ids []string
			for r.Next() {
				var e table2Entity
				if err := r.Scan(&e); err != nil {
					t.Fatalf("got: %v", err)
				}

				ids = append(ids, strconv.Itoa(e.ID))
			}

			if act := r.Len(); act != 3 {
				t.Fatalf("got: %v", act)
			}

			if act := strings.Join(ids, ","); act != "5,7,6" {
				t.Fatalf("got: %v", act)
			}

			if err = r.Err(); err != nil {
				t.Fatalf("got: %v", err)
			}
		})
	})
}
