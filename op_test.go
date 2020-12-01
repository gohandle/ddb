package ddb

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
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
	PK string `dynamodbav:"pk"`
	F1 string `dynamodbav:"f1"`
}

func (it testItem1) Item() Item {
	return it
}

func (it testItem1) Keys() (pk, sk string) {
	return "pk", ""
}

func TestOpDo(t *testing.T) {
	for i, c := range []struct {
		eb        e.Builder
		sop       interface{}
		ik        []Itemizer
		expErr    error
		expWrites bool
		expReads  bool
	}{
		{eb: e.NewBuilder(), sop: dynamodb.Delete{}, expWrites: true},
		{eb: e.NewBuilder(), sop: dynamodb.Get{}, expReads: true},
		{eb: e.NewBuilder(), sop: dynamodb.Put{}, expWrites: true},
		{eb: e.NewBuilder(), sop: dynamodb.Update{}, expWrites: true},
		{eb: e.NewBuilder(), sop: dynamodb.ConditionCheck{}, expWrites: true},
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

func withLocalDB(tb testing.TB, tbls ...*dynamodb.CreateTableInput) (ddb *dynamodb.DynamoDB) {
	jexe, err := exec.LookPath("java")
	if jexe == "" || err != nil {
		tb.Fatalf("java not available in PATH: %v", err)
	}

	cmd := exec.Command(jexe,
		"-D"+filepath.Join("internal", "localddb", "DynamoDBLocal_lib"),
		"-jar", filepath.Join("internal", "localddb", "DynamoDBLocal.jar"),
		"-inMemory", "--port", "12000",
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
		Endpoint: aws.String("http://localhost:12000"),
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

type entity1 struct {
	Name string
}

func (e *entity1) Item() Item {
	return &testItem1{
		PK: base64.URLEncoding.EncodeToString([]byte(e.Name)),
		F1: e.Name,
	}
}

func (tbl table1) itemPut1(e *entity1) (eb e.Builder, p dynamodb.Put, it Itemizer) {
	p.SetTableName(string(tbl))
	it = e
	return
}

func TestPutEnd2End(t *testing.T) {
	tbl1 := table1(t.Name())
	ddb := withLocalDB(t, tbl1.createInput())

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
	defer cancel()

	t.Run("put", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			e := &entity1{"entity_" + strconv.Itoa(i)}
			if _, err := Exec(ctx, ddb).Do(tbl1.itemPut1(e)).Run(); err != nil {
				t.Fatalf("got: %v", err)
			}
		}

		// @TODO query

		// @TODO update (with return old item)
	})

}
