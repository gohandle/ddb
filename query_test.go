package ddb

// type table2 string

// func (tbl table2) createInput() *dynamodb.CreateTableInput {
// 	return (&dynamodb.CreateTableInput{}).
// 		SetTableName(string(tbl)).
// 		SetProvisionedThroughput((&dynamodb.ProvisionedThroughput{}).
// 			SetReadCapacityUnits(1).
// 			SetWriteCapacityUnits(1)).
// 		SetAttributeDefinitions(
// 			[]*dynamodb.AttributeDefinition{
// 				(&dynamodb.AttributeDefinition{}).
// 					SetAttributeName("id").SetAttributeType("N"),
// 			}).
// 		SetKeySchema(
// 			[]*dynamodb.KeySchemaElement{
// 				(&dynamodb.KeySchemaElement{}).
// 					SetAttributeName("id").
// 					SetKeyType("HASH"),
// 			})
// }

// // func (tbl table2) EntityPutIfNotExist(p *dynamodb.Put) (b e.Builder, it Itemizer) {
// // 	p.SetTableName(string(tbl))
// // 	return b.WithCondition(
// // 		e.AttributeNotExists(e.Name("id")),
// // 	)
// // }

// // ByFooNameQuery creates a query that uses the name as a condition
// func (tbl table2) ByFooNameQuery(name string) (q dynamodb.QueryInput, b e.Builder) {
// 	q.SetTableName(string(tbl))

// 	return q, b.WithKeyCondition(
// 		e.Key("foo").Equal(e.Value("bar")),
// 	)
// }

// func TestQuerySyntax(t *testing.T) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second*3)
// 	defer cancel()

// 	tbl := table2(t.Name())
// 	ddb := withLocalDB(t, tbl.createInput())

// 	t.Run("query", func(t *testing.T) {
// 		r, err := NewQuery(tbl.ByFooNameQuery("foo1")).Run(ctx, ddb)
// 		if err != nil {
// 			t.Fatalf("got: %v", err)
// 		}

// 		_ = r
// 	})
// }
