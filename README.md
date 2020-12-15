# ddb
Helper types and for writing expressive and reusable DynamoDB code in Go without getting in the
way.

## Introduction
The DynamoDB API can be bit unwieldy any scary to look at. It has many different types and functions
with no clear guidance on how to combine them into a maintainable codebase. This library therefore
provides an opionated set of small tools that provides some guidance for accessing Dynamod from Go 
without abstracting too much and getting in the way.

Specifically, this project is not a ORM of sorts. It simply provides several small utility methods 
that fit together really nicely and allows your codebase to just focus on several modelling principles 
that are specific to DynamoDB:

### Principle 1: Mapping entities onto Items
Unlike relational data modelling, it is common with DynamoDB to store multiple entities into a single 
table. For example: when using DynamoDB it would be common that you store the "user" entity in the 
same table as the "team" entity. You would then use indexes and/or attribute 
overloading in a [clever way](https://www.alexdebrie.com/posts/dynamodb-one-to-many/) to model 
the many-to-one relation between them. 

To facilitate this, the library allows (and expects!) you to explicitely map each "entity" onto an "item" 
before it can be stored in dynamodb. There should be only one "item" type per table and the item 
type will be used for marshalling and unmarshalling through the [dynamodbattribute](https://godoc.org/github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute) package. 

Each entity must implement the [`Itemizer`](https://pkg.go.dev/github.com/gohandle/ddb#Itemizer) 
and [`Deitemizer`](https://pkg.go.dev/github.com/gohandle/ddb#Deitemizer) interface to encode how 
the entity is turned into an item and vice versa.

This is better explained through an example and might sound like a lot of unessary work but 
the mapping logic is an important part of the modelling process and a primary concern of the 
programmer. This library allows you to focus on that.

### Principle 2: Thinking in Access Patterns
Relations databases come with a dedicated language (SQL) for encoding how to the database is accessed. And although
AWS [announced](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html) PartiQL support for DynamoDB it doesn't provide all the options. 

Instead it is recommend to think about ["access patterns"](https://docs.amazonaws.cn/en_us/amazondynamodb/latest/developerguide/bp-modeling-nosql.html) in order to model the data of your application. In the "user" 
and "group" example above an acess pattern could be "get-all-users-in-group" or "assign-user-to-group".  

Similar to the first principle, this library brings the implementation of these access patterns to the
forefront. Each acces pattern is represented by a function (or method) that is expected to return
an [expression builder](https://godoc.org/github.com/aws/aws-sdk-go/service/dynamodb/expression), 
one of the basic operations (Put, Get, Update, Delete, Check and Query) and an entity for Put 
operations.

Again, see one of the examples below to get a better feel for this.

### Principle 3: Transactions as compositions of basic operations
While relation database have transactions as a core functionality, for dynamodb this has only
been added at the end of 2018. As such there is now a mix of transaction and non-transactional 
operations exposed by the official API. 

This library focusses on the newest set of transaction operations and allows you to compose 
several basic access pattern functions (see above) into larger transactions that modify a 
set of items atomically across multiple tables.

## Examples
The best way to illustrate the principles above is through so lets look at a few. Slowly
building up the complexity

### Example 1: Storing a simple Foo entity
The simplest possible table (table1) is one that stores just one entity and only has a partition key:

```Go

// The simplest possible item has just one attribute: the partition key
type Table1Item struct{
  PK string `dynamodbav:"pk"`
}

// Keys indicates which attributes of this item represent the partition and sort key
func (Table1Item) Keys() (pk, sk string) { return "pk", ""}

// The "Foo" entity is stored in table 1, it just has an identifier.
type Foo struct{
  ID   int
}

// We map the Foo entity onto the FooItem. With the partion key set to the 
// identifier. This is done for operations that store entities into the table: Put
func (e Foo) Item() ddb.Item { 
  return Table1Item{PK: strconv.Itoa(e.ID)}
}

// FromItem unmaps the item back into an entity. This is used for operations that read from the
// database: Query, Get etc
func (e *Foo) FromItem(it ddb.Item) {
  e.ID, _ = strconv.Atoi(it.(*Table1Item).PK)
}

// PutFoo implements the access pattern for storing new foo entities.
func PutFoo(id int) (eb expression.Builder, p dynamodb.Put, it Itemizer) {
  p.SetTableName("table1")
  return eb, p, &Foo{ID: id}
}

```

## docs
- [ ] Items can also implement the itemizer interface
- [ ] Examples for each operation
- [ ] Item returned from entity.Item() should be a pointer, else unmarshal will fail

## backlog
- [ ] MUST try to see what happens if access patterns are added to the item.
- [ ] SHOULD panic/error if a "op" is used after running it
- [ ] COULD  add an option that always runs singleton operations in a transaction alsop
- [ ] COULD  add option for consisten read
- [ ] COULD  add option for ReturnConsumedCapacity
- [ ] COULD  make Do return an TXDB-like interface for composability of operations
- [ ] COULD  turn a set of Put, and Deletes into a BatchWriteRequest as well
- [ ] COULD  turn a set Get requests into a BatchGetItem/BatchWriteItem request 
- [ ] SHOULD support 	"ReturnValues" while wi.Put/Delete/Update doesn't support it
- [ ] COULD create error types that show the dynamodb input for debugging
- [ ] COULD  be nice to have an helper that decodes the whole result into a slice of entities
- [ ] COULD  be nice to have an helper that decodes the result with the expectation there is only one item
- [ ] COULD  share more code between QueryResult and ScanResult