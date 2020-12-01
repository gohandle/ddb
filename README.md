# ddb
Helper types for writing expression DynamoDB code

## backlog
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

## notes
- A Query, Scan, Put, Get, Delete, Update and ConditionCheck only variables in an operations are:
  - The dynamodb.QueryInput, dynamodb.Put...
  - The expression.Builder 
  - The Item/Key
- There is "QueryPages" already
- There is "ScanPages" already