# ddb
Helper types for writing expression DynamoDB code


## backlog
- [ ] SHOULD panic/error if a "op" is used after running it
- [ ] COULD  add an option that always runs singleton operations in a transaction alsop
- [ ] COULD  add option for consisten read
- [ ] COULD  add option for ReturnConsumedCapacity
- [ ] COULD  make Do return an TXDB-like interface for composability of operations
- [ ] COULD  turn a set of Put, and Deletes into a BatchWriteRequest as well
- [ ] COULD  turn a set Get requests into a BatchGetItem request 
- [ ] SHOULD support 	"ReturnValues" while wi.Put/Delete/Update doesn't support it

## notes
- A Query, Scan, Put, Get, Delete, Update and ConditionCheck only variables in an operations are:
  - The dynamodb.QueryInput, dynamodb.Put...
  - The expression.Builder 
  - The Item/Key
- There is "QueryPages" already
- There is "ScanPages" already