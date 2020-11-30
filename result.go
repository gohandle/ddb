package ddb

// Results presents one-or-more items from DynamoDB.
type Result interface {
	Next() bool
	Scan(v interface {
		Itemizer
		Deitemizer
	}) error
}
