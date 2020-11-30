package ddb

// Results presents one-or-more items from DynamoDB.
type Result interface {
	Err() error
	Next() bool
	Scan(v interface {
		Itemizer
		Deitemizer
	}) error
}
