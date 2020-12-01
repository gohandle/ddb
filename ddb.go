package ddb

type Item interface {
	Keys() (pk, sk string)
}

type Itemizer interface {
	Item() Item
}

type Deitemizer interface {
	FromItem(Item)
}

type Result interface {
	Err() error
	Next() bool
	Scan(v interface {
		Itemizer
		Deitemizer
	}) error
}
