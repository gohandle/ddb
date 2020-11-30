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
