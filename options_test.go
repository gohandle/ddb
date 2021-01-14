package ddb

import "testing"

func TestOptions(t *testing.T) {
	var opts Options
	if opts.enableEmptyCollections {
		t.Fatalf("got: %v", opts.enableEmptyCollections)
	}

	opts.Apply(EnableEmptyCollections())
	if !opts.enableEmptyCollections {
		t.Fatalf("got: %v", opts.enableEmptyCollections)
	}

	var opts2 Options
	opts2.Apply(DefaultOptions...)
	if !opts2.enableEmptyCollections {
		t.Fatalf("got: %v", opts2.enableEmptyCollections)
	}
}
