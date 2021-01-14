package ddb

// DefaultOptions will be used when no options are specified
var DefaultOptions = []Option{EnableEmptyCollections()}

// Options holds option values for all options that we support
type Options struct {
	enableEmptyCollections bool
}

// Apply options
func (opts *Options) Apply(os ...Option) {
	for _, o := range os {
		o(opts)
	}
}

// Option get's called to configure options
type Option func(*Options)

// EnableEmptyCollections is an option that enables the EnableEmptyCollections on all marshal calls
func EnableEmptyCollections() func(o *Options) {
	return func(o *Options) { o.enableEmptyCollections = true }
}
