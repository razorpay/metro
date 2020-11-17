package errors

// Class allows for classification of errors by a Name.
// A default error code for the can be set in the Code attribute.
type Class struct {
	name string
	code string
}

// NewClass returns an new instance of Class
func NewClass(name string, defaultCode string) Class {
	return Class{
		name: name,
		code: defaultCode,
	}
}

// GetName returns the class name
func (c Class) Name() string {
	return c.name
}

// Error makes the class compatible with error interface
func (c Class) Error() string {
	return c.name
}

// New returns an Error instance for a specific Class.
func (c Class) New(code string) IError {
	if code == "" {
		code = c.code
	}

	// construct error struct with the given data
	// also get Public from map, if registered, by code
	// and embed into the error
	err := Error{
		class:    c,
		internal: newInternal(code),
		public:   newPublicFromCode(code),
	}

	return err
}
