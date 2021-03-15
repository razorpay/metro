package stream

func isErrorRecoverable(err error) bool {

	// TODO : implement!
	switch err.(type) {
	default:
		return true
	}

	return false
}
