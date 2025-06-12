package base

import "errors"

type CustomError struct {
	error
	code int
}

var ClientInvalidRequestError = CustomError{
	error: errors.New("client invalid request error"),
	code:  400,
}
var ClientContentTooLargeError = CustomError{
	error: errors.New("client is too large"),
	code:  413,
}
